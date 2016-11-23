/*
 * Copyright 2015-2016 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as Q from 'q';
import { Timezone, Duration } from 'chronoshift';
import { immutableArraysEqual, immutableLookupsEqual, SimpleArray, NamedArray } from 'immutable-class';
import { PlyType, DatasetFullType, PlyTypeSimple, FullType } from '../types';
import { hasOwnProperty, nonEmptyLookup, safeAdd } from '../helper/utils';
import {
  $, Expression, RefExpression, ExternalExpression,
  ChainableExpression,
  ChainableUnaryExpression,
  ApplyExpression,
  FilterExpression,
  LimitExpression,
  NumberBucketExpression,
  SelectExpression,
  SortExpression,
  SplitExpression,
  TimeBucketExpression,
  TimeFloorExpression,
  AndExpression
} from '../expressions/index';
import { PlywoodValue, Datum, Dataset } from '../datatypes/dataset';
import { Attributes, AttributeInfo, AttributeJSs } from '../datatypes/attributeInfo';
import { NumberRange } from '../datatypes/numberRange';
import { unwrapSetType } from '../datatypes/common';
import { CustomDruidAggregations, CustomDruidTransforms } from './utils/druidTypes';
import { ExpressionJS } from '../expressions/baseExpression';
import { Set } from '../datatypes/set';
import { StringRange } from '../datatypes/stringRange';
import { TimeRange } from '../datatypes/timeRange';
import { promiseWhile } from '../helper/promiseWhile';

export class TotalContainer {
  public datum: Datum;

  constructor(d: Datum) {
    this.datum = d;
  }

  toJS(): any {
    return {
      datum: Dataset.datumToJS(this.datum)
    };
  }
}

export interface PostProcess {
  (result: any): PlywoodValue | TotalContainer;
}

export interface NextFn<Q, R> {
  (prevQuery: Q, prevResult: R): Q;
}

export interface QueryAndPostProcess<T> {
  query: T;
  postProcess: PostProcess;
  next?: NextFn<T, any>;
}

export interface Inflater {
  (d: Datum, i: number, data: Datum[]): void;
}

export type QueryMode = "raw" | "value" | "total" | "split";

function nullMap<T, Q>(xs: T[], fn: (x: T) => Q): Q[] {
  if (!xs) return null;
  let res: Q[] = [];
  for (let x of xs) {
    let y = fn(x);
    if (y) res.push(y);
  }
  return res.length ? res : null;
}

function filterToAnds(filter: Expression): Expression[] {
  if (filter.equals(Expression.TRUE)) return [];
  if (filter instanceof AndExpression) return filter.getExpressionList();
  return [filter];
}

function filterDiff(strongerFilter: Expression, weakerFilter: Expression): Expression {
  let strongerFilterAnds = filterToAnds(strongerFilter);
  let weakerFilterAnds = filterToAnds(weakerFilter);
  if (weakerFilterAnds.length > strongerFilterAnds.length) return null;
  for (let i = 0; i < weakerFilterAnds.length; i++) {
    if (!(weakerFilterAnds[i].equals(strongerFilterAnds[i]))) return null;
  }
  return Expression.and(strongerFilterAnds.slice(weakerFilterAnds.length));
}

function getCommonFilter(filter1: Expression, filter2: Expression): Expression {
  let filter1Ands = filterToAnds(filter1);
  let filter2Ands = filterToAnds(filter2);
  let minLength = Math.min(filter1Ands.length, filter2Ands.length);
  let commonExpressions: Expression[] = [];
  for (let i = 0; i < minLength; i++) {
    if (!filter1Ands[i].equals(filter2Ands[i])) break;
    commonExpressions.push(filter1Ands[i]);
  }
  return Expression.and(commonExpressions);
}

function mergeDerivedAttributes(derivedAttributes1: Lookup<Expression>, derivedAttributes2: Lookup<Expression>): Lookup<Expression> {
  let derivedAttributes: Lookup<Expression> = Object.create(null);
  for (let k in derivedAttributes1) {
    derivedAttributes[k] = derivedAttributes1[k];
  }
  for (let k in derivedAttributes2) {
    if (hasOwnProperty(derivedAttributes, k) && !derivedAttributes[k].equals(derivedAttributes2[k])) {
      throw new Error(`can not currently redefine conflicting ${k}`);
    }
    derivedAttributes[k] = derivedAttributes2[k];
  }
  return derivedAttributes;
}

function getSampleValue(valueType: string, ex: Expression): PlywoodValue {
  switch (valueType) {
    case 'BOOLEAN':
      return true;

    case 'NUMBER':
      return 4;

    case 'NUMBER_RANGE':
      if (ex instanceof NumberBucketExpression) {
        return new NumberRange({
          start: ex.offset,
          end: ex.offset + ex.size
        });
      } else {
        return new NumberRange({ start: 0, end: 1 });
      }

    case 'TIME':
      return new Date('2015-03-14T00:00:00');

    case 'TIME_RANGE':
      if (ex instanceof TimeBucketExpression) {
        let timezone = ex.timezone || Timezone.UTC;
        let start = ex.duration.floor(new Date('2015-03-14T00:00:00'), timezone);
        return new TimeRange({
          start,
          end: ex.duration.shift(start, timezone, 1)
        });
      } else {
        return new TimeRange({ start: new Date('2015-03-14T00:00:00'), end: new Date('2015-03-15T00:00:00') });
      }

    case 'STRING':
      if (ex instanceof RefExpression) {
        return 'some_' + ex.name;
      } else {
        return 'something';
      }

    case 'SET/STRING':
      if (ex instanceof RefExpression) {
        return Set.fromJS([ex.name + '1']);
      } else {
        return Set.fromJS(['something']);
      }

    case 'STRING_RANGE':
      if (ex instanceof RefExpression) {
        return StringRange.fromJS({start: 'some_' + ex.name, end: null});
      } else {
        return StringRange.fromJS({start: 'something', end: null});
      }

    default:
      throw new Error("unsupported simulation on: " + valueType);
  }
}

function immutableAdd<T>(obj: Lookup<T>, key: string, value: T): Lookup<T> {
  let newObj = Object.create(null);
  for (let k in obj) newObj[k] = obj[k];
  newObj[key] = value;
  return newObj;
}

function findApplyByExpression(applies: ApplyExpression[], expression: Expression): ApplyExpression {
  for (let apply of applies) {
    if (apply.expression.equals(expression)) return apply;
  }
  return null;
}

export interface ExternalValue {
  engine?: string;
  version?: string;
  suppress?: boolean;
  source?: string | string[];
  rollup?: boolean;
  attributes?: Attributes;
  attributeOverrides?: Attributes;
  derivedAttributes?: Lookup<Expression>;
  delegates?: External[];
  concealBuckets?: boolean;
  mode?: QueryMode;
  dataName?: string;
  rawAttributes?: Attributes;
  filter?: Expression;
  valueExpression?: Expression;
  select?: SelectExpression;
  split?: SplitExpression;
  applies?: ApplyExpression[];
  sort?: SortExpression;
  limit?: LimitExpression;
  havingFilter?: Expression;

  // Druid

  timeAttribute?: string;
  customAggregations?: CustomDruidAggregations;
  customTransforms?: CustomDruidTransforms;
  allowEternity?: boolean;
  allowSelectQueries?: boolean;
  introspectionStrategy?: string;
  exactResultsOnly?: boolean;
  context?: Lookup<any>;

  requester?: Requester.PlywoodRequester<any>;
}

export interface ExternalJS {
  engine: string;
  version?: string;
  source?: string | string[];
  rollup?: boolean;
  attributes?: AttributeJSs;
  attributeOverrides?: AttributeJSs;
  derivedAttributes?: Lookup<ExpressionJS>;
  filter?: ExpressionJS;
  rawAttributes?: AttributeJSs;
  concealBuckets?: boolean;

  // Druid

  timeAttribute?: string;
  customAggregations?: CustomDruidAggregations;
  customTransforms?: CustomDruidTransforms;
  allowEternity?: boolean;
  allowSelectQueries?: boolean;
  introspectionStrategy?: string;
  exactResultsOnly?: boolean;
  context?: Lookup<any>;
}

export interface ApplySegregation {
  aggregateApplies: ApplyExpression[];
  postAggregateApplies: ApplyExpression[];
}

export interface AttributesAndApplies {
  attributes?: Attributes;
  applies?: ApplyExpression[];
}

export abstract class External {
  static type = 'EXTERNAL';

  static SEGMENT_NAME = '__SEGMENT__';
  static VALUE_NAME = '__VALUE__';

  static isExternal(candidate: any): candidate is External {
    return candidate instanceof External;
  }

  static extractVersion(v: string): string {
    if (!v) return null;
    let m = v.match(/^\d+\.\d+\.\d+(?:-[\w\-]+)?/);
    return m ? m[0] : null;
  }

  static versionLessThan(va: string, vb: string): boolean {
    let pa = va.split('-')[0].split('.');
    let pb = vb.split('-')[0].split('.');
    if (pa[0] !== pb[0]) return pa[0] < pb[0];
    if (pa[1] !== pb[1]) return pa[1] < pb[1];
    return pa[2] < pb[2];
  }

  static deduplicateExternals(externals: External[]): External[] {
    if (externals.length < 2) return externals;
    let uniqueExternals = [externals[0]];

    function addToUniqueExternals(external: External) {
      for (let uniqueExternal of uniqueExternals) {
        if (uniqueExternal.equalBaseAndFilter(external)) return;
      }
      uniqueExternals.push(external);
    }

    for (let i = 1; i < externals.length; i++) addToUniqueExternals(externals[i]);
    return uniqueExternals;
  }

  static addExtraFilter(ex: Expression, extraFilter: Expression): Expression {
    if (extraFilter.equals(Expression.TRUE)) return ex;

    return ex.substitute(ex => {
      if (ex instanceof RefExpression && ex.type === 'DATASET' && ex.name === External.SEGMENT_NAME) {
        return ex.filter(extraFilter);
      }
      return null;
    });
  }

  static makeZeroDatum(applies: ApplyExpression[]): Datum {
    let newDatum = Object.create(null);
    for (let apply of applies) {
      let applyName = apply.name;
      if (applyName[0] === '_') continue;
      newDatum[applyName] = 0;
    }
    return newDatum;
  }

  static normalizeAndAddApply(attributesAndApplies: AttributesAndApplies, apply: ApplyExpression): AttributesAndApplies {
    let { attributes, applies } = attributesAndApplies;

    let expressions: Lookup<Expression> = Object.create(null);
    for (let existingApply of applies) expressions[existingApply.name] = existingApply.expression;
    apply = apply.changeExpression(apply.expression.resolveWithExpressions(expressions, 'leave').simplify());

    return {
      attributes: NamedArray.overrideByName(attributes, new AttributeInfo({ name: apply.name, type: apply.expression.type })),
      applies: NamedArray.overrideByName(applies, apply)
    };
  }

  static segregationAggregateApplies(applies: ApplyExpression[]): ApplySegregation {
    let aggregateApplies: ApplyExpression[] = [];
    let postAggregateApplies: ApplyExpression[] = [];
    let nameIndex = 0;

    // First extract all the simple cases
    let appliesToSegregate: ApplyExpression[] = [];
    for (let apply of applies) {
      let applyExpression = apply.expression;
      if (applyExpression.isAggregate()) {
        // This is a vanilla aggregate, just push it in.
        aggregateApplies.push(apply);
      } else {
        appliesToSegregate.push(apply);
      }
    }

    // Now do all the segregation
    for (let apply of appliesToSegregate) {
      let newExpression = apply.expression.substitute((ex) => {
        if (ex.isAggregate()) {
          let existingApply = findApplyByExpression(aggregateApplies, ex);
          if (existingApply) {
            return $(existingApply.name, ex.type);
          } else {
            let name = '!T_' + (nameIndex++);
            aggregateApplies.push(Expression._.apply(name, ex));
            return $(name, ex.type);
          }
        }
        return null;
      });

      postAggregateApplies.push(apply.changeExpression(newExpression));
    }

    return {
      aggregateApplies,
      postAggregateApplies
    };
  }

  static getCommonFilterFromExternals(externals: External[]): Expression {
    if (!externals.length) throw new Error('must have externals');
    let commonFilter = externals[0].filter;
    for (let i = 1; i < externals.length; i++) {
      commonFilter = getCommonFilter(commonFilter, externals[i].filter);
    }
    return commonFilter;
  }

  static getMergedDerivedAttributesFromExternals(externals: External[]): Lookup<Expression> {
    if (!externals.length) throw new Error('must have externals');
    let derivedAttributes = externals[0].derivedAttributes;
    for (let i = 1; i < externals.length; i++) {
      derivedAttributes = mergeDerivedAttributes(derivedAttributes, externals[i].derivedAttributes);
    }
    return derivedAttributes;
  }

  // ==== Inflaters

  static getSimpleInflater(splitExpression: Expression, label: string): Inflater {
    switch (splitExpression.type) {
      case 'BOOLEAN': return External.booleanInflaterFactory(label);
      case 'NUMBER': return External.numberInflaterFactory(label);
      case 'TIME': return External.timeInflaterFactory(label);
      default: return null;
    }
  }

  static booleanInflaterFactory(label: string): Inflater {
    return (d: any) => {
      let v = '' + d[label];
      switch (v) {
        case 'null':
          d[label] = null;
          break;

        case '0':
        case 'false':
          d[label] = false;
          break;

        case '1':
        case 'true':
          d[label] = true;
          break;

        default:
          throw new Error("got strange result from boolean: " + v);
      }
    };
  }

  static timeRangeInflaterFactory(label: string, duration: Duration, timezone: Timezone): Inflater {
    return (d: any) => {
      let v = d[label];
      if ('' + v === "null") {
        d[label] = null;
        return;
      }

      let start = new Date(v);
      d[label] = new TimeRange({ start, end: duration.shift(start, timezone) });
    };
  }

  static numberRangeInflaterFactory(label: string, rangeSize: number): Inflater  {
    return (d: any) => {
      let v = d[label];
      if ('' + v === "null") {
        d[label] = null;
        return;
      }

      let start = Number(v);
      d[label] = new NumberRange({
        start: start,
        end: safeAdd(start, rangeSize)
      });
    };
  }

  static numberInflaterFactory(label: string): Inflater  {
    return (d: any) => {
      let v = d[label];
      if ('' + v === "null") {
        d[label] = null;
        return;
      }

      v = Number(v);
      d[label] = isNaN(v) ? null : v;
    };
  }

  static timeInflaterFactory(label: string): Inflater  {
    return (d: any) => {
      let v = d[label];
      if ('' + v === "null") {
        d[label] = null;
        return;
      }

      d[label] = new Date(v);
    };
  }

  static setStringInflaterFactory(label: string): Inflater  {
    return (d: any) => {
      let v = d[label];
      if ('' + v === "null") {
        d[label] = null;
        return;
      }

      if (typeof v === 'string') v = [v];
      d[label] = Set.fromJS({
        setType: 'STRING',
        elements: v
      });
    };
  }

  static setCardinalityInflaterFactory(label: string): Inflater  {
    return (d: any) => {
      let v = d[label];
      d[label] = Array.isArray(v) ? v.length : 1;
    };
  }

  static typeCheckDerivedAttributes(derivedAttributes: Lookup<Expression>, typeContext: DatasetFullType): Lookup<Expression> {
    let changed = false;
    let newDerivedAttributes: Lookup<Expression> = {};
    for (let k in derivedAttributes) {
      let ex = derivedAttributes[k];
      let newEx = ex.changeInTypeContext(typeContext);
      if (ex !== newEx) changed = true;
      newDerivedAttributes[k] = newEx;
    }
    return changed ? newDerivedAttributes : derivedAttributes;
  }

  static jsToValue(parameters: ExternalJS, requester: Requester.PlywoodRequester<any>): ExternalValue {
    let value: ExternalValue = {
      engine: parameters.engine,
      version: parameters.version,
      source: parameters.source,
      suppress: true,
      rollup: parameters.rollup,
      concealBuckets: Boolean(parameters.concealBuckets),
      requester
    };
    if (parameters.attributes) {
      value.attributes = AttributeInfo.fromJSs(parameters.attributes);
    }
    if (parameters.attributeOverrides) {
      value.attributeOverrides = AttributeInfo.fromJSs(parameters.attributeOverrides);
    }
    if (parameters.derivedAttributes) {
      value.derivedAttributes = Expression.expressionLookupFromJS(parameters.derivedAttributes);
    }

    value.filter = parameters.filter ? Expression.fromJS(parameters.filter) : Expression.TRUE;

    return value;
  }

  static classMap: Lookup<typeof External> = {};
  static register(ex: typeof External): void {
    let engine = (<any>ex).engine.replace(/^\w/, (s: string) => s.toLowerCase());
    External.classMap[engine] = ex;
  }

  static getConstructorFor(engine: string): typeof External {
    const ClassFn = External.classMap[engine];
    if (!ClassFn) throw new Error(`unsupported engine '${engine}'`);
    return ClassFn;
  }

  static uniteValueExternalsIntoTotal(keyExternals: { key: string, external?: External }[]): External {
    if (keyExternals.length === 0) return null;
    let applies: ApplyExpression[] = [];

    let baseExternal: External = null;
    for (let keyExternal of keyExternals) {
      let key = keyExternal.key;
      let external = keyExternal.external;
      if (!baseExternal) baseExternal = external;
      applies.push(Expression._.apply(key, new ExternalExpression({ external })));
    }

    return keyExternals[0].external.getBase().makeTotal(applies);
  }

  static fromJS(parameters: ExternalJS, requester: Requester.PlywoodRequester<any> = null): External {
    if (!hasOwnProperty(parameters, "engine")) {
      throw new Error("external `engine` must be defined");
    }
    let engine: string = parameters.engine;
    if (typeof engine !== "string") throw new Error("engine must be a string");
    let ClassFn = External.getConstructorFor(engine);

    // Back compat
    if (!requester && hasOwnProperty(parameters, 'requester')) {
      console.warn("'requester' parameter should be passed as context (2nd argument)");
      requester = (parameters as any).requester;
    }
    if (!parameters.source) {
      parameters.source = (parameters as any).dataSource || (parameters as any).table;
    }

    return ClassFn.fromJS(parameters, requester);
  }

  static fromValue(parameters: ExternalValue): External {
    const { engine } = parameters;
    let ClassFn = External.getConstructorFor(engine) as any;
    return new ClassFn(parameters);
  }

  public engine: string;
  public version: string;
  public source: string | string[];
  public suppress: boolean;
  public rollup: boolean;
  public attributes: Attributes = null;
  public attributeOverrides: Attributes = null;
  public derivedAttributes: Lookup<Expression>;
  public delegates: External[];
  public concealBuckets: boolean;

  public rawAttributes: Attributes;
  public requester: Requester.PlywoodRequester<any>;
  public mode: QueryMode;
  public filter: Expression;
  public valueExpression: Expression;
  public select: SelectExpression;
  public split: SplitExpression;
  public dataName: string;
  public applies: ApplyExpression[];
  public sort: SortExpression;
  public limit: LimitExpression;
  public havingFilter: Expression;

  constructor(parameters: ExternalValue, dummy: any = null) {
    if (dummy !== dummyObject) {
      throw new TypeError("can not call `new External` directly use External.fromJS instead");
    }
    this.engine = parameters.engine;

    let version: string = null;
    if (parameters.version) {
      version = External.extractVersion(parameters.version);
      if (!version) throw new Error(`invalid version ${parameters.version}`);
    }
    this.version = version;
    this.source = parameters.source;

    this.suppress = Boolean(parameters.suppress);
    this.rollup = Boolean(parameters.rollup);
    if (parameters.attributes) {
      this.attributes = parameters.attributes;
    }
    if (parameters.attributeOverrides) {
      this.attributeOverrides = parameters.attributeOverrides;
    }
    this.derivedAttributes = parameters.derivedAttributes || {};
    if (parameters.delegates) {
      this.delegates = parameters.delegates;
    }
    this.concealBuckets = parameters.concealBuckets;

    this.rawAttributes = parameters.rawAttributes || parameters.attributes || [];
    this.requester = parameters.requester;

    this.mode = parameters.mode || 'raw';
    this.filter = parameters.filter || Expression.TRUE;

    if (this.rawAttributes.length) {
      this.derivedAttributes = External.typeCheckDerivedAttributes(this.derivedAttributes, this.getRawFullType(true));
      this.filter = this.filter.changeInTypeContext(this.getRawFullType());
    }

    switch (this.mode) {
      case 'raw':
        this.select = parameters.select;
        this.sort = parameters.sort;
        this.limit = parameters.limit;
        break;

      case 'value':
        this.valueExpression = parameters.valueExpression;
        break;

      case 'total':
        this.applies = parameters.applies || [];
        break;

      case 'split':
        this.select = parameters.select;
        this.dataName = parameters.dataName;
        this.split = parameters.split;
        if (!this.split) throw new Error('must have split action in split mode');
        this.applies = parameters.applies || [];
        this.sort = parameters.sort;
        this.limit = parameters.limit;
        this.havingFilter = parameters.havingFilter || Expression.TRUE;
        break;
    }
  }

  protected _ensureEngine(engine: string) {
    if (!this.engine) {
      this.engine = engine;
      return;
    }
    if (this.engine !== engine) {
      throw new TypeError(`incorrect engine '${this.engine}' (needs to be: '${engine}')`);
    }
  }

  protected _ensureMinVersion(minVersion: string) {
    if (this.version && External.versionLessThan(this.version, minVersion)) {
      throw new Error(`only ${this.engine} versions >= ${minVersion} are supported`);
    }
  }

  public valueOf(): ExternalValue {
    let value: ExternalValue = {
      engine: this.engine,
      version: this.version,
      source: this.source,
      rollup: this.rollup,
      mode: this.mode
    };
    if (this.suppress) value.suppress = this.suppress;
    if (this.attributes) value.attributes = this.attributes;
    if (this.attributeOverrides) value.attributeOverrides = this.attributeOverrides;
    if (nonEmptyLookup(this.derivedAttributes)) value.derivedAttributes = this.derivedAttributes;
    if (this.delegates) value.delegates = this.delegates;
    value.concealBuckets = this.concealBuckets;

    if (this.mode !== 'raw' && this.rawAttributes) {
      value.rawAttributes = this.rawAttributes;
    }
    if (this.requester) {
      value.requester = this.requester;
    }

    if (this.dataName) {
      value.dataName = this.dataName;
    }
    value.filter = this.filter;
    if (this.valueExpression) {
      value.valueExpression = this.valueExpression;
    }
    if (this.select) {
      value.select = this.select;
    }
    if (this.split) {
      value.split = this.split;
    }
    if (this.applies) {
      value.applies = this.applies;
    }
    if (this.sort) {
      value.sort = this.sort;
    }
    if (this.limit) {
      value.limit = this.limit;
    }
    if (this.havingFilter) {
      value.havingFilter = this.havingFilter;
    }
    return value;
  }

  public toJS(): ExternalJS {
    let js: ExternalJS = {
      engine: this.engine,
      source: this.source
    };
    if (this.version) js.version = this.version;
    if (this.rollup) js.rollup = true;
    if (this.attributes) js.attributes = AttributeInfo.toJSs(this.attributes);
    if (this.attributeOverrides) js.attributeOverrides = AttributeInfo.toJSs(this.attributeOverrides);
    if (nonEmptyLookup(this.derivedAttributes)) js.derivedAttributes = Expression.expressionLookupToJS(this.derivedAttributes);
    if (this.concealBuckets) js.concealBuckets = true;

    if (this.mode !== 'raw' && this.rawAttributes) js.rawAttributes = AttributeInfo.toJSs(this.rawAttributes);
    if (!this.filter.equals(Expression.TRUE)) {
      js.filter = this.filter.toJS();
    }
    return js;
  }

  public toJSON(): ExternalJS {
    return this.toJS();
  }

  public toString(): string {
    const { mode } = this;
    switch (mode) {
      case 'raw':
        return `ExternalRaw(${this.filter})`;

      case 'value':
        return `ExternalValue(${this.valueExpression})`;

      case 'total':
        return `ExternalTotal(${this.applies.length})`;

      case 'split':
        return `ExternalSplit(${this.split}, ${this.applies.length})`;

      default:
        throw new Error(`unknown mode: ${mode}`);
    }

  }

  public equals(other: External): boolean {
    return this.equalBaseAndFilter(other) &&
      immutableLookupsEqual(this.derivedAttributes, other.derivedAttributes) &&
      immutableArraysEqual(this.attributes, other.attributes) &&
      immutableArraysEqual(this.delegates, other.delegates) &&
      this.concealBuckets === other.concealBuckets &&
      Boolean(this.requester) === Boolean(other.requester);
  }

  public equalBaseAndFilter(other: External): boolean {
    return this.equalBase(other) &&
      this.filter.equals(other.filter);
  }

  public equalBase(other: External): boolean {
    return other instanceof External &&
      this.engine === other.engine &&
      String(this.source) === String(other.source) &&
      this.version === other.version &&
      this.rollup === other.rollup &&
      this.mode === other.mode;
  }

  public changeVersion(version: string) {
    let value = this.valueOf();
    value.version = version;
    return External.fromValue(value);
  }

  public attachRequester(requester: Requester.PlywoodRequester<any>): External {
    let value = this.valueOf();
    value.requester = requester;
    return External.fromValue(value);
  }

  public versionBefore(neededVersion: string): boolean {
    const { version } = this;
    return version && External.versionLessThan(version, neededVersion);
  }

  public getAttributesInfo(attributeName: string) {
    return NamedArray.get(this.rawAttributes, attributeName);
  }

  public updateAttribute(newAttribute: AttributeInfo): External {
    if (!this.attributes) return this;
    let value = this.valueOf();
    value.attributes = AttributeInfo.override(value.attributes, [newAttribute]);
    return External.fromValue(value);
  }

  public show(): External {
    let value = this.valueOf();
    value.suppress = false;
    return External.fromValue(value);
  }

  public hasAttribute(name: string): boolean {
    const { attributes, rawAttributes, derivedAttributes } = this;
    if (SimpleArray.find(rawAttributes || attributes, (a) => a.name === name)) return true;
    return hasOwnProperty(derivedAttributes, name);
  }

  public expressionDefined(ex: Expression): boolean {
    return ex.definedInTypeContext(this.getFullType());
  }

  public bucketsConcealed(ex: Expression) {
    return ex.every((ex, index, depth, nestDiff) => {
      if (nestDiff) return true;
      if (ex instanceof RefExpression) {
        let refAttributeInfo = this.getAttributesInfo(ex.name);
        if (refAttributeInfo && refAttributeInfo.maker instanceof TimeFloorExpression) {
          return refAttributeInfo.maker.alignsWith(ex);
        }

      } else if (ex instanceof ChainableExpression) {
        let refExpression = ex.operand;
        if (refExpression instanceof RefExpression) {
          let refAttributeInfo = this.getAttributesInfo(refExpression.name);
          if (refAttributeInfo && refAttributeInfo.maker instanceof TimeFloorExpression) {
            return refAttributeInfo.maker.alignsWith(ex);
          }
        }

      }
      return null;
    });
  }

  // -----------------

  public abstract canHandleTotal(): boolean;

  public abstract canHandleFilter(filter: FilterExpression): boolean;

  public abstract canHandleSplit(split: SplitExpression): boolean;

  public abstract canHandleSplitExpression(ex: Expression): boolean;

  public abstract canHandleApply(apply: ApplyExpression): boolean;

  public abstract canHandleSort(sort: SortExpression): boolean;

  public abstract canHandleLimit(limit: LimitExpression): boolean;

  public abstract canHandleHavingFilter(havingFilter: FilterExpression): boolean;

  // -----------------

  public addDelegate(delegate: External): External {
    let value = this.valueOf();
    if (!value.delegates) value.delegates = [];
    value.delegates = value.delegates.concat(delegate);
    return External.fromValue(value);
  }

  public getBase(): External {
    let value = this.valueOf();
    value.suppress = true;
    value.mode = 'raw';
    value.dataName = null;
    if (this.mode !== 'raw') value.attributes = value.rawAttributes;
    value.rawAttributes = null;
    value.filter = null;
    value.applies = [];
    value.split = null;
    value.sort = null;
    value.limit = null;

    value.delegates = nullMap(value.delegates, (e) => e.getBase());
    return External.fromValue(value);
  }

  public getRaw(): External {
    if (this.mode === 'raw') return this;

    let value = this.valueOf();
    value.suppress = true;
    value.mode = 'raw';
    value.dataName = null;
    value.attributes = value.rawAttributes;
    value.rawAttributes = null;
    value.applies = [];
    value.split = null;
    value.sort = null;
    value.limit = null;

    value.delegates = nullMap(value.delegates, (e) => e.getRaw());
    return External.fromValue(value);
  }

  public makeTotal(applies: ApplyExpression[]): External {
    if (this.mode !== 'raw') return null;
    if (!this.canHandleTotal()) return null;

    if (!applies.length) throw new Error('must have applies');

    let externals: External[] = [];
    for (let apply of applies) {
      let applyExpression = apply.expression;
      if (applyExpression instanceof ExternalExpression) {
        externals.push(applyExpression.external);
      }
    }

    let commonFilter = External.getCommonFilterFromExternals(externals);

    let value = this.valueOf();
    value.mode = 'total';
    value.suppress = false;
    value.rawAttributes = value.attributes;
    value.derivedAttributes = External.getMergedDerivedAttributesFromExternals(externals);
    value.filter = commonFilter;
    value.attributes = [];
    value.applies = [];
    value.delegates = nullMap(value.delegates, (e) => e.makeTotal(applies));
    let totalExternal = External.fromValue(value);

    for (let apply of applies) {
      totalExternal = totalExternal._addApplyExpression(apply);
      if (!totalExternal) return null;
    }

    return totalExternal;
  }

  public addExpression(ex: Expression): External {
    if (ex instanceof FilterExpression) {
      return this._addFilterExpression(ex);
    }
    if (ex instanceof SelectExpression) {
      return this._addSelectExpression(ex);
    }
    if (ex instanceof SplitExpression) {
      return this._addSplitExpression(ex);
    }
    if (ex instanceof ApplyExpression) {
      return this._addApplyExpression(ex);
    }
    if (ex instanceof SortExpression) {
      return this._addSortExpression(ex);
    }
    if (ex instanceof LimitExpression) {
      return this._addLimitExpression(ex);
    }
    if (ex.isAggregate()) {
      return this._addAggregateExpression(ex);
    }
    return this._addPostAggregateExpression(ex);
  }

  private _addFilterExpression(filter: FilterExpression): External {
    const { expression } = filter;
    if (!expression.resolvedWithoutExternals()) return null;
    if (!this.expressionDefined(expression)) return null;

    let value = this.valueOf();
    switch (this.mode) {
      case 'raw':
        if (this.concealBuckets && !this.bucketsConcealed(expression)) return null;
        if (!this.canHandleFilter(filter)) return null;
        if (value.filter.equals(Expression.TRUE)) {
          value.filter = expression;
        } else {
          value.filter = value.filter.and(expression);
        }
        break;

      case 'split':
        if (!this.canHandleHavingFilter(filter)) return null;
        value.havingFilter = value.havingFilter.and(expression).simplify();
        break;

      default:
        return null; // can not add filter in total mode
    }

    value.delegates = nullMap(value.delegates, (e) => e._addFilterExpression(filter));
    return External.fromValue(value);
  }

  private _addSelectExpression(selectExpression: SelectExpression): External {
    const { mode } = this;
    if (mode !== 'raw' && mode !== 'split') return null; // Can only select on 'raw' or 'split' datasets

    const { datasetType } = this.getFullType();
    const { attributes } = selectExpression;
    for (let attribute of attributes) {
      if (!datasetType[attribute]) return null;
    }

    let value = this.valueOf();
    value.suppress = false;
    value.select = selectExpression;
    value.delegates = nullMap(value.delegates, (e) => e._addSelectExpression(selectExpression));

    if (mode === 'split') {
      value.applies = value.applies.filter((apply) => attributes.indexOf(apply.name) !== -1);
      value.attributes = value.attributes.filter((attribute) => attributes.indexOf(attribute.name) !== -1);
    }

    return External.fromValue(value);
  }

  private _addSplitExpression(split: SplitExpression): External {
    if (this.mode !== 'raw') return null; // Can only split on 'raw' datasets
    let splitKeys = split.keys;
    for (let splitKey of splitKeys) {
      let splitExpression = split.splits[splitKey];
      if (!this.expressionDefined(splitExpression)) return null;
      if (this.concealBuckets && !this.bucketsConcealed(splitExpression)) return null;
      if (!this.canHandleSplitExpression(splitExpression)) return null;
    }
    if (!this.canHandleSplit(split)) return null;

    let value = this.valueOf();
    value.suppress = false;
    value.mode = 'split';
    value.dataName = split.dataName;
    value.split = split;
    value.rawAttributes = value.attributes;
    value.attributes = split.mapSplits((name, expression) => new AttributeInfo({ name, type: unwrapSetType(expression.type) }));
    value.delegates = nullMap(value.delegates, (e) => e._addSplitExpression(split));
    return External.fromValue(value);
  }

  private _addApplyExpression(apply: ApplyExpression): External {
    let expression = apply.expression;
    if (expression.type === 'DATASET') return null;
    if (!expression.resolved()) return null;
    if (!this.expressionDefined(expression)) return null;
    if (!this.canHandleApply(apply)) return null;

    let value: ExternalValue;
    if (this.mode === 'raw') {
      value = this.valueOf();
      value.derivedAttributes = immutableAdd(
        value.derivedAttributes, apply.name, apply.expression
      );
    } else {
      // Can not redefine index for now.
      if (this.split && this.split.hasKey(apply.name)) return null;

      let applyExpression = apply.expression;
      if (applyExpression instanceof ExternalExpression) {
        apply = <ApplyExpression>apply.changeExpression(applyExpression.external.valueExpressionWithinFilter(this.filter));
      }

      value = this.valueOf();
      let added = External.normalizeAndAddApply(value, apply);
      value.applies = added.applies;
      value.attributes = added.attributes;
    }
    value.delegates = nullMap(value.delegates, (e) => e._addApplyExpression(apply));
    return External.fromValue(value);
  }

  private _addSortExpression(sort: SortExpression): External {
    if (this.limit) return null; // Can not sort after limit
    if (!this.canHandleSort(sort)) return null;

    let value = this.valueOf();
    value.sort = sort;
    value.delegates = nullMap(value.delegates, (e) => e._addSortExpression(sort));
    return External.fromValue(value);
  }

  private _addLimitExpression(limit: LimitExpression): External {
    if (!this.canHandleLimit(limit)) return null;

    let value = this.valueOf();
    value.suppress = false;
    if (!value.limit || limit.value < value.limit.value) {
      value.limit = limit;
    }
    value.delegates = nullMap(value.delegates, (e) => e._addLimitExpression(limit));
    return External.fromValue(value);
  }

  private _addAggregateExpression(aggregate: Expression): External {
    if (this.mode !== 'raw' || this.limit) return null; // Can not value aggregate something with a limit
    if (aggregate instanceof ChainableExpression) {
      if (aggregate instanceof ChainableUnaryExpression) {
        if (!this.expressionDefined(aggregate.expression)) return null;
      }

      let value = this.valueOf();
      value.mode = 'value';
      value.suppress = false;
      value.valueExpression = aggregate.changeOperand($(External.SEGMENT_NAME, 'DATASET'));
      value.rawAttributes = value.attributes;
      value.attributes = null;
      value.delegates = nullMap(value.delegates, (e) => e._addAggregateExpression(aggregate));
      return External.fromValue(value);
    } else {
      return null;
    }
  }

  private _addPostAggregateExpression(action: Expression): External {
    if (this.mode !== 'value') throw new Error('must be in value mode to call addPostAggregateExpression');
    if (action instanceof ChainableExpression) {
      if (!action.operand.equals(Expression._)) return null;

      let commonFilter = this.filter;
      let newValueExpression: Expression;

      if (action instanceof ChainableUnaryExpression) {
        let actionExpression = action.expression;
        if (actionExpression instanceof ExternalExpression) {
          let otherExternal = actionExpression.external;
          if (!this.equalBase(otherExternal)) return null;

          commonFilter = getCommonFilter(commonFilter, otherExternal.filter);
          let newExpression = action.changeExpression(otherExternal.valueExpressionWithinFilter(commonFilter));
          newValueExpression = this.valueExpressionWithinFilter(commonFilter).performAction(newExpression);

        } else if (!actionExpression.hasExternal()) {
          newValueExpression = this.valueExpression.performAction(action);

        } else {
          return null;
        }
      } else {
        newValueExpression = this.valueExpression.performAction(action);
      }

      let value = this.valueOf();
      value.valueExpression = newValueExpression;
      value.filter = commonFilter;
      value.delegates = nullMap(value.delegates, (e) => e._addPostAggregateExpression(action));
      return External.fromValue(value);
    } else {
      return null;
    }
  }

  public prePush(ex: ChainableUnaryExpression): External {
    if (this.mode !== 'value') return null;
    if (ex.type === 'DATASET') return null;
    if (!ex.operand.noRefs() || !ex.expression.equals(Expression._)) return null;

    let value = this.valueOf();
    value.valueExpression = ex.changeExpression(value.valueExpression);
    value.delegates = nullMap(value.delegates, (e) => e.prePush(ex));
    return External.fromValue(value);
  }

  // ----------------------

  public valueExpressionWithinFilter(withinFilter: Expression): Expression {
    if (this.mode !== 'value') return null;
    let extraFilter = filterDiff(this.filter, withinFilter);
    if (!extraFilter) throw new Error('not within the segment');
    return External.addExtraFilter(this.valueExpression, extraFilter);
  }

  public toValueApply(): ApplyExpression {
    if (this.mode !== 'value') return null;
    return Expression._.apply(External.VALUE_NAME, this.valueExpression);
  }

  public sortOnLabel(): boolean {
    let sort = this.sort;
    if (!sort) return false;

    let sortOn = (<RefExpression>sort.expression).name;
    if (!this.split || !this.split.hasKey(sortOn)) return false;

    let applies = this.applies;
    for (let apply of applies) {
      if (apply.name === sortOn) return false;
    }

    return true;
  }

  public getQuerySplit(): SplitExpression {
    return this.split.transformExpressions((ex) => {
      return this.inlineDerivedAttributes(ex);
    });
  }

  public getQueryFilter(): Expression {
    return this.inlineDerivedAttributes(this.filter).simplify();
  }

  public inlineDerivedAttributes(expression: Expression): Expression {
    const { derivedAttributes } = this;
    return expression.substitute(refEx => {
      if (refEx instanceof RefExpression) {
        let refName = refEx.name;
        return derivedAttributes[refName] || null;
      } else {
        return null;
      }
    });
  }

  public getSelectedAttributes(): Attributes {
    let { select, attributes, derivedAttributes } = this;
    attributes = attributes.slice();
    for (let k in derivedAttributes) {
      attributes.push(new AttributeInfo({ name: k, type: derivedAttributes[k].type }));
    }
    if (!select) return attributes;
    const selectAttributes = select.attributes;
    return selectAttributes.map(s => NamedArray.findByName(attributes, s));
  }

  public getValueType(): PlyTypeSimple {
    const { valueExpression } = this;
    if (!valueExpression) return null;
    return valueExpression.type as PlyTypeSimple;
  }

  // -----------------

  public addNextExternal(dataset: Dataset): Dataset {
    const { mode, dataName, split } = this;
    if (mode !== 'split') throw new Error('must be in split mode to addNextExternal');
    return dataset.applyFn(dataName, (d: Datum) => {
      return this.getRaw()._addFilterExpression(Expression._.filter(split.filterFromDatum(d)));
    }, 'DATASET');
  }

  public getDelegate(): External {
    const { mode, delegates } = this;
    if (!delegates || !delegates.length || mode === 'raw') return null;
    return delegates[0];
  }

  public simulateValue(lastNode: boolean, simulatedQueries: any[], externalForNext: External = null): PlywoodValue | TotalContainer {
    const { mode } = this;

    if (!externalForNext) externalForNext = this;

    let delegate = this.getDelegate();
    if (delegate) {
      return delegate.simulateValue(lastNode, simulatedQueries, externalForNext);
    }

    simulatedQueries.push(this.getQueryAndPostProcess().query);

    if (mode === 'value') {
      let valueExpression = this.valueExpression;
      return getSampleValue(valueExpression.type, valueExpression);
    }

    let datum: Datum = {};

    if (mode === 'raw') {
      let attributes = this.attributes;
      for (let attribute of attributes) {
        datum[attribute.name] = getSampleValue(attribute.type, null);
      }
    } else {
      if (mode === 'split') {
        this.split.mapSplits((name, expression) => {
          datum[name] = getSampleValue(unwrapSetType(expression.type), expression);
        });
      }

      let applies = this.applies;
      for (let apply of applies) {
        datum[apply.name] = getSampleValue(apply.expression.type, apply.expression);
      }
    }

    if (mode === 'total') {
      return new TotalContainer(datum);
    }

    let dataset = new Dataset({ data: [datum] });
    if (!lastNode && mode === 'split') dataset = externalForNext.addNextExternal(dataset);
    return dataset;
  }

  public getQueryAndPostProcess(): QueryAndPostProcess<any> {
    throw new Error("can not call getQueryAndPostProcess directly");
  }

  public queryValue(lastNode: boolean, externalForNext: External = null): Q.Promise<PlywoodValue | TotalContainer> {
    const { mode, requester } = this;

    if (!externalForNext) externalForNext = this;

    let delegate = this.getDelegate();
    if (delegate) {
      return delegate.queryValue(lastNode, externalForNext);
    }

    if (!requester) {
      return <Q.Promise<PlywoodValue | TotalContainer>>Q.reject(new Error('must have a requester to make queries'));
    }
    let queryAndPostProcess: QueryAndPostProcess<any>;
    try {
      queryAndPostProcess = this.getQueryAndPostProcess();
    } catch (e) {
      return <Q.Promise<PlywoodValue | TotalContainer>>Q.reject(e);
    }

    let { query, postProcess, next } = queryAndPostProcess;
    if (!query || typeof postProcess !== 'function') {
      return <Q.Promise<PlywoodValue>>Q.reject(new Error('no query or postProcess'));
    }

    let finalResult: Q.Promise<PlywoodValue | TotalContainer>;
    if (next) {
      let results: any[] = [];
      finalResult = promiseWhile(
        () => query,
        () => {
          return requester({ query })
            .then((result) => {
              results.push(result);
              query = next(query, result);
            });
        }
      )
        .then(() => {
          return queryAndPostProcess.postProcess(results);
        });
    } else {
      finalResult = requester({ query })
        .then(queryAndPostProcess.postProcess);
    }

    if (!lastNode && mode === 'split') {
      finalResult = <Q.Promise<PlywoodValue>>finalResult.then(externalForNext.addNextExternal.bind(externalForNext));
    }

    return finalResult;
  }

  // -------------------------

  public needsIntrospect(): boolean {
    return !this.rawAttributes.length;
  }

  protected abstract getIntrospectAttributes(): Q.Promise<Attributes>

  public introspect(): Q.Promise<External> {
    if (!this.requester) {
      return <Q.Promise<External>>Q.reject(new Error('must have a requester to introspect'));
    }

    if (!this.version) {
      return (this.constructor as any).getVersion(this.requester).then((version: string) => {
        version = External.extractVersion(version);
        if (!version) throw new Error('external version not found, please specify explicitly');
        return this.changeVersion(version).introspect();
      });
    }

    return this.getIntrospectAttributes()
      .then((attributes) => {
        let value = this.valueOf();

        // Apply user provided (if any) overrides to the received attributes
        if (value.attributeOverrides) {
          attributes = AttributeInfo.override(attributes, value.attributeOverrides);
        }

        // Override any existing attributes (we do not just replace them)
        if (value.attributes) {
          attributes = AttributeInfo.override(value.attributes, attributes);
        }

        value.attributes = attributes;
        // Once attributes are set attributeOverrides will be ignored
        return External.fromValue(value);
      });
  }

  public getRawFullType(skipDerived = false): DatasetFullType {
    let { rawAttributes, derivedAttributes } = this;
    if (!rawAttributes.length) throw new Error("dataset has not been introspected");

    let myDatasetType: Lookup<FullType> = {};
    for (let rawAttribute of rawAttributes) {
      let attrName = rawAttribute.name;
      myDatasetType[attrName] = {
        type: <PlyTypeSimple>rawAttribute.type
      };
    }

    if (!skipDerived) {
      for (let name in derivedAttributes) {
        myDatasetType[name] = {
          type: <PlyTypeSimple>derivedAttributes[name].type
        };
      }
    }

    return {
      type: 'DATASET',
      datasetType: myDatasetType
    };
  }

  public getFullType(): DatasetFullType {
    const { mode, attributes } = this;

    if (mode === 'value') throw new Error('not supported for value mode yet');
    let myFullType = this.getRawFullType();

    if (mode !== 'raw') {
      let splitDatasetType: Lookup<FullType> = {};
      splitDatasetType[this.dataName || External.SEGMENT_NAME] = myFullType;

      for (let attribute of attributes) {
        let attrName = attribute.name;
        splitDatasetType[attrName] = {
          type: <PlyTypeSimple>attribute.type
        };
      }

      myFullType = {
        type: 'DATASET',
        datasetType: splitDatasetType
      };
    }

    return myFullType;
  }

  // ------------------------

  /*
  private _joinDigestHelper(joinExpression: JoinExpression, action: Action): JoinExpression {
    let ids = action.expression.getExternalIds();
    if (ids.length !== 1) throw new Error('must be single dataset');
    if (ids[0] === (<External>(<LiteralExpression>joinExpression.lhs).value).getId()) {
      let lhsDigest = this.digest(joinExpression.lhs, action);
      if (!lhsDigest) return null;
      return new JoinExpression({
        op: 'join',
        lhs: lhsDigest.expression,
        rhs: joinExpression.rhs
      });
    } else {
      let rhsDigest = this.digest(joinExpression.rhs, action);
      if (!rhsDigest) return null;
      return new JoinExpression({
        op: 'join',
        lhs: joinExpression.lhs,
        rhs: rhsDigest.expression
      });
    }
  }
  */

}
