/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2020 Imply Data, Inc.
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

/* eslint-disable prefer-rest-params */
import { Duration, parseISODate, Timezone } from 'chronoshift';
import * as hasOwnProp from 'has-own-prop';
import { generalLookupsEqual, Instance, isImmutableClass } from 'immutable-class';
import { PassThrough } from 'readable-stream';

import {
  ComputeFn,
  Dataset,
  DatasetExternalAlterations,
  Datum,
  fillExpressionExternalAlteration,
  NumberRange,
  PlywoodValue,
  Range,
  Set,
  sizeOfDatasetExternalAlterations,
  StringRange,
  TimeRange,
} from '../datatypes';
import {
  failIfIntrospectNeededInDatum,
  getFullTypeFromDatum,
  introspectDatum,
} from '../datatypes/common';
import { Ip } from '../datatypes/ip';
import { iteratorFactory, PlyBit } from '../datatypes/valueStream';
import { SQLDialect } from '../dialect/baseDialect';
import { External, ExternalJS } from '../external/baseExternal';
import { promiseWhile } from '../helper/promiseWhile';
import { deduplicateSort, pipeWithError, repeat, shallowCopy } from '../helper/utils';
import { DatasetFullType, Environment, PlyType, PlyTypeSimple, PlyTypeSingleValue } from '../types';

import { AbsoluteExpression } from './absoluteExpression';
import { AddExpression } from './addExpression';
import { AndExpression } from './andExpression';
import { ApplyExpression } from './applyExpression';
import { AverageExpression } from './averageExpression';
import { CardinalityExpression } from './cardinalityExpression';
import { CastExpression } from './castExpression';
import { CollectExpression } from './collectExpression';
import { ConcatExpression } from './concatExpression';
import { ContainsExpression } from './containsExpression';
import { CountDistinctExpression } from './countDistinctExpression';
import { CountExpression } from './countExpression';
import { CustomAggregateExpression } from './customAggregateExpression';
import { CustomTransformExpression } from './customTransformExpression';
import { DivideExpression } from './divideExpression';
import { ExternalExpression } from './externalExpression';
import { ExtractExpression } from './extractExpression';
import { FallbackExpression } from './fallbackExpression';
import { FilterExpression } from './filterExpression';
import { GreaterThanExpression } from './greaterThanExpression';
import { GreaterThanOrEqualExpression } from './greaterThanOrEqualExpression';
import { IndexOfExpression } from './indexOfExpression';
import { InExpression } from './inExpression';
import { IpMatchExpression } from './ipMatchExpression';
import { IpSearchExpression } from './ipSearchExpression';
import { IpStringifyExpression } from './ipStringifyExpression';
import { IsExpression } from './isExpression';
import { JoinExpression } from './joinExpression';
import { LengthExpression } from './lengthExpression';
import { LessThanExpression } from './lessThanExpression';
import { LessThanOrEqualExpression } from './lessThanOrEqualExpression';
import { LimitExpression } from './limitExpression';
import { LiteralExpression } from './literalExpression';
import { LogExpression } from './logExpression';
import { LookupExpression } from './lookupExpression';
import { MatchExpression } from './matchExpression';
import { MaxExpression } from './maxExpression';
import { MinExpression } from './minExpression';
import { MultiplyExpression } from './multiplyExpression';
import { MvContainsExpression } from './mvContainsExpression';
import { MvFilterOnlyExpression } from './mvFilterOnlyExpression';
import { MvOverlapExpression } from './mvOverlapExpression';
import { NotExpression } from './notExpression';
import { NumberBucketExpression } from './numberBucketExpression';
import { OrExpression } from './orExpression';
import { OverlapExpression } from './overlapExpression';
import { PowerExpression } from './powerExpression';
import { QuantileExpression } from './quantileExpression';
import { RefExpression } from './refExpression';
import { SelectExpression } from './selectExpression';
import { Direction, SortExpression } from './sortExpression';
import { SplitExpression } from './splitExpression';
import { SqlAggregateExpression } from './sqlAggregateExpression';
import { SqlRefExpression } from './sqlRefExpression';
import { SubstrExpression } from './substrExpression';
import { SubtractExpression } from './subtractExpression';
import { SumExpression } from './sumExpression';
import { ThenExpression } from './thenExpression';
import { TimeBucketExpression } from './timeBucketExpression';
import { TimeFloorExpression } from './timeFloorExpression';
import { TimePartExpression } from './timePartExpression';
import { TimeRangeExpression } from './timeRangeExpression';
import { TimeShiftExpression } from './timeShiftExpression';
import { TransformCaseExpression } from './transformCaseExpression';

export interface ComputeOptions extends Environment {
  rawQueries?: any[];
  maxQueries?: number;
  maxRows?: number;
  maxComputeCycles?: number;
  concurrentQueryLimit?: number;
}

export type AlterationFillerPromise = (external: External, terminal: boolean) => Promise<any>;

function fillExpressionExternalAlterationAsync(
  alteration: ExpressionExternalAlteration,
  filler: AlterationFillerPromise,
): Promise<ExpressionExternalAlteration> {
  const tasks: Promise<any>[] = [];
  fillExpressionExternalAlteration(alteration, (external, terminal) => {
    tasks.push(filler(external, terminal));
    return null;
  });

  return Promise.all(tasks).then(results => {
    let i = 0;
    fillExpressionExternalAlteration(alteration, () => {
      const res = results[i];
      i++;
      return res;
    });
    return alteration;
  });
}

export interface ExpressionExternalAlterationSimple {
  external: External;
  terminal?: boolean;
  result?: any;
}

export type ExpressionExternalAlteration = Record<
  string,
  ExpressionExternalAlterationSimple | DatasetExternalAlterations
>;

export type BooleanExpressionIterator = (
  ex: Expression,
  index: int,
  depth: int,
  nestDiff: int,
) => boolean | null;

export type VoidExpressionIterator = (
  ex: Expression,
  index: int,
  depth: int,
  nestDiff: int,
) => void;

export type SubstitutionFn = (
  ex: Expression,
  index: int,
  depth: int,
  nestDiff: int,
  typeContext: DatasetFullType,
) => Expression | null;

export type ExpressionMatchFn = (ex: Expression) => boolean;

export interface DatasetBreakdown {
  singleDatasetActions: ApplyExpression[];
  combineExpression: Expression;
}

export interface Indexer {
  index: int;
}

export interface ExpressionTypeContext {
  expression: Expression;
  typeContext: DatasetFullType;
}

export type Alterations = Record<string, Expression>;

export interface SQLParse {
  verb: string;
  rewrite?: string;
  expression?: Expression;
  table?: string;
  database?: string;
  rest?: string;
}

export interface Splits {
  [name: string]: Expression;
}

export interface SplitsJS {
  [name: string]: ExpressionJS;
}

export type CaseType = 'upperCase' | 'lowerCase';

export interface ExpressionValue {
  op?: string;
  type?: PlyType;
  simple?: boolean;
  options?: Record<string, any>;
  operand?: Expression;
  value?: any;
  name?: string;
  nest?: int;
  external?: External;
  expression?: Expression;
  actions?: any[]; // ToDo remove
  ignoreCase?: boolean;

  dataName?: string;
  splits?: Splits;
  direction?: Direction;
  size?: number;
  offset?: number;
  duration?: Duration;
  timezone?: Timezone;
  part?: string;
  step?: number;
  position?: int;
  len?: int;
  regexp?: string;
  custom?: string;
  compare?: string;
  lookupFn?: string;
  attributes?: string[];
  transformType?: CaseType;
  outputType?: PlyTypeSimple;
  tuning?: string;
  sql?: string;
  mvArray?: (string | null)[];
  ipToSearch?: Ip;
  ipSearchType?: string;
  bounds?: string;
}

export interface ExpressionJS {
  op?: string;
  type?: PlyType;
  options?: Record<string, any>;
  value?: any;
  operand?: ExpressionJS;
  name?: string;
  nest?: int;
  external?: ExternalJS;
  expression?: ExpressionJS;
  action?: any;
  actions?: any[]; // ToDo: remove
  ignoreCase?: boolean;

  dataName?: string;
  splits?: SplitsJS;
  direction?: Direction;
  size?: number;
  offset?: number;
  duration?: string;
  timezone?: string;
  part?: string;
  step?: number;
  position?: int;
  len?: int;
  regexp?: string;
  custom?: string;
  compare?: string;
  lookupFn?: string;
  attributes?: string[];
  transformType?: CaseType;
  outputType?: PlyTypeSimple;
  tuning?: string;
  sql?: string;
  mvArray?: string[];
  ipToSearch?: Ip;
  ipSearchType?: string;
  bounds?: string;
}

export interface ExtractAndRest {
  extract: Expression;
  rest: Expression;
}

export type IfNotFound = 'throw' | 'leave' | 'null';

function runtimeAbstract() {
  return new Error('must be implemented');
}

function getDataName(ex: Expression): string {
  if (ex instanceof RefExpression) {
    return ex.name;
  } else if (ex instanceof ChainableExpression) {
    return getDataName(ex.operand);
  } else {
    return null;
  }
}

function getValue(param: any): any {
  if (param instanceof LiteralExpression) return param.value;
  return param;
}

function getString(param: string | Expression): string {
  if (typeof param === 'string') return param;
  if (param instanceof LiteralExpression && param.type === 'STRING') {
    return param.value;
  }
  if (param instanceof RefExpression && param.nest === 0) {
    return param.name;
  }
  throw new Error('could not extract a string out of ' + String(param));
}

function getNumber(param: number | Expression): number {
  if (typeof param === 'number') return param;
  if (param instanceof LiteralExpression && param.type === 'NUMBER') {
    return param.value;
  }
  throw new Error('could not extract a number out of ' + String(param));
}

// -----------------------------

/**
 * The expression starter function. It produces a native dataset with a singleton empty datum inside of it.
 * This is useful to describe the base container
 */
export function ply(dataset?: Dataset): LiteralExpression {
  if (!dataset) {
    dataset = new Dataset({
      keys: [],
      data: [{}],
    });
  }
  return r(dataset);
}

/**
 * $('blah') produces an reference lookup expression on 'blah'
 *
 * @param name The name of the column
 * @param nest (optional) the amount of nesting to add default: 0
 * @param type (optional) force the type of the reference
 */
export function $(name: string, nest?: number, type?: PlyType): RefExpression;
export function $(name: string, type?: PlyType): RefExpression;
export function $(name: string, nest?: any, type?: PlyType): RefExpression {
  if (typeof name !== 'string') throw new TypeError('$() argument must be a string');
  if (typeof nest === 'string') {
    type = nest as PlyType;
    nest = 0;
  }
  return new RefExpression({
    name,
    nest: nest != null ? nest : 0,
    type,
  });
}

export function i$(name: string, nest?: number, type?: PlyType): RefExpression {
  if (typeof name !== 'string') throw new TypeError('i$() argument must be a string');
  if (typeof nest === 'string') {
    type = nest as PlyType;
    nest = 0;
  }

  return new RefExpression({
    name,
    nest: nest != null ? nest : 0,
    type,
    ignoreCase: true,
  });
}

export function s$(sql: string, type?: PlyType): SqlRefExpression {
  if (typeof sql !== 'string') throw new TypeError('s$() argument must be a string');

  return new SqlRefExpression({ sql, type });
}

export function r(value: any): LiteralExpression {
  if (value instanceof External) throw new TypeError('r() can not accept externals');
  if (Array.isArray(value)) value = Set.fromJS(value);
  return LiteralExpression.fromJS({ op: 'literal', value: value });
}

export function toJS(thing: any): any {
  return thing && typeof thing.toJS === 'function' ? thing.toJS() : thing;
}

function chainVia(op: string, expressions: Expression[], zero: Expression): Expression {
  const n = expressions.length;
  if (!n) return zero;
  let acc = expressions[0];
  if (!(acc instanceof Expression)) acc = Expression.fromJSLoose(acc);
  for (let i = 1; i < n; i++) acc = (<any>acc)[op](expressions[i]);
  return acc;
}

export interface PEGParserOptions {
  cache?: boolean;
  allowedStartRules?: string;
  output?: string;
  optimize?: string;
  plugins?: any;
  [key: string]: any;
}

export interface PEGParser {
  parse: (str: string, options?: PEGParserOptions) => any;
}

/**
 * Provides a way to express arithmetic operations, aggregations and database operators.
 * This class is the backbone of plywood
 */
export abstract class Expression implements Instance<ExpressionValue, ExpressionJS> {
  static NULL: LiteralExpression;
  static ZERO: LiteralExpression;
  static ONE: LiteralExpression;
  static FALSE: LiteralExpression;
  static TRUE: LiteralExpression;
  static EMPTY_STRING: LiteralExpression;
  static EMPTY_SET: LiteralExpression;

  static _: RefExpression;

  static expressionParser: PEGParser;
  static defaultParserTimezone: Timezone = Timezone.UTC; // The default timezone within which dates in expressions are parsed

  static isExpression(candidate: any): candidate is Expression {
    return candidate instanceof Expression;
  }

  static expressionLookupFromJS(
    expressionJSs: Record<string, ExpressionJS>,
  ): Record<string, Expression> {
    const expressions: Record<string, Expression> = Object.create(null);
    for (const name in expressionJSs) {
      if (!hasOwnProp(expressionJSs, name)) continue;
      expressions[name] = Expression.fromJSLoose(expressionJSs[name]);
    }
    return expressions;
  }

  static expressionLookupToJS(
    expressions: Record<string, Expression>,
  ): Record<string, ExpressionJS> {
    const expressionsJSs: Record<string, ExpressionJS> = {};
    for (const name in expressions) {
      if (!hasOwnProp(expressions, name)) continue;
      expressionsJSs[name] = expressions[name].toJS();
    }
    return expressionsJSs;
  }

  /**
   * Parses an expression
   * @param str The expression to parse
   * @param timezone The timezone within which to evaluate any untimezoned date strings
   */
  static parse(str: string, timezone?: Timezone): Expression {
    if (str[0] === '{' && str[str.length - 1] === '}') {
      return Expression.fromJS(JSON.parse(str));
    }

    const original = Expression.defaultParserTimezone;
    if (timezone) Expression.defaultParserTimezone = timezone;
    try {
      return Expression.expressionParser.parse(str);
    } catch (e) {
      // Re-throw to add the stacktrace
      throw new Error(`Expression parse error: ${e.message} on '${str}'`);
    } finally {
      Expression.defaultParserTimezone = original;
    }
  }

  /**
   * Deserializes or parses an expression
   * @param param The expression to parse
   */
  static fromJSLoose(param: any): Expression {
    let expressionJS: ExpressionJS;
    // Quick parse simple expressions
    switch (typeof param) {
      case 'undefined':
        throw new Error('must have an expression');

      case 'object':
        if (param === null) {
          return Expression.NULL;
        } else if (param instanceof Expression) {
          return param;
        } else if (isImmutableClass(param)) {
          if (param.constructor.type) {
            // Must be a datatype
            expressionJS = { op: 'literal', value: param };
          } else {
            throw new Error('unknown object'); // ToDo: better error
          }
        } else if (param.op) {
          expressionJS = <ExpressionJS>param;
        } else if (param.toISOString) {
          expressionJS = { op: 'literal', value: new Date(param) };
        } else if (Array.isArray(param)) {
          expressionJS = { op: 'literal', value: Set.fromJS(param) };
        } else if (hasOwnProp(param, 'start') && hasOwnProp(param, 'end')) {
          expressionJS = { op: 'literal', value: Range.fromJS(param) };
        } else {
          throw new Error('unknown parameter');
        }
        break;

      case 'number':
      case 'boolean':
        expressionJS = { op: 'literal', value: param };
        break;

      case 'string':
        return Expression.parse(param);

      default:
        throw new Error('unrecognizable expression');
    }

    return Expression.fromJS(expressionJS);
  }

  static parseTuning(tuning: string | null): Record<string, string> {
    if (typeof tuning !== 'string') return {};
    const parts = tuning.split(',');
    const parsed: Record<string, string> = {};
    for (const part of parts) {
      const subParts = part.split('=');
      if (subParts.length !== 2) throw new Error(`can not parse tuning '${tuning}'`);
      parsed[subParts[0]] = subParts[1];
    }
    return parsed;
  }

  static safeString(str: string): string {
    return /^[a-z]\w+$/i.test(str) ? str : JSON.stringify(str);
  }

  /**
   * Composes the given expressions with an AND
   * @param expressions the array of expressions to compose
   */
  static and(expressions: Expression[]): Expression {
    return chainVia('and', expressions, Expression.TRUE);
  }

  /**
   * Composes the given expressions as E0 or E1 or ... or En
   * @param expressions the array of expressions to compose
   */
  static or(expressions: Expression[]): Expression {
    return chainVia('or', expressions, Expression.FALSE);
  }

  /**
   * Composes the given expressions as E0 + E1+ ... + En
   * @param expressions the array of expressions to compose
   */
  static add(expressions: Expression[]): Expression {
    return chainVia('add', expressions, Expression.ZERO);
  }

  /**
   * Composes the given expressions as E0 - E1- ... - En
   * @param expressions the array of expressions to compose
   */
  static subtract(expressions: Expression[]): Expression {
    return chainVia('subtract', expressions, Expression.ZERO);
  }

  static multiply(expressions: Expression[]): Expression {
    return chainVia('multiply', expressions, Expression.ONE);
  }

  static power(expressions: Expression[]): Expression {
    return chainVia('power', expressions, Expression.ZERO);
  }

  static concat(expressions: Expression[]): Expression {
    return chainVia('concat', expressions, Expression.EMPTY_STRING);
  }

  static classMap: Record<string, typeof Expression> = {};
  static register(ex: typeof Expression): void {
    const op = (<any>ex).op.replace(/^\w/, (s: string) => s.toLowerCase());
    Expression.classMap[op] = ex;
  }

  static getConstructorFor(op: string): typeof Expression {
    const ClassFn = Expression.classMap[op];
    if (!ClassFn) throw new Error(`unsupported expression op '${op}'`);
    return ClassFn;
  }

  static applyMixins(derivedCtor: any, baseCtors: any[]) {
    // From: https://www.typescriptlang.org/docs/handbook/mixins.html
    baseCtors.forEach(baseCtor => {
      Object.getOwnPropertyNames(baseCtor.prototype).forEach(name => {
        derivedCtor.prototype[name] = baseCtor.prototype[name];
      });
    });
  }

  static jsToValue(js: ExpressionJS): ExpressionValue {
    return {
      op: js.op,
      type: js.type,
      options: js.options,
    };
  }

  /**
   * Deserializes the expression JSON
   * @param expressionJS
   */
  static fromJS(expressionJS: ExpressionJS): Expression {
    if (!expressionJS) throw new Error('must have expressionJS');
    if (!hasOwnProp(expressionJS, 'op')) {
      if (hasOwnProp(expressionJS, 'action')) {
        expressionJS = shallowCopy(expressionJS);
        expressionJS.op = expressionJS.action;
        delete expressionJS.action;
        expressionJS.operand = { op: 'ref', name: '_' };
      } else {
        throw new Error('op must be defined');
      }
    }

    // Back compat.
    if (expressionJS.op === 'custom') {
      expressionJS = shallowCopy(expressionJS);
      expressionJS.op = 'customAggregate';
    }

    const op = expressionJS.op;
    if (typeof op !== 'string') {
      throw new Error('op must be a string');
    }

    // Back compat.
    if (op === 'chain') {
      const actions = expressionJS.actions || [expressionJS.action];
      return Expression.fromJS(expressionJS.expression).performActions(
        actions.map(Expression.fromJS),
      );
    }

    const ClassFn = Expression.getConstructorFor(op);
    return ClassFn.fromJS(expressionJS);
  }

  static fromValue(parameters: ExpressionValue): Expression {
    const { op } = parameters;
    const ClassFn = Expression.getConstructorFor(op) as any;
    return new ClassFn(parameters);
  }

  public op: string;
  public type: PlyType;
  public simple: boolean;
  public options?: Record<string, any>;

  constructor(parameters: ExpressionValue, dummy: any = null) {
    this.op = parameters.op;
    if (dummy !== dummyObject) {
      throw new TypeError('can not call `new Expression` directly use Expression.fromJS instead');
    }
    if (parameters.simple) this.simple = true;
    if (parameters.options) this.options = parameters.options;
  }

  protected _ensureOp(op: string) {
    if (!this.op) {
      this.op = op;
      return;
    }
    if (this.op !== op) {
      throw new TypeError(`incorrect expression op '${this.op}' (needs to be: '${op}')`);
    }
  }

  public valueOf(): ExpressionValue {
    const value: ExpressionValue = { op: this.op };
    if (this.simple) value.simple = true;
    if (this.options) value.options = this.options;
    return value;
  }

  /**
   * Serializes the expression into a simple JS object that can be passed to JSON.serialize
   */
  public toJS(): ExpressionJS {
    const js: ExpressionJS = { op: this.op };
    if (this.options) js.options = this.options;
    return js;
  }

  /**
   * Makes it safe to call JSON.serialize on expressions
   */
  public toJSON(): ExpressionJS {
    return this.toJS();
  }

  public abstract toString(indent?: int): string;

  /**
   * Validate that two expressions are equal in their meaning
   * @param other
   */
  public equals(other: Expression | undefined): boolean {
    return (
      other instanceof Expression &&
      this.op === other.op &&
      this.type === other.type &&
      generalLookupsEqual(this.options, other.options)
    );
  }

  /**
   * Check that the expression can potentially have the desired type
   * If wanted type is 'SET' then any SET/* type is matched
   * @param wantedType The type that is wanted
   */
  public canHaveType(wantedType: string): boolean {
    const { type } = this;
    if (!type || type === 'NULL') return true;
    if (wantedType === 'SET') {
      return Set.isSetType(type);
    } else {
      return type === wantedType;
    }
  }

  /**
   * Counts the number of expressions contained within this expression
   */
  public expressionCount(): int {
    return 1;
  }

  /**
   * Check if the expression is of the given operation (op)
   * @param op The operation to test
   */
  public isOp(op: string): boolean {
    return this.op === op;
  }

  public markSimple(): this {
    if (this.simple) return this;
    const value = this.valueOf();
    value.simple = true;
    return Expression.fromValue(value) as any;
  }

  /**
   * Check if the expression contains the given operation (op)
   * @param op The operation to test
   */
  public containsOp(op: string): boolean {
    return this.some((ex: Expression) => ex.isOp(op) || null);
  }

  /**
   * Check if the expression contains externals
   */
  public hasExternal(): boolean {
    return this.some((ex: Expression) => {
      if (ex instanceof ExternalExpression) return true;
      return null; // search further
    });
  }

  public getBaseExternals(): External[] {
    const externals: External[] = [];
    this.forEach((ex: Expression) => {
      if (ex instanceof ExternalExpression) externals.push(ex.external.getBase());
    });
    return External.deduplicateExternals(externals);
  }

  public getRawExternals(): External[] {
    const externals: External[] = [];
    this.forEach((ex: Expression) => {
      if (ex instanceof ExternalExpression) externals.push(ex.external.getRaw());
    });
    return External.deduplicateExternals(externals);
  }

  public getReadyExternals(limit = Infinity): ExpressionExternalAlteration {
    const indexToSkip: Record<string, boolean> = {};
    const externalsByIndex: ExpressionExternalAlteration = {};

    this.every((ex: Expression, index: int) => {
      if (limit <= 0) return null;

      if (ex instanceof ExternalExpression) {
        if (indexToSkip[index]) return null;
        if (!ex.external.suppress) {
          limit--;
          externalsByIndex[index] = {
            external: ex.external,
            terminal: true,
          };
        }
      } else if (ex instanceof ChainableExpression) {
        const h = ex._headExternal();
        if (h) {
          if (h.allGood) {
            limit--;
            externalsByIndex[index + h.offset] = { external: h.external };
            return true;
          } else {
            indexToSkip[index + h.offset] = true;
            return null;
          }
        }
      } else if (ex instanceof LiteralExpression && ex.type === 'DATASET') {
        const datasetExternals = (ex.value as Dataset).getReadyExternals(limit);
        const size = sizeOfDatasetExternalAlterations(datasetExternals);
        if (size) {
          limit -= size;
          externalsByIndex[index] = datasetExternals;
        }
        return null;
      }
      return null;
    });
    return externalsByIndex;
  }

  public applyReadyExternals(alterations: ExpressionExternalAlteration): Expression {
    return this.substitute((ex, index) => {
      const alteration = alterations[index];
      if (!alteration) return null;
      if (Array.isArray(alteration)) {
        return r((ex.getLiteralValue() as Dataset).applyReadyExternals(alteration));
      } else {
        return r(alteration.result);
      }
    }).simplify();
  }

  private _headExternal(): any {
    let ex: Expression = this;
    let allGood = true;
    let offset = 0;
    while (ex instanceof ChainableExpression) {
      allGood =
        allGood &&
        (ex.op === 'filter' ? ex.argumentsResolvedWithoutExternals() : ex.argumentsResolved());
      ex = ex.operand;
      offset++;
    }

    if (ex instanceof ExternalExpression) {
      return {
        allGood,
        external: ex.external,
        offset,
      };
    } else {
      return null;
    }
  }

  public getHeadOperand(): Expression {
    return this;
  }

  /**
   * Retrieve all free references by name
   * returns the alphabetically sorted list of the references
   */
  public getFreeReferences(): string[] {
    const freeReferences: string[] = [];
    this.forEach((ex: Expression, index: int, depth: int, nestDiff: int) => {
      if (ex instanceof RefExpression && nestDiff <= ex.nest) {
        freeReferences.push(repeat('^', ex.nest - nestDiff) + ex.name);
      }
    });
    return deduplicateSort(freeReferences);
  }

  /**
   * Retrieve all free references by index in the query
   */
  public getFreeReferenceIndexes(): number[] {
    const freeReferenceIndexes: number[] = [];
    this.forEach((ex: Expression, index: int, depth: int, nestDiff: int) => {
      if (ex instanceof RefExpression && nestDiff <= ex.nest) {
        freeReferenceIndexes.push(index);
      }
    });
    return freeReferenceIndexes;
  }

  /**
   * Increment the ^ nesting on all the free reference variables within this expression
   * @param by The number of generation to increment by (default: 1)
   */
  public incrementNesting(by: int = 1): Expression {
    const freeReferenceIndexes = this.getFreeReferenceIndexes();
    if (freeReferenceIndexes.length === 0) return this;
    return this.substitute((ex: Expression, index: int) => {
      if (ex instanceof RefExpression && freeReferenceIndexes.indexOf(index) !== -1) {
        return ex.incrementNesting(by);
      }
      return null;
    });
  }

  /**
   * Returns an expression that is equivalent but no more complex
   * If no simplification can be done will return itself.
   */
  public simplify(): Expression {
    return this;
  }

  /**
   * Runs iter over all the sub expression and return true if iter returns true for everything
   * @param iter The function to run
   * @param thisArg The this for the substitution function
   */
  public every(iter: BooleanExpressionIterator, thisArg?: any): boolean {
    return this._everyHelper(iter, thisArg, { index: 0 }, 0, 0);
  }

  public _everyHelper(
    iter: BooleanExpressionIterator,
    thisArg: any,
    indexer: Indexer,
    depth: int,
    nestDiff: int,
  ): boolean {
    const pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
    if (pass != null) {
      return pass;
    } else {
      indexer.index++;
    }
    return true;
  }

  /**
   * Runs iter over all the sub expression and return true if iter returns true for anything
   * @param iter The function to run
   * @param thisArg The this for the substitution function
   */
  public some(iter: BooleanExpressionIterator, thisArg?: any): boolean {
    return !this.every((ex: Expression, index: int, depth: int, nestDiff: int) => {
      const v = iter.call(this, ex, index, depth, nestDiff);
      return v == null ? null : !v;
    }, thisArg);
  }

  /**
   * Runs iter over all the sub expressions
   * @param iter The function to run
   * @param thisArg The this for the substitution function
   */
  public forEach(iter: VoidExpressionIterator, thisArg?: any): void {
    this.every((ex: Expression, index: int, depth: int, nestDiff: int) => {
      iter.call(this, ex, index, depth, nestDiff);
      return null;
    }, thisArg);
  }

  /**
   * Performs a substitution by recursively applying the given substitutionFn to every sub-expression
   * if substitutionFn returns an expression than it is replaced; if null is returned this expression is returned
   * @param substitutionFn The function with which to substitute
   */
  public substitute(
    substitutionFn: SubstitutionFn,
    typeContext: DatasetFullType = null,
  ): Expression {
    return this._substituteHelper(substitutionFn, { index: 0 }, 0, 0, typeContext).expression;
  }

  public _substituteHelper(
    substitutionFn: SubstitutionFn,
    indexer: Indexer,
    depth: int,
    nestDiff: int,
    typeContext: DatasetFullType,
  ): ExpressionTypeContext {
    const sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff, typeContext);
    if (sub) {
      indexer.index += this.expressionCount();
      return {
        expression: sub,
        typeContext: sub.updateTypeContextIfNeeded(typeContext),
      };
    } else {
      indexer.index++;
    }

    return {
      expression: this,
      typeContext: this.updateTypeContextIfNeeded(typeContext),
    };
  }

  public abstract getFn(): ComputeFn;

  public fullyDefined(): boolean {
    return true;
  }

  public abstract calc(datum: Datum): PlywoodValue;

  public abstract getSQL(dialect: SQLDialect): string;

  public extractFromAnd(matchFn: ExpressionMatchFn): ExtractAndRest {
    if (this.type !== 'BOOLEAN') return null;
    if (matchFn(this)) {
      return {
        extract: this,
        rest: Expression.TRUE,
      };
    } else {
      return {
        extract: Expression.TRUE,
        rest: this,
      };
    }
  }

  public breakdownByDataset(_tempNamePrefix = 'b'): DatasetBreakdown {
    throw new Error('ToDo');
    // let nameIndex = 0;
    // let singleDatasetActions: ApplyExpression[] = [];
    //
    // let externals = this.getBaseExternals();
    // if (externals.length < 2) {
    //   throw new Error('not a multiple dataset expression');
    // }
    //
    // const combine = this.substitute(ex => {
    //   let externals = ex.getBaseExternals();
    //   if (externals.length !== 1) return null;
    //
    //   let existingApply = SimpleArray.find(singleDatasetActions, (apply) => apply.expression.equals(ex));
    //
    //   let tempName: string;
    //   if (existingApply) {
    //     tempName = existingApply.name;
    //   } else {
    //     tempName = tempNamePrefix + (nameIndex++);
    //     singleDatasetActions.push(Expression._.apply(tempName, ex));
    //   }
    //
    //   return $(tempName);
    // });
    //
    // return {
    //   singleDatasetActions: singleDatasetActions,
    //   combineExpression: combine
    // };
  }

  public getLiteralValue(): any {
    return null;
  }

  public upgradeToType(_targetType: PlyType): Expression {
    return this;
  }

  public performAction(action: Expression): Expression {
    return action.substitute(ex => (ex.equals(Expression._) ? this : null));
  }

  public performActions(actions: Expression[]): Expression {
    let ex: Expression = this;
    for (const action of actions) ex = ex.performAction(action);
    return ex;
  }

  public getOptions(): Record<string, any> {
    return this.options || {};
  }

  public setOptions(options: Record<string, any> | null): this {
    const value = this.valueOf();
    value.options = options;
    return Expression.fromValue(value) as any;
  }

  public setOption(optionKey: string, optionValue: any): this {
    const newOptions = { ...this.getOptions() };
    newOptions[optionKey] = optionValue;
    return this.setOptions(newOptions);
  }

  // ------------------------------------------------------------------------
  // API behaviour

  private _mkChain<T extends ChainableUnaryExpression>(ExpressionClass: any, exs: any[]): T {
    let cur: any = this;
    for (const ex of exs) {
      cur = new ExpressionClass({
        operand: cur,
        expression: ex instanceof Expression ? ex : Expression.fromJSLoose(ex),
      });
    }
    return cur;
  }

  // Basic arithmetic

  public add(...exs: any[]) {
    return this._mkChain<AddExpression>(AddExpression, exs);
  }

  public subtract(...exs: any[]) {
    return this._mkChain<SubtractExpression>(SubtractExpression, exs);
  }

  public negate() {
    return Expression.ZERO.subtract(this);
  }

  public multiply(...exs: any[]) {
    return this._mkChain<MultiplyExpression>(MultiplyExpression, exs);
  }

  public divide(...exs: any[]) {
    return this._mkChain<DivideExpression>(DivideExpression, exs);
  }

  public reciprocate() {
    return Expression.ONE.divide(this);
  }

  public sqrt() {
    return this.power(0.5);
  }

  public power(...exs: any[]) {
    return this._mkChain<PowerExpression>(PowerExpression, exs);
  }

  public log(ex: any = Math.E) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new LogExpression({ operand: this, expression: ex });
  }

  public ln(): LogExpression {
    return new LogExpression({ operand: this, expression: r(Math.E) });
  }

  // Control flow

  public then(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new ThenExpression({ operand: this, expression: ex });
  }

  public fallback(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new FallbackExpression({ operand: this, expression: ex });
  }

  // Boolean predicates

  public is(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new IsExpression({ operand: this, expression: ex });
  }

  public isnt(ex: any) {
    return this.is(ex).not();
  }

  public lessThan(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new LessThanExpression({ operand: this, expression: ex });
  }

  public lessThanOrEqual(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new LessThanOrEqualExpression({ operand: this, expression: ex });
  }

  public greaterThan(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new GreaterThanExpression({ operand: this, expression: ex });
  }

  public greaterThanOrEqual(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new GreaterThanOrEqualExpression({ operand: this, expression: ex });
  }

  public contains(ex: any, compare?: string) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    if (compare) compare = getString(compare);
    return new ContainsExpression({ operand: this, expression: ex, compare });
  }

  public mvContains(mvArray: string[]) {
    return new MvContainsExpression({ operand: this, mvArray });
  }

  public mvFilterOnly(mvArray: string[]) {
    return new MvFilterOnlyExpression({ operand: this, mvArray });
  }

  public mvOverlap(mvArray: string[]) {
    return new MvOverlapExpression({ operand: this, mvArray });
  }

  public match(re: string) {
    return new MatchExpression({ operand: this, regexp: getString(re) });
  }

  public in(ex: any): InExpression {
    if (arguments.length === 2) {
      // Back Compat
      return this.overlap(ex, arguments[1]) as any;
    }

    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);

    // Back Compat
    if (Range.isRangeType(ex.type)) {
      return new OverlapExpression({ operand: this, expression: ex }) as any;
    }

    return new InExpression({ operand: this, expression: ex });
  }

  public overlap(ex: any, snd?: Date | number | string) {
    if (arguments.length === 2) {
      ex = getValue(ex);
      snd = getValue(snd);

      if (typeof ex === 'string') {
        const parse = parseISODate(ex, Expression.defaultParserTimezone);
        if (parse) ex = parse;
      }

      if (typeof snd === 'string') {
        const parse = parseISODate(snd, Expression.defaultParserTimezone);
        if (parse) snd = parse;
      }

      if (typeof ex === 'number' && typeof snd === 'number') {
        ex = new NumberRange({ start: ex, end: snd });
      } else if (ex.toISOString && (snd as Date).toISOString) {
        ex = new TimeRange({ start: ex, end: snd as Date });
      } else if (typeof ex === 'string' && typeof snd === 'string') {
        ex = new StringRange({ start: ex, end: snd });
      } else {
        throw new Error('uninterpretable IN parameters');
      }
    }

    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new OverlapExpression({ operand: this, expression: ex });
  }

  public not() {
    return new NotExpression({ operand: this });
  }

  public and(...exs: any[]) {
    return this._mkChain<AndExpression>(AndExpression, exs);
  }

  public or(...exs: any[]) {
    return this._mkChain<OrExpression>(OrExpression, exs);
  }

  public ipMatch(searchString: string, ipSearchType: string) {
    return new IpMatchExpression({
      operand: this,
      ipToSearch: Ip.fromString(searchString),
      ipSearchType: ipSearchType,
    });
  }

  public ipSearch(searchString: string, ipSearchType: string) {
    return new IpSearchExpression({
      operand: this,
      ipToSearch: Ip.fromString(searchString),
      ipSearchType: ipSearchType,
    });
  }

  public ipStringify() {
    return new IpStringifyExpression({ operand: this });
  }

  // String manipulation

  public substr(position: number, len: number) {
    return new SubstrExpression({
      operand: this,
      position: getNumber(position),
      len: getNumber(len),
    });
  }

  public extract(re: string) {
    return new ExtractExpression({ operand: this, regexp: getString(re) });
  }

  public concat(...exs: any[]) {
    return this._mkChain<ConcatExpression>(ConcatExpression, exs);
  }

  public lookup(lookupFn: string) {
    return new LookupExpression({ operand: this, lookupFn: getString(lookupFn) });
  }

  public indexOf(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new IndexOfExpression({ operand: this, expression: ex });
  }

  public transformCase(transformType: CaseType) {
    return new TransformCaseExpression({
      operand: this,
      transformType: getString(transformType) as CaseType,
    });
  }

  public customTransform(custom: string, outputType?: PlyTypeSingleValue) {
    if (!custom) throw new Error('Must provide an extraction function name for custom transform');
    outputType = outputType !== undefined ? (getString(outputType) as PlyTypeSingleValue) : null;
    return new CustomTransformExpression({ operand: this, custom: getString(custom), outputType });
  }

  // Number manipulation

  public numberBucket(size: number, offset = 0) {
    return new NumberBucketExpression({
      operand: this,
      size: getNumber(size),
      offset: getNumber(offset),
    });
  }

  public absolute() {
    return new AbsoluteExpression({ operand: this });
  }

  public length() {
    return new LengthExpression({ operand: this });
  }

  // Time manipulation

  public timeBucket(duration: any, timezone?: any) {
    if (!(duration instanceof Duration)) duration = Duration.fromJS(getString(duration));
    if (timezone && !(timezone instanceof Timezone))
      timezone = Timezone.fromJS(getString(timezone));
    return new TimeBucketExpression({ operand: this, duration, timezone });
  }

  public timeFloor(duration: any, timezone?: any) {
    if (!(duration instanceof Duration)) duration = Duration.fromJS(getString(duration));
    if (timezone && !(timezone instanceof Timezone))
      timezone = Timezone.fromJS(getString(timezone));
    return new TimeFloorExpression({ operand: this, duration, timezone });
  }

  public timeShift(duration: any, step?: number, timezone?: any) {
    if (!(duration instanceof Duration)) duration = Duration.fromJS(getString(duration));
    step = typeof step !== 'undefined' ? getNumber(step) : null;
    if (timezone && !(timezone instanceof Timezone))
      timezone = Timezone.fromJS(getString(timezone));
    return new TimeShiftExpression({ operand: this, duration, step, timezone });
  }

  public timeRange(duration: any, step?: number, timezone?: any) {
    if (!(duration instanceof Duration)) duration = Duration.fromJS(getString(duration));
    step = typeof step !== 'undefined' ? getNumber(step) : null;
    if (timezone && !(timezone instanceof Timezone))
      timezone = Timezone.fromJS(getString(timezone));
    return new TimeRangeExpression({ operand: this, duration, step, timezone });
  }

  public timePart(part: string, timezone?: any) {
    if (timezone && !(timezone instanceof Timezone))
      timezone = Timezone.fromJS(getString(timezone));
    return new TimePartExpression({ operand: this, part: getString(part), timezone });
  }

  public cast(outputType: PlyType) {
    return new CastExpression({
      operand: this,
      outputType: getString(outputType) as PlyTypeSimple,
    });
  }

  // Set operations

  public cardinality() {
    return new CardinalityExpression({ operand: this });
  }

  // Split Apply Combine based transformations

  /**
   * Filter the dataset with a boolean expression
   * Only works on expressions that return DATASET
   * @param ex A boolean expression to filter on
   */
  public filter(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new FilterExpression({ operand: this, expression: ex });
  }

  public split(splits: any, dataName?: string): SplitExpression;
  // eslint-disable-next-line @typescript-eslint/unified-signatures
  public split(ex: any, name: string, dataName?: string): SplitExpression;
  public split(splits: any, name?: string, dataName?: string): SplitExpression {
    // Determine if use case #2 (ex + name)
    if (
      arguments.length === 3 ||
      ((arguments.length === 2 || arguments.length === 1) &&
        (typeof splits === 'string' || typeof splits.op === 'string'))
    ) {
      name = arguments.length === 1 ? 'split' : getString(name);
      const realSplits = Object.create(null);
      realSplits[name] = splits;
      splits = realSplits;
    } else {
      dataName = name;
    }

    const parsedSplits: Splits = Object.create(null);
    for (const k in splits) {
      if (!hasOwnProp(splits, k)) continue;
      const ex = splits[k];
      parsedSplits[k] = ex instanceof Expression ? ex : Expression.fromJSLoose(ex);
    }

    dataName = dataName ? getString(dataName) : getDataName(this);
    if (!dataName)
      throw new Error('could not guess data name in `split`, please provide one explicitly');
    return new SplitExpression({ operand: this, splits: parsedSplits, dataName: dataName });
  }

  /**
   * Evaluate some expression on every datum in the dataset. Record the result as `name`
   * @param name The name of where to store the results
   * @param ex The expression to evaluate
   */
  public apply(name: string, ex: any) {
    if (arguments.length < 2)
      throw new Error('invalid arguments to .apply, did you forget to specify a name?');
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new ApplyExpression({ operand: this, name: getString(name), expression: ex });
  }

  public sort(ex: any, direction?: Direction) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new SortExpression({
      operand: this,
      expression: ex,
      direction: direction ? (getString(direction) as Direction) : null,
    });
  }

  public limit(value: number) {
    return new LimitExpression({ operand: this, value: getNumber(value) });
  }

  public select(attributes: string[]): SelectExpression;
  public select(...attributes: any[]): SelectExpression {
    attributes =
      attributes.length === 1 && Array.isArray(attributes[0])
        ? attributes[0]
        : attributes.map(getString);
    return new SelectExpression({ operand: this, attributes });
  }

  // Aggregate expressions

  public count() {
    if (arguments.length)
      throw new Error('.count() should not have arguments, did you want to .filter().count() ?');
    return new CountExpression({ operand: this });
  }

  public sum(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new SumExpression({ operand: this, expression: ex });
  }

  public min(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new MinExpression({ operand: this, expression: ex });
  }

  public max(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new MaxExpression({ operand: this, expression: ex });
  }

  public average(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new AverageExpression({ operand: this, expression: ex });
  }

  public countDistinct(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new CountDistinctExpression({ operand: this, expression: ex });
  }

  public quantile(ex: any, value: number, tuning?: string) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new QuantileExpression({
      operand: this,
      expression: ex,
      value: getNumber(value),
      tuning: tuning ? getString(tuning) : null,
    });
  }

  public collect(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new CollectExpression({ operand: this, expression: ex });
  }

  public customAggregate(custom: string) {
    return new CustomAggregateExpression({ operand: this, custom: getString(custom) });
  }

  public sqlAggregate(sql: string) {
    return new SqlAggregateExpression({ operand: this, sql: getString(sql) });
  }

  // Undocumented (for now)

  public join(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new JoinExpression({ operand: this, expression: ex });
  }

  public needsEnvironment(): boolean {
    return false;
  }

  /**
   * Rewrites the expression with all the references typed correctly and resolved to the correct parental level
   * @param environment The environment that will be defined
   */
  public defineEnvironment(environment: Environment): Expression {
    if (!environment.timezone) environment = { timezone: Timezone.UTC };

    // Allow strings as well
    if (typeof environment.timezone === 'string')
      environment = { timezone: Timezone.fromJS(environment.timezone as any) };

    return this.substitute(ex => {
      if (ex.needsEnvironment()) {
        return ex.defineEnvironment(environment);
      }
      return null;
    });
  }

  /**
   * Rewrites the expression with all the references typed correctly and resolved to the correct parental level
   * @param context The datum within which the check is happening
   */
  public referenceCheck(context: Datum): Expression {
    return this.changeInTypeContext(getFullTypeFromDatum(context));
  }

  /**
   * Check if the expression is defined in the given type context
   * @param typeContext The FullType context within which to resolve
   */
  public definedInTypeContext(typeContext: DatasetFullType): boolean {
    try {
      this.changeInTypeContext(typeContext);
    } catch (e) {
      return false;
    }
    return true;
  }

  /**
   * Check if the expression is defined in the given type context
   * @param typeContext The FullType context within which to resolve
   * @deprecated
   */
  public referenceCheckInTypeContext(typeContext: DatasetFullType): Expression {
    console.warn(`referenceCheckInTypeContext is deprecated, use changeInTypeContext instead`);
    return this.changeInTypeContext(typeContext);
  }

  /**
   * Rewrites the expression with all the references typed correctly and resolved to the correct parental level
   * @param typeContext The FullType context within which to resolve
   */
  public changeInTypeContext(typeContext: DatasetFullType): Expression {
    return this.substitute(
      (ex: Expression, index: int, depth: int, nestDiff: int, typeContext: DatasetFullType) => {
        if (ex instanceof RefExpression) {
          return ex.changeInTypeContext(typeContext);
        }
        return null;
      },
      typeContext,
    );
  }

  public updateTypeContext(typeContext: DatasetFullType, _extra?: any): DatasetFullType {
    return typeContext;
  }

  public updateTypeContextIfNeeded(
    typeContext: DatasetFullType | null,
    extra?: any,
  ): DatasetFullType | null {
    return typeContext ? this.updateTypeContext(typeContext, extra) : null;
  }

  /**
   * Resolves one level of dependencies that refer outside of this expression.
   * @param context The context containing the values to resolve to
   * @param ifNotFound If the reference is not in the context what to do? "throw", "leave", "null"
   * @return The resolved expression
   */
  public resolve(context: Datum, ifNotFound: IfNotFound = 'throw'): Expression {
    const expressions: Record<string, Expression> = Object.create(null);
    for (const k in context) {
      if (!hasOwnProp(context, k)) continue;
      const value = context[k];
      if (value instanceof External) {
        expressions[k] = new ExternalExpression({ external: value });
      } else if (value instanceof Expression) {
        expressions[k] = value;
      } else {
        expressions[k] = new LiteralExpression({ value });
      }
    }

    return this.resolveWithExpressions(expressions, ifNotFound);
  }

  public resolveWithExpressions(
    expressions: Record<string, Expression>,
    ifNotFound: IfNotFound = 'throw',
  ): Expression {
    return this.substitute((ex: Expression, index: int, depth: int, nestDiff: int) => {
      if (ex instanceof RefExpression) {
        const { nest, ignoreCase, name } = ex;
        if (nestDiff === nest) {
          let foundExpression: Expression = null;
          let valueFound = false;
          const property = ignoreCase
            ? RefExpression.findPropertyCI(expressions, name)
            : RefExpression.findProperty(expressions, name);
          if (property != null) {
            foundExpression = expressions[property];
            valueFound = true;
          }

          if (foundExpression instanceof ExternalExpression) {
            const mode = foundExpression.external.mode;

            // Never substitute split externals at all
            if (mode === 'split') {
              return ex;
            }

            // Never substitute non-raw externals from an outside nesting
            if (nest > 0 && mode !== 'raw') {
              return ex;
            }
          }

          if (valueFound) {
            return foundExpression;
          } else if (ifNotFound === 'throw') {
            throw new Error(`could not resolve ${ex} because is was not in the context`);
          } else if (ifNotFound === 'null') {
            return Expression.NULL;
          } else if (ifNotFound === 'leave') {
            return ex;
          }
        } else if (nestDiff < nest) {
          throw new Error(`went too deep during resolve on: ${ex}`);
        }
      }
      return null;
    });
  }

  public resolved(): boolean {
    return this.every((ex, index, depth, nestDiff) => {
      return ex instanceof RefExpression ? ex.nest <= nestDiff : null; // Search within
    });
  }

  public resolvedWithoutExternals(): boolean {
    return this.every((ex, index, depth, nestDiff) => {
      if (ex instanceof ExternalExpression) return false;
      return ex instanceof RefExpression ? ex.nest <= nestDiff : null; // Search within
    });
  }

  public noRefs(): boolean {
    return this.every(ex => {
      if (ex instanceof RefExpression) return false;
      return null;
    });
  }

  public isAggregate(): boolean {
    return false;
  }

  /**
   * Decompose instances of $data.average($x) into $data.sum($x) / $data.count()
   * @param countEx and optional expression to use in a sum instead of a count
   */
  public decomposeAverage(countEx?: Expression): Expression {
    return this.substitute(ex => {
      if (ex instanceof AverageExpression) {
        return ex.decomposeAverage(countEx);
      }
      return null;
    });
  }

  /**
   * Apply the distributive law wherever possible to aggregates
   * Turns $data.sum($x - 2 * $y) into $data.sum($x) - 2 * $data.sum($y)
   */
  public distribute(): Expression {
    return this.substitute((ex: Expression, index: int) => {
      if (index === 0) return null;
      const distributedEx = ex.distribute();
      if (distributedEx === ex) return null;
      return distributedEx;
    }).simplify();
  }

  /**
   * Returns the maximum number of possible values this expression can return in a split context
   */
  public maxPossibleSplitValues(): number {
    return this.type === 'BOOLEAN' ? 3 : Infinity;
  }

  // ---------------------------------------------------------
  // Evaluation

  private _initialPrepare(context: Datum, environment: Environment): Expression {
    return this.defineEnvironment(environment).referenceCheck(context).resolve(context).simplify();
  }

  /**
   * Simulates computing the expression and returns the results with the right shape (but fake data)
   * @param context The context within which to compute the expression
   * @param options The options within which to evaluate
   */
  public simulate(context: Datum = {}, options: ComputeOptions = {}): PlywoodValue {
    failIfIntrospectNeededInDatum(context);

    let readyExpression = this._initialPrepare(context, options);
    if (readyExpression instanceof ExternalExpression) {
      // Top level externals need to be unsuppressed
      readyExpression = readyExpression.unsuppress();
    }

    return readyExpression._computeResolvedSimulate(options, []);
  }

  /**
   * Simulates computing the expression and returns the queries that would have been made
   * @param context The context within which to compute the expression
   * @param options The options within which to evaluate
   */
  public simulateQueryPlan(context: Datum = {}, options: ComputeOptions = {}): any[][] {
    failIfIntrospectNeededInDatum(context);

    let readyExpression = this._initialPrepare(context, options);
    if (readyExpression instanceof ExternalExpression) {
      // Top level externals need to be unsuppressed
      readyExpression = readyExpression.unsuppress();
    }

    const simulatedQueryGroups: any[] = [];
    readyExpression._computeResolvedSimulate(options, simulatedQueryGroups);
    return simulatedQueryGroups;
  }

  private _computeResolvedSimulate(
    options: ComputeOptions,
    simulatedQueryGroups: any[][],
  ): PlywoodValue {
    const {
      maxComputeCycles = 5,
      maxQueries = 500,
      maxRows,
      concurrentQueryLimit = Infinity,
    } = options;

    let ex: Expression = this;
    let readyExternals = ex.getReadyExternals(concurrentQueryLimit);

    let computeCycles = 0;
    let queries = 0;

    while (
      Object.keys(readyExternals).length > 0 &&
      computeCycles < maxComputeCycles &&
      queries < maxQueries
    ) {
      const simulatedQueryGroup: any[] = [];
      fillExpressionExternalAlteration(readyExternals, (external, terminal) => {
        if (queries < maxQueries) {
          queries++;
          return external.simulateValue(terminal, simulatedQueryGroup);
        } else {
          queries++;
          return null; // Query limit reached, don't do any more queries.
        }
      });

      simulatedQueryGroups.push(simulatedQueryGroup);
      ex = ex.applyReadyExternals(readyExternals);
      const literalValue = ex.getLiteralValue();
      if (maxRows && literalValue instanceof Dataset) {
        ex = r(literalValue.depthFirstTrimTo(maxRows));
      }
      readyExternals = ex.getReadyExternals(concurrentQueryLimit);
      computeCycles++;
    }
    return ex.getLiteralValue();
  }

  /**
   * Computes a general asynchronous expression
   * @param context The context within which to compute the expression
   * @param options The options determining computation
   */
  public compute(context: Datum = {}, options: ComputeOptions = {}): Promise<PlywoodValue> {
    return Promise.resolve(null)
      .then(() => {
        return introspectDatum(context);
      })
      .then((introspectedContext: Datum) => {
        let readyExpression = this._initialPrepare(introspectedContext, options);
        if (readyExpression instanceof ExternalExpression) {
          // Top level externals need to be unsuppressed
          readyExpression = readyExpression.unsuppress();
        }
        return readyExpression._computeResolved(options);
      });
  }

  /**
   * Computes a general asynchronous expression and streams the results
   * @param context The context within which to compute the expression
   * @param options The options determining computation
   */
  public computeStream(context: Datum = {}, options: ComputeOptions = {}): ReadableStream {
    const pt = new PassThrough({ objectMode: true });

    const rawQueries = options.rawQueries;

    introspectDatum(context)
      .then((introspectedContext: Datum) => {
        const readyExpression = this._initialPrepare(introspectedContext, options);
        if (readyExpression instanceof ExternalExpression) {
          // Top level externals need to be unsuppressed
          // readyExpression = readyExpression.unsuppress();
          pipeWithError(readyExpression.external.queryValueStream(true, rawQueries), pt);
          return;
        }

        void readyExpression._computeResolved(options).then(v => {
          const i = iteratorFactory(v as Dataset);
          let bit: PlyBit;
          while ((bit = i())) {
            pt.write(bit);
          }
          pt.end();
        });
      })
      .catch(e => {
        pt.emit('error', e);
      });

    return pt as any;
  }

  private _computeResolved(options: ComputeOptions): Promise<PlywoodValue> {
    const {
      rawQueries,
      maxComputeCycles = 5,
      maxQueries = 500,
      maxRows,
      concurrentQueryLimit = Infinity,
    } = options;

    let ex: Expression = this;
    let readyExternals = ex.getReadyExternals(concurrentQueryLimit);

    let computeCycles = 0;
    let queriesMade = 0;

    return promiseWhile(
      () =>
        Object.keys(readyExternals).length > 0 &&
        computeCycles < maxComputeCycles &&
        queriesMade < maxQueries,
      async () => {
        const readyExternalsFilled = await fillExpressionExternalAlterationAsync(
          readyExternals,
          (external, terminal) => {
            if (queriesMade < maxQueries) {
              queriesMade++;
              return external.queryValue(terminal, rawQueries);
            } else {
              queriesMade++;
              return Promise.resolve(null); // Query limit reached, don't do any more queries.
            }
          },
        );

        ex = ex.applyReadyExternals(readyExternalsFilled);
        const literalValue = ex.getLiteralValue();
        if (maxRows && literalValue instanceof Dataset) {
          ex = r(literalValue.depthFirstTrimTo(maxRows));
        }
        readyExternals = ex.getReadyExternals(concurrentQueryLimit);
        computeCycles++;
      },
    ).then(() => {
      if (!ex.isOp('literal')) throw new Error(`something went wrong, did not get literal: ${ex}`);
      return ex.getLiteralValue();
    });
  }
}

export abstract class ChainableExpression extends Expression {
  static jsToValue(js: ExpressionJS): ExpressionValue {
    const value = Expression.jsToValue(js);
    value.operand = js.operand ? Expression.fromJS(js.operand) : Expression._;
    return value;
  }

  public operand: Expression;

  constructor(value: ExpressionValue, dummy: any = null) {
    super(value, dummy);
    this.operand = value.operand || Expression._;
  }

  protected _checkTypeAgainstTypes(name: string, type: string, neededTypes: string[]) {
    if (type && type !== 'NULL' && neededTypes.indexOf(type) === -1) {
      if (neededTypes.length === 1) {
        throw new Error(`${this.op} must have ${name} of type ${neededTypes[0]} (is ${type})`);
      } else {
        throw new Error(
          `${this.op} must have ${name} of type ${neededTypes.join(' or ')} (is ${type})`,
        );
      }
    }
  }

  protected _checkOperandTypes(...neededTypes: string[]) {
    this._checkTypeAgainstTypes('operand', Set.unwrapSetType(this.operand.type), neededTypes);
  }

  protected _checkOperandTypesStrict(...neededTypes: string[]) {
    this._checkTypeAgainstTypes('operand', this.operand.type, neededTypes);
  }

  protected _bumpOperandToTime() {
    if (this.operand.type === 'STRING') {
      this.operand = this.operand.upgradeToType('TIME');
    }
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.operand = this.operand;
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    if (!this.operand.equals(Expression._)) {
      js.operand = this.operand.toJS();
    }
    return js;
  }

  protected _toStringParameters(_indent?: int): string[] {
    return [];
  }

  public toString(indent?: int): string {
    return `${this.operand.toString(indent)}.${this.op}(${this._toStringParameters(indent).join(
      ',',
    )})`;
  }

  public equals(other: ChainableExpression | undefined): boolean {
    return super.equals(other) && this.operand.equals(other.operand);
  }

  public changeOperand(operand: Expression): this {
    if (this.operand === operand || this.operand.equals(operand)) return this;

    const value = this.valueOf();
    value.operand = operand;
    delete value.simple;
    return Expression.fromValue(value) as any;
  }

  public swapWithOperand(): ChainableExpression {
    const { operand } = this;
    if (operand instanceof ChainableExpression) {
      return operand.changeOperand(this.changeOperand(operand.operand));
    } else {
      throw new Error('operand must be chainable');
    }
  }

  public getAction(): Expression {
    return this.changeOperand(Expression._);
  }

  public getHeadOperand(): Expression {
    let iter = this.operand;
    while (iter instanceof ChainableExpression) iter = iter.operand;
    return iter;
  }

  public getArgumentExpressions(): Expression[] {
    return [];
  }

  public expressionCount(): int {
    let sum = super.expressionCount() + this.operand.expressionCount();
    this.getArgumentExpressions().forEach(ex => (sum += ex.expressionCount()));
    return sum;
  }

  public argumentsResolved(): boolean {
    return this.getArgumentExpressions().every(ex => ex.resolved());
  }

  public argumentsResolvedWithoutExternals(): boolean {
    return this.getArgumentExpressions().every(ex => ex.resolvedWithoutExternals());
  }

  public getFn(): ComputeFn {
    // ToDo: this should be moved into Expression
    return (d: Datum) => this.calc(d);
  }

  protected _calcChainableHelper(_operandValue: any): PlywoodValue {
    throw runtimeAbstract();
  }

  public fullyDefined(): boolean {
    return this.operand.isOp('literal');
  }

  public calc(datum: Datum): PlywoodValue {
    return this._calcChainableHelper(this.operand.calc(datum));
  }

  protected _getSQLChainableHelper(_dialect: SQLDialect, _operandSQL: string): string {
    throw runtimeAbstract();
  }

  public getSQL(dialect: SQLDialect): string {
    return this._getSQLChainableHelper(dialect, this.operand.getSQL(dialect));
  }

  public pushIntoExternal(): Expression | null {
    const { operand } = this;
    if (operand instanceof ExternalExpression) {
      return operand.addExpression(this.getAction());
    }
    return null;
  }

  protected specialSimplify(): Expression {
    return this;
  }

  public simplify(): Expression {
    if (this.simple) return this;

    let simpler: Expression = this.changeOperand(this.operand.simplify());

    if (simpler.fullyDefined()) {
      return r(simpler.calc({}));
    }

    const specialSimpler = (simpler as ChainableUnaryExpression).specialSimplify();
    if (specialSimpler === simpler) {
      simpler = specialSimpler.markSimple();
    } else {
      simpler = specialSimpler.simplify();
    }

    if (simpler instanceof ChainableExpression) {
      const pushedInExternal = simpler.pushIntoExternal();
      if (pushedInExternal) return pushedInExternal;
    }

    return simpler;
  }

  public isNester(): boolean {
    return false;
  }

  public _everyHelper(
    iter: BooleanExpressionIterator,
    thisArg: any,
    indexer: Indexer,
    depth: int,
    nestDiff: int,
  ): boolean {
    const pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
    if (pass != null) {
      return pass;
    } else {
      indexer.index++;
    }
    depth++;

    const operand = this.operand;
    if (!operand._everyHelper(iter, thisArg, indexer, depth, nestDiff)) return false;

    const nestDiffNext = nestDiff + Number(this.isNester());
    return this.getArgumentExpressions().every(ex =>
      ex._everyHelper(iter, thisArg, indexer, depth, nestDiffNext),
    );
  }

  public _substituteHelper(
    substitutionFn: SubstitutionFn,
    indexer: Indexer,
    depth: int,
    nestDiff: int,
    typeContext: DatasetFullType,
  ): ExpressionTypeContext {
    const sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff, typeContext);
    if (sub) {
      indexer.index += this.expressionCount();
      return {
        expression: sub,
        typeContext: sub.updateTypeContextIfNeeded(typeContext),
      };
    } else {
      indexer.index++;
    }
    depth++;

    const operandSubs = this.operand._substituteHelper(
      substitutionFn,
      indexer,
      depth,
      nestDiff,
      typeContext,
    );
    const updatedThis = this.changeOperand(operandSubs.expression);

    return {
      expression: updatedThis,
      typeContext: updatedThis.updateTypeContextIfNeeded(operandSubs.typeContext),
    };
  }
}

export abstract class ChainableUnaryExpression extends ChainableExpression {
  static jsToValue(js: ExpressionJS): ExpressionValue {
    const value = ChainableExpression.jsToValue(js);
    value.expression = Expression.fromJS(js.expression);
    return value;
  }

  public expression: Expression;

  constructor(value: ExpressionValue, dummy: any = null) {
    super(value, dummy);
    if (!value.expression) throw new Error(`must have an expression`);
    this.expression = value.expression;
  }

  protected _checkExpressionTypes(...neededTypes: string[]) {
    this._checkTypeAgainstTypes('expression', Set.unwrapSetType(this.expression.type), neededTypes);
  }

  protected _checkExpressionTypesStrict(...neededTypes: string[]) {
    this._checkTypeAgainstTypes('expression', this.expression.type, neededTypes);
  }

  protected _checkOperandExpressionTypesAlign() {
    const operandType = Set.unwrapSetType(this.operand.type);
    const expressionType = Set.unwrapSetType(this.expression.type);
    if (
      !operandType ||
      operandType === 'NULL' ||
      !expressionType ||
      expressionType === 'NULL' ||
      operandType === expressionType
    )
      return;
    throw new Error(
      `${this.op} must have matching types (are ${this.operand.type}, ${this.expression.type})`,
    );
  }

  protected _bumpOperandExpressionToTime() {
    if (this.expression.type === 'TIME' && this.operand.type === 'STRING') {
      this.operand = this.operand.upgradeToType('TIME');
    }

    if (this.operand.type === 'TIME' && this.expression.type === 'STRING') {
      this.expression = this.expression.upgradeToType('TIME');
    }
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.expression = this.expression;
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    js.expression = this.expression.toJS();
    return js;
  }

  protected _toStringParameters(indent?: int): string[] {
    return [this.expression.toString(indent)];
  }

  public toString(indent?: int): string {
    // ToDo: handle indent
    return `${this.operand.toString(indent)}.${this.op}(${this._toStringParameters(indent).join(
      ',',
    )})`;
  }

  public equals(other: ChainableUnaryExpression | undefined): boolean {
    return super.equals(other) && this.expression.equals(other.expression);
  }

  public changeExpression(expression: Expression): this {
    if (this.expression === expression || this.expression.equals(expression)) return this;

    const value = this.valueOf();
    value.expression = expression;
    delete value.simple;
    return Expression.fromValue(value) as any;
  }

  protected _calcChainableUnaryHelper(_operandValue: any, _expressionValue: any): PlywoodValue {
    throw runtimeAbstract();
  }

  public fullyDefined(): boolean {
    return this.operand.isOp('literal') && this.expression.isOp('literal');
  }

  public calc(datum: Datum): PlywoodValue {
    return this._calcChainableUnaryHelper(
      this.operand.calc(datum),
      this.isNester() ? null : this.expression.calc(datum),
    );
  }

  protected _getSQLChainableUnaryHelper(
    _dialect: SQLDialect,
    _operandSQL: string,
    _expressionSQL: string,
  ): string {
    throw runtimeAbstract();
  }

  public getSQL(dialect: SQLDialect): string {
    return this._getSQLChainableUnaryHelper(
      dialect,
      this.operand.getSQL(dialect),
      this.expression.getSQL(dialect),
    );
  }

  public getExpressionList(): Expression[] {
    const { op, operand, expression } = this;
    const expressionList = [expression];
    let iter = operand;
    while (iter.op === op) {
      expressionList.unshift((iter as ChainableUnaryExpression).expression);
      iter = (iter as ChainableUnaryExpression).operand;
    }
    expressionList.unshift(iter);
    return expressionList;
  }

  public isCommutative(): boolean {
    return false;
  }

  public isAssociative(): boolean {
    return false;
  }

  public associateLeft(): this | null {
    if (!this.isAssociative()) return null;
    const { op, operand, expression } = this;
    if (op !== expression.op) return null;
    const MyClass: any = this.constructor;

    return new MyClass({
      operand: new MyClass({
        operand: operand,
        expression: (expression as ChainableUnaryExpression).operand,
      }),
      expression: (expression as ChainableUnaryExpression).expression,
    });
  }

  public associateRightIfSimpler(): this | null {
    if (!this.isAssociative()) return null;
    const { op, operand, expression } = this;
    if (op !== operand.op) return null;
    const MyClass: any = this.constructor;

    const simpleExpression = new MyClass({
      operand: (operand as ChainableUnaryExpression).expression,
      expression: expression,
    }).simplify();

    if (simpleExpression instanceof LiteralExpression) {
      return new MyClass({
        operand: (operand as ChainableUnaryExpression).operand,
        expression: simpleExpression,
      }).simplify();
    } else {
      return null;
    }
  }

  public pushIntoExternal(): Expression | null {
    const { operand, expression } = this;
    if (operand instanceof ExternalExpression) {
      return operand.addExpression(this.getAction());
    }
    if (expression instanceof ExternalExpression) {
      return expression.prePush(this.changeExpression(Expression._));
    }
    return null;
  }

  public simplify(): Expression {
    if (this.simple) return this;

    const simpleOperand = this.operand.simplify();
    const simpleExpression = this.expression.simplify();
    let simpler: Expression = this.changeOperand(simpleOperand).changeExpression(simpleExpression);
    if (simpler.fullyDefined()) return r(simpler.calc({}));

    if (this.isCommutative() && simpleOperand instanceof LiteralExpression) {
      // Swap!
      const MyClass: any = this.constructor;
      const myValue = this.valueOf();
      myValue.operand = simpleExpression;
      myValue.expression = simpleOperand;
      return new MyClass(myValue).simplify();
    }

    // Auto associate left if possible
    const assLeft = (simpler as ChainableUnaryExpression).associateLeft();
    if (assLeft) return assLeft.simplify();

    if (simpler instanceof ChainableUnaryExpression) {
      const specialSimpler = simpler.specialSimplify();
      if (specialSimpler !== simpler) {
        return specialSimpler.simplify();
      } else {
        simpler = specialSimpler;
      }

      if (simpler instanceof ChainableUnaryExpression) {
        // Try to associate right and pick that if that is simpler
        const assRight = simpler.associateRightIfSimpler();
        if (assRight) return assRight;
      }
    }

    simpler = simpler.markSimple();

    if (simpler instanceof ChainableExpression) {
      const pushedInExternal = simpler.pushIntoExternal();
      if (pushedInExternal) return pushedInExternal;
    }

    return simpler;
  }

  public getArgumentExpressions(): Expression[] {
    return [this.expression];
  }

  public _substituteHelper(
    substitutionFn: SubstitutionFn,
    indexer: Indexer,
    depth: int,
    nestDiff: int,
    typeContext: DatasetFullType,
  ): ExpressionTypeContext {
    const sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff);
    if (sub) {
      indexer.index += this.expressionCount();
      return {
        expression: sub,
        typeContext: sub.updateTypeContextIfNeeded(typeContext),
      };
    } else {
      indexer.index++;
    }
    depth++;

    const operandSubs = this.operand._substituteHelper(
      substitutionFn,
      indexer,
      depth,
      nestDiff,
      typeContext,
    );
    const nestDiffNext = nestDiff + Number(this.isNester());
    const expressionSubs = this.expression._substituteHelper(
      substitutionFn,
      indexer,
      depth,
      nestDiffNext,
      this.isNester() ? operandSubs.typeContext : typeContext,
    );
    const updatedThis = this.changeOperand(operandSubs.expression).changeExpression(
      expressionSubs.expression,
    );

    return {
      expression: updatedThis,
      typeContext: updatedThis.updateTypeContextIfNeeded(
        operandSubs.typeContext,
        expressionSubs.typeContext,
      ),
    };
  }
}
