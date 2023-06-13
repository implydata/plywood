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

import { isDate, Timezone } from 'chronoshift';
import * as hasOwnProp from 'has-own-prop';
import { Class, generalEqual, Instance, NamedArray, SimpleArray } from 'immutable-class';

import {
  Direction,
  Expression,
  ExpressionExternalAlteration,
  ExternalExpression,
  LiteralExpression,
} from '../expressions';
import { External, TotalContainer } from '../external/baseExternal';
import { deduplicateSort } from '../helper';
import { DatasetFullType, FullType, PlyType } from '../types';

import { AttributeInfo, AttributeJSs, Attributes } from './attributeInfo';
import { datumHasExternal, valueFromJS, valueToJS } from './common';
import { Ip } from './ip';
import { NumberRange } from './numberRange';
import { Set } from './set';
import { StringRange } from './stringRange';
import { TimeRange } from './timeRange';

export type ComputeFn = (d: Datum) => any;

export type DirectionFn = (a: any, b: any) => number;

export type PlywoodValue =
  | null
  | boolean
  | number
  | string
  | Date
  | NumberRange
  | TimeRange
  | StringRange
  | Set
  | Dataset
  | External
  | Ip;

export interface PseudoDatum {
  [attribute: string]: any;
}

export interface Datum {
  [attribute: string]: PlywoodValue | Expression;
}

export interface DatasetExternalAlteration {
  index: number;
  key: string;
  external?: External;
  terminal?: boolean;
  result?: any;
  datasetAlterations?: DatasetExternalAlterations;
  expressionAlterations?: ExpressionExternalAlteration;
}

export type DatasetExternalAlterations = DatasetExternalAlteration[];

export type AlterationFiller = (external: External, terminal: boolean) => any;

export function fillExpressionExternalAlteration(
  alteration: ExpressionExternalAlteration,
  filler: AlterationFiller,
): void {
  for (const k in alteration) {
    const thing = alteration[k];
    if (Array.isArray(thing)) {
      fillDatasetExternalAlterations(thing, filler);
    } else {
      thing.result = filler(thing.external, Boolean(thing.terminal));
    }
  }
}

export function sizeOfExpressionExternalAlteration(
  alteration: ExpressionExternalAlteration,
): number {
  let count = 0;
  for (const k in alteration) {
    const thing = alteration[k];
    if (Array.isArray(thing)) {
      count += sizeOfDatasetExternalAlterations(thing);
    } else {
      count++;
    }
  }
  return count;
}

export function fillDatasetExternalAlterations(
  alterations: DatasetExternalAlterations,
  filler: AlterationFiller,
): void {
  for (const alteration of alterations) {
    if (alteration.external) {
      alteration.result = filler(alteration.external, alteration.terminal);
    } else if (alteration.datasetAlterations) {
      fillDatasetExternalAlterations(alteration.datasetAlterations, filler);
    } else if (alteration.expressionAlterations) {
      fillExpressionExternalAlteration(alteration.expressionAlterations, filler);
    } else {
      throw new Error('fell through');
    }
  }
}

export function sizeOfDatasetExternalAlterations(alterations: DatasetExternalAlterations): number {
  let count = 0;
  for (const alteration of alterations) {
    if (alteration.external) {
      count += 1;
    } else if (alteration.datasetAlterations) {
      count += sizeOfDatasetExternalAlterations(alteration.datasetAlterations);
    } else if (alteration.expressionAlterations) {
      count += sizeOfExpressionExternalAlteration(alteration.expressionAlterations);
    } else {
      throw new Error('fell through');
    }
  }
  return count;
}

const directionFns: Record<string, DirectionFn> = {
  ascending: (a: any, b: any): number => {
    if (a == null) {
      return b == null ? 0 : -1;
    } else {
      if (a.compare) return a.compare(b);
      if (b == null) return 1;
    }
    return a < b ? -1 : a > b ? 1 : a >= b ? 0 : NaN;
  },
  descending: (a: any, b: any): number => {
    if (b == null) {
      return a == null ? 0 : -1;
    } else {
      if (b.compare) return b.compare(a);
      if (a == null) return 1;
    }
    return b < a ? -1 : b > a ? 1 : b >= a ? 0 : NaN;
  },
};

function removeLineBreaks(v: string): string {
  return v.replace(/(?:\r\n|\r|\n)/g, ' ');
}

const typeOrder: Record<string, number> = {
  'NULL': 0,
  'TIME': 1,
  'TIME_RANGE': 2,
  'SET/TIME': 3,
  'SET/TIME_RANGE': 4,
  'STRING': 5,
  'SET/STRING': 6,
  'BOOLEAN': 7,
  'NUMBER': 8,
  'NUMBER_RANGE': 9,
  'SET/NUMBER': 10,
  'SET/NUMBER_RANGE': 11,
  'DATASET': 12,
};

export interface Formatter extends Record<string, Function | undefined> {
  'NULL'?: (v: any) => string;
  'TIME'?: (v: Date, tz: Timezone) => string;
  'TIME_RANGE'?: (v: TimeRange, tz: Timezone) => string;
  'SET/TIME'?: (v: Set, tz: Timezone) => string;
  'SET/TIME_RANGE'?: (v: Set, tz: Timezone) => string;
  'STRING'?: (v: string) => string;
  'SET/STRING'?: (v: Set) => string;
  'BOOLEAN'?: (v: boolean) => string;
  'NUMBER'?: (v: number) => string;
  'NUMBER_RANGE'?: (v: NumberRange) => string;
  'SET/NUMBER'?: (v: Set) => string;
  'SET/NUMBER_RANGE'?: (v: Set) => string;
  'DATASET'?: (v: Dataset) => string;
}

export type Finalizer = (v: string) => string;

export interface FlattenOptions {
  prefixColumns?: boolean;
  order?: 'preorder' | 'inline' | 'postorder'; // default: inline
  nestingName?: string;
  columnOrdering?: 'as-seen' | 'keys-first';
}

export type FinalLineBreak = 'include' | 'suppress';

export interface TabulatorOptions extends FlattenOptions {
  separator?: string;
  lineBreak?: string;
  finalLineBreak?: FinalLineBreak;
  formatter?: Formatter;
  finalizer?: Finalizer;
  timezone?: Timezone;
  attributeTitle?: (attribute: AttributeInfo) => string;
  attributeFilter?: (attribute: AttributeInfo) => boolean;
}

function isBoolean(b: any) {
  return b === true || b === false;
}

function isNumber(n: any) {
  return n !== null && !isNaN(Number(n));
}

function isString(str: string) {
  return typeof str === 'string';
}

function getAttributeInfo(name: string, attributeValue: any): AttributeInfo {
  if (attributeValue == null) return null;
  if (isDate(attributeValue)) {
    return new AttributeInfo({ name, type: 'TIME' });
  } else if (isBoolean(attributeValue)) {
    return new AttributeInfo({ name, type: 'BOOLEAN' });
  } else if (isNumber(attributeValue)) {
    return new AttributeInfo({ name, type: 'NUMBER' });
  } else if (isString(attributeValue)) {
    return new AttributeInfo({ name, type: 'STRING' });
  } else if (attributeValue instanceof Ip) {
    return new AttributeInfo({ name, type: 'IP' });
  } else if (attributeValue instanceof NumberRange) {
    return new AttributeInfo({ name, type: 'NUMBER_RANGE' });
  } else if (attributeValue instanceof StringRange) {
    return new AttributeInfo({ name, type: 'STRING_RANGE' });
  } else if (attributeValue instanceof TimeRange) {
    return new AttributeInfo({ name, type: 'TIME_RANGE' });
  } else if (attributeValue instanceof Set) {
    return new AttributeInfo({ name, type: attributeValue.getType() });
  } else if (attributeValue instanceof Dataset || attributeValue instanceof External) {
    return new AttributeInfo({ name, type: 'DATASET' }); // , datasetType: attributeValue.getFullType().datasetType
  } else {
    throw new Error(`Could not introspect ${attributeValue}`);
  }
}

function joinDatums(datumA: Datum, datumB: Datum): Datum {
  const newDatum: Datum = Object.create(null);
  for (const k in datumB) {
    newDatum[k] = datumB[k];
  }
  for (const k in datumA) {
    newDatum[k] = datumA[k];
  }
  return newDatum;
}

function copy(obj: Record<string, any>): Record<string, any> {
  const newObj: Record<string, any> = {};
  let k: string;
  for (k in obj) {
    if (hasOwnProp(obj, k)) newObj[k] = obj[k];
  }
  return newObj;
}

export interface DatasetValue {
  attributes?: Attributes;
  keys?: string[];
  data: Datum[];
  suppress?: boolean;
}

export interface DatasetJSFull {
  attributes?: AttributeJSs;
  keys?: string[];
  data?: Datum[];
}

export type DatasetJS = DatasetJSFull | Datum[];

export class Dataset implements Instance<DatasetValue, DatasetJS> {
  static type = 'DATASET';

  static DEFAULT_FORMATTER: Formatter = {
    'NULL': (v: any) => (isDate(v) ? v.toISOString() : '' + v),
    'TIME': (v: Date, tz: Timezone) => Timezone.formatDateWithTimezone(v, tz),
    'TIME_RANGE': (v: TimeRange, tz: Timezone) => v.toString(tz),
    'SET/TIME': (v: Set, tz: Timezone) => v.toString(tz),
    'SET/TIME_RANGE': (v: Set, tz: Timezone) => v.toString(tz),
    'STRING': (v: string) => '' + v,
    'SET/STRING': (v: Set) => '' + v,
    'IP': (v: Ip) => '' + v.toString(),
    'BOOLEAN': (v: boolean) => '' + v,
    'NUMBER': (v: number) => '' + v,
    'NUMBER_RANGE': (v: NumberRange) => '' + v,
    'SET/NUMBER': (v: Set) => '' + v,
    'SET/NUMBER_RANGE': (v: Set) => '' + v,
    'DATASET': (_v: Dataset) => 'DATASET',
  };

  static CSV_FINALIZER: Finalizer = (v: string) => {
    v = removeLineBreaks(v);
    if (v.indexOf('"') === -1 && v.indexOf(',') === -1) return v;
    return `"${v.replace(/"/g, '""')}"`;
  };

  static TSV_FINALIZER: Finalizer = (v: string) => {
    return removeLineBreaks(v).replace(/\t/g, '').replace(/"/g, '""');
  };

  static datumToLine(
    datum: Datum,
    attributes: Attributes,
    timezone: Timezone,
    formatter: Formatter,
    finalizer: Finalizer,
    separator: string,
  ) {
    return attributes
      .map(c => {
        const value = datum[c.name];
        const fmtrType = value != null ? c.type : 'NULL';
        const fmtr = formatter[fmtrType] || Dataset.DEFAULT_FORMATTER[fmtrType];
        const formatted = String(fmtr(value, timezone));
        return finalizer(formatted);
      })
      .join(separator);
  }

  static isDataset(candidate: any): candidate is Dataset {
    return candidate instanceof Dataset;
  }

  static datumFromJS(js: PseudoDatum, attributeLookup: Record<string, AttributeInfo> = {}): Datum {
    if (typeof js !== 'object') throw new TypeError('datum must be an object');

    const datum: Datum = Object.create(null);
    for (const k in js) {
      if (!hasOwnProp(js, k)) continue;
      datum[k] = valueFromJS(
        js[k],
        hasOwnProp(attributeLookup, k) ? attributeLookup[k].type : null,
      );
    }

    return datum;
  }

  static datumToJS(datum: Datum): PseudoDatum {
    const js: PseudoDatum = {};
    for (const k in datum) {
      const v = datum[k];
      if (v && (v as any).suppress) continue;
      js[k] = valueToJS(v);
    }
    return js;
  }

  static getAttributesFromData(data: Datum[]): Attributes {
    if (!data.length) return [];

    let attributeNamesToIntrospect = Object.keys(data[0]);
    const attributes: Attributes = [];

    for (const datum of data) {
      const attributeNamesStillToIntrospect: string[] = [];
      for (const attributeNameToIntrospect of attributeNamesToIntrospect) {
        const attributeInfo = getAttributeInfo(
          attributeNameToIntrospect,
          datum[attributeNameToIntrospect],
        );
        if (attributeInfo) {
          attributes.push(attributeInfo);
        } else {
          attributeNamesStillToIntrospect.push(attributeNameToIntrospect);
        }
      }

      attributeNamesToIntrospect = attributeNamesStillToIntrospect;
      if (!attributeNamesToIntrospect.length) break;
    }

    // Assume all the remaining nulls are strings
    for (const attributeName of attributeNamesToIntrospect) {
      attributes.push(new AttributeInfo({ name: attributeName, type: 'STRING' }));
    }

    attributes.sort((a, b) => {
      const typeDiff = typeOrder[a.type] - typeOrder[b.type];
      if (typeDiff) return typeDiff;
      return a.name.localeCompare(b.name);
    });

    return attributes;
  }

  static parseJSON(text: string): any[] {
    text = text.trim();
    const firstChar = text[0];

    if (firstChar[0] === '[') {
      try {
        return JSON.parse(text);
      } catch (e) {
        throw new Error(`could not parse`);
      }
    } else if (firstChar[0] === '{') {
      // Also support line json
      return text.split(/\r?\n/).map((line, i) => {
        try {
          return JSON.parse(line);
        } catch (e) {
          throw new Error(`problem in line: ${i}: '${line}'`);
        }
      });
    } else {
      throw new Error(`Unsupported start, starts with '${firstChar[0]}'`);
    }
  }

  static fromJS(parameters: DatasetJS | any[]): Dataset {
    if (Array.isArray(parameters)) {
      parameters = { data: parameters };
    }

    if (!Array.isArray(parameters.data)) {
      throw new Error('must have data');
    }

    let attributes: Attributes | undefined;
    const attributeLookup: Record<string, AttributeInfo> = {};
    if (parameters.attributes) {
      attributes = AttributeInfo.fromJSs(parameters.attributes);
      for (const attribute of attributes) attributeLookup[attribute.name] = attribute;
    }

    return new Dataset({
      attributes,
      keys: parameters.keys || [],
      data: parameters.data.map(d => Dataset.datumFromJS(d, attributeLookup)),
    });
  }

  public suppress: boolean;
  public attributes: Attributes = null;
  public keys: string[];
  public data: Datum[];

  constructor(parameters: DatasetValue) {
    if (parameters.suppress === true) this.suppress = true;

    this.keys = parameters.keys || [];
    const data = parameters.data;
    if (!Array.isArray(data)) {
      throw new TypeError('must have a `data` array');
    }
    this.data = data;

    let attributes = parameters.attributes;
    if (!attributes) attributes = Dataset.getAttributesFromData(data);

    this.attributes = attributes;
  }

  public valueOf(): DatasetValue {
    const value: DatasetValue = {
      keys: this.keys,
      attributes: this.attributes,
      data: this.data,
    };
    if (this.suppress) value.suppress = true;
    return value;
  }

  public toJS(): DatasetJS {
    const js: DatasetJSFull = {};
    if (this.keys.length) js.keys = this.keys;
    if (this.attributes) js.attributes = AttributeInfo.toJSs(this.attributes);
    js.data = this.data.map(Dataset.datumToJS);
    return js;
  }

  public toString(): string {
    return `Dataset(${this.data.length})`;
  }

  public toJSON(): any {
    return this.toJS();
  }

  public equals(other: Dataset | undefined): boolean {
    return other instanceof Dataset && this.data.length === other.data.length;
    // ToDo: probably add something else here?
  }

  public hide(): Dataset {
    const value = this.valueOf();
    value.suppress = true;
    return new Dataset(value);
  }

  public changeData(data: Datum[]): Dataset {
    const value = this.valueOf();
    value.data = data;
    return new Dataset(value);
  }

  public basis(): boolean {
    const data = this.data;
    return data.length === 1 && Object.keys(data[0]).length === 0;
  }

  public hasExternal(): boolean {
    if (!this.data.length) return false;
    return datumHasExternal(this.data[0]);
  }

  public getFullType(): DatasetFullType {
    const { attributes } = this;
    if (!attributes) throw new Error('dataset has not been introspected');

    const myDatasetType: Record<string, FullType> = {};
    for (const attribute of attributes) {
      const attrName = attribute.name;
      if (attribute.type === 'DATASET') {
        let v0: any; // ToDo: revisit, look beyond 0
        if (this.data.length && (v0 = this.data[0][attrName]) && v0 instanceof Dataset) {
          myDatasetType[attrName] = v0.getFullType();
        } else {
          myDatasetType[attrName] = {
            type: 'DATASET',
            datasetType: {},
          };
        }
      } else {
        myDatasetType[attrName] = {
          type: attribute.type,
        };
      }
    }

    return {
      type: 'DATASET',
      datasetType: myDatasetType,
    };
  }

  // Actions
  public select(attrs: string[]): Dataset {
    const attributes = this.attributes;
    const newAttributes: Attributes = [];
    const attrLookup: Record<string, boolean> = Object.create(null);
    for (const attr of attrs) {
      attrLookup[attr] = true;
      const existingAttribute = NamedArray.get(attributes, attr);
      if (existingAttribute) newAttributes.push(existingAttribute);
    }

    const data = this.data;
    const n = data.length;
    const newData = new Array(n);
    for (let i = 0; i < n; i++) {
      const datum = data[i];
      const newDatum = Object.create(null);
      for (const key in datum) {
        if (attrLookup[key]) {
          newDatum[key] = datum[key];
        }
      }
      newData[i] = newDatum;
    }

    const value = this.valueOf();
    value.attributes = newAttributes;
    value.data = newData;
    return new Dataset(value);
  }

  public apply(name: string, ex: Expression): Dataset {
    if (typeof ex === 'function') {
      // ToDo: add better deprecation
      console.warn(`Dataset#apply now takes Expressions use Dataset.applyFn instead`);
      // eslint-disable-next-line prefer-rest-params
      return this.applyFn(name, ex as any, arguments[2]);
    }
    return this.applyFn(name, ex.getFn(), ex.type);
  }

  public applyFn(name: string, exFn: ComputeFn, type: PlyType): Dataset {
    const data = this.data;
    const n = data.length;
    const newData = new Array(n);
    for (let i = 0; i < n; i++) {
      const datum = data[i];
      const newDatum = Object.create(null);
      for (const key in datum) newDatum[key] = datum[key];
      newDatum[name] = exFn(datum);
      newData[i] = newDatum;
    }

    // // Hack
    // let datasetType: Record<string, FullType> = null;
    // if (type === 'DATASET' && newData[0] && newData[0][name]) {
    //   let thing: any = newData[0][name];
    //   if (thing instanceof Dataset) {
    //     datasetType = thing.getFullType().datasetType;
    //   } else {
    //     datasetType = {}; // Temp hack, (a hack within a hack), technically this should be dataset type;
    //   }
    // }
    // // End Hack

    const value = this.valueOf();
    value.attributes = NamedArray.overrideByName(
      value.attributes,
      new AttributeInfo({ name, type }),
    );
    value.data = newData;
    return new Dataset(value);
  }

  public filter(ex: Expression): Dataset {
    if (typeof ex === 'function') {
      // ToDo: add better deprecation
      console.warn(`Dataset#filter now takes Expressions use Dataset.filterFn instead`);
      return this.filterFn(ex as any);
    }
    return this.filterFn(ex.getFn());
  }

  public filterFn(exFn: ComputeFn): Dataset {
    const value = this.valueOf();
    value.data = value.data.filter(datum => exFn(datum));
    return new Dataset(value);
  }

  public sort(ex: Expression, direction: Direction): Dataset {
    if (typeof ex === 'function') {
      // ToDo: add better deprecation
      console.warn(`Dataset#sort now takes Expressions use Dataset.sortFn instead`);
      return this.sortFn(ex as any, direction);
    }
    return this.sortFn(ex.getFn(), direction);
  }

  public sortFn(exFn: ComputeFn, direction: Direction): Dataset {
    const value = this.valueOf();
    const directionFn = directionFns[direction];
    value.data = this.data.slice().sort((a, b) => {
      return directionFn(exFn(a), exFn(b));
    });
    return new Dataset(value);
  }

  public limit(limit: number): Dataset {
    const data = this.data;
    if (data.length <= limit) return this;
    const value = this.valueOf();
    value.data = data.slice(0, limit);
    return new Dataset(value);
  }

  // Aggregators
  public count(): int {
    return this.data.length;
  }

  public sum(ex: Expression): number {
    if (typeof ex === 'function') {
      // ToDo: add better deprecation
      console.warn(`Dataset#sum now takes Expressions use Dataset.sumFn instead`);
      return this.sumFn(ex as any);
    }
    return this.sumFn(ex.getFn());
  }

  public sumFn(exFn: ComputeFn): number {
    const data = this.data;
    let sum = 0;
    for (const datum of data) {
      sum += exFn(datum);
    }
    return sum;
  }

  public average(ex: Expression): number {
    if (typeof ex === 'function') {
      // ToDo: add better deprecation
      console.warn(`Dataset#average now takes Expressions use Dataset.averageFn instead`);
      return this.averageFn(ex as any);
    }
    return this.averageFn(ex.getFn());
  }

  public averageFn(exFn: ComputeFn): number {
    const count = this.count();
    return count ? this.sumFn(exFn) / count : null;
  }

  public min(ex: Expression): number {
    if (typeof ex === 'function') {
      // ToDo: add better deprecation
      console.warn(`Dataset#min now takes Expressions use Dataset.minFn instead`);
      return this.minFn(ex as any);
    }
    return this.minFn(ex.getFn());
  }

  public minFn(exFn: ComputeFn): number {
    const data = this.data;
    let min = Infinity;
    for (const datum of data) {
      const v = exFn(datum);
      if (v < min) min = v;
    }
    return min;
  }

  public max(ex: Expression): number {
    if (typeof ex === 'function') {
      // ToDo: add better deprecation
      console.warn(`Dataset#max now takes Expressions use Dataset.maxFn instead`);
      return this.maxFn(ex as any);
    }
    return this.maxFn(ex.getFn());
  }

  public maxFn(exFn: ComputeFn): number {
    const data = this.data;
    let max = -Infinity;
    for (const datum of data) {
      const v = exFn(datum);
      if (max < v) max = v;
    }
    return max;
  }

  public countDistinct(ex: Expression): number {
    if (typeof ex === 'function') {
      // ToDo: add better deprecation
      console.warn(
        `Dataset#countDistinct now takes Expressions use Dataset.countDistinctFn instead`,
      );
      return this.countDistinctFn(ex as any);
    }
    return this.countDistinctFn(ex.getFn());
  }

  public countDistinctFn(exFn: ComputeFn): number {
    const data = this.data;
    const seen: Record<string, number> = Object.create(null);
    let count = 0;
    for (const datum of data) {
      const v = exFn(datum);
      if (!seen[v]) {
        seen[v] = 1;
        ++count;
      }
    }
    return count;
  }

  public quantile(ex: Expression, quantile: number): number {
    if (typeof ex === 'function') {
      // ToDo: add better deprecation
      console.warn(`Dataset#quantile now takes Expressions use Dataset.quantileFn instead`);
      return this.quantileFn(ex as any, quantile);
    }
    return this.quantileFn(ex.getFn(), quantile);
  }

  public quantileFn(exFn: ComputeFn, quantile: number): number {
    const data = this.data;
    const vs: number[] = [];
    for (const datum of data) {
      const v = exFn(datum);
      if (v != null) vs.push(v);
    }

    vs.sort((a: number, b: number) => a - b);

    const n = vs.length;
    if (quantile === 0) return vs[0];
    if (quantile === 1) return vs[n - 1];
    const rank = n * quantile - 1;

    // Is the rank an integer?
    if (rank === Math.floor(rank)) {
      return (vs[rank] + vs[rank + 1]) / 2;
    } else {
      return vs[Math.ceil(rank)];
    }
  }

  public collect(ex: Expression): Set {
    if (typeof ex === 'function') {
      // ToDo: add better deprecation
      console.warn(`Dataset#collect now takes Expressions use Dataset.collectFn instead`);
      return this.collectFn(ex as any);
    }
    return this.collectFn(ex.getFn());
  }

  public collectFn(exFn: ComputeFn): Set {
    return Set.fromJS(this.data.map(exFn));
  }

  public split(splits: Record<string, Expression>, datasetName: string): Dataset {
    const splitFns: Record<string, ComputeFn> = {};
    for (const k in splits) {
      const ex = splits[k];
      if (typeof ex === 'function') {
        // ToDo: add better deprecation
        console.warn(`Dataset#collect now takes Expressions use Dataset.collectFn instead`);
        return this.split(splits as any, datasetName);
      }
      splitFns[k] = ex.getFn();
    }
    return this.splitFn(splitFns, datasetName);
  }

  public splitFn(splitFns: Record<string, ComputeFn>, datasetName: string): Dataset {
    const { data, attributes } = this;

    const keys = Object.keys(splitFns);
    const numberOfKeys = keys.length;
    const splitFnList = keys.map(k => splitFns[k]);

    const splits: Record<string, Datum> = {};
    const datumGroups: Record<string, Datum[]> = {};
    const finalData: Datum[] = [];
    const finalDataset: Datum[][] = [];

    function addDatum(datum: Datum, valueList: any): void {
      const key = valueList.join(';_PLYw00d_;');
      if (hasOwnProp(datumGroups, key)) {
        datumGroups[key].push(datum);
      } else {
        const newDatum: Datum = Object.create(null);
        for (let i = 0; i < numberOfKeys; i++) {
          newDatum[keys[i]] = valueList[i];
        }
        finalDataset.push((datumGroups[key] = [datum]));
        splits[key] = newDatum;
        finalData.push(newDatum);
      }
    }

    for (const datum of data) {
      const valueList = splitFnList.map(splitFn => splitFn(datum));

      const setIndex: number[] = [];
      const setElements: any[][] = [];
      for (let i = 0; i < valueList.length; i++) {
        if (Set.isSet(valueList[i])) {
          setIndex.push(i);
          setElements.push(valueList[i].elements);
        }
      }
      const numSets = setIndex.length;

      if (numSets) {
        const cp = Set.cartesianProductOf(...setElements);
        for (const v of cp) {
          for (let j = 0; j < numSets; j++) {
            valueList[setIndex[j]] = v[j];
          }
          addDatum(datum, valueList);
        }
      } else {
        addDatum(datum, valueList);
      }
    }

    for (let i = 0; i < finalData.length; i++) {
      finalData[i][datasetName] = new Dataset({
        suppress: true,
        attributes,
        data: finalDataset[i],
      });
    }

    return new Dataset({
      keys,
      data: finalData,
    });
  }

  public getReadyExternals(limit = Infinity): DatasetExternalAlterations {
    const externalAlterations: DatasetExternalAlterations = [];
    const { data, attributes } = this;

    for (let i = 0; i < data.length; i++) {
      if (limit <= 0) break;

      const datum = data[i];
      const normalExternalAlterations: DatasetExternalAlterations = [];
      const valueExternalAlterations: DatasetExternalAlterations = [];
      for (const attribute of attributes) {
        const value = datum[attribute.name];
        if (value instanceof Expression) {
          const subExpressionAlterations = value.getReadyExternals(limit);
          const size = sizeOfExpressionExternalAlteration(subExpressionAlterations);
          if (size) {
            limit -= size;
            normalExternalAlterations.push({
              index: i,
              key: attribute.name,
              expressionAlterations: subExpressionAlterations,
            });
          }
        } else if (value instanceof Dataset) {
          const subDatasetAlterations = value.getReadyExternals(limit);
          const size = sizeOfDatasetExternalAlterations(subDatasetAlterations);
          if (size) {
            limit -= size;
            normalExternalAlterations.push({
              index: i,
              key: attribute.name,
              datasetAlterations: subDatasetAlterations,
            });
          }
        } else if (value instanceof External) {
          if (!value.suppress) {
            const externalAlteration: DatasetExternalAlteration = {
              index: i,
              key: attribute.name,
              external: value,
              terminal: true,
            };

            if (value.mode === 'value') {
              valueExternalAlterations.push(externalAlteration);
            } else {
              limit--;
              normalExternalAlterations.push(externalAlteration);
            }
          }
        }
      }

      if (valueExternalAlterations.length) {
        limit--;
        if (valueExternalAlterations.length === 1) {
          externalAlterations.push(valueExternalAlterations[0]);
        } else {
          externalAlterations.push({
            index: i,
            key: '',
            external: External.uniteValueExternalsIntoTotal(valueExternalAlterations),
          });
        }
      }

      if (normalExternalAlterations.length) {
        Array.prototype.push.apply(externalAlterations, normalExternalAlterations);
      }
    }
    return externalAlterations;
  }

  public applyReadyExternals(alterations: DatasetExternalAlterations): Dataset {
    const data = this.data;
    for (const alteration of alterations) {
      const datum = data[alteration.index];
      const key = alteration.key;

      if (alteration.external) {
        const result = alteration.result;
        if (result instanceof TotalContainer) {
          const resultDatum = result.datum;
          for (const k in resultDatum) {
            datum[k] = resultDatum[k];
          }
        } else {
          datum[key] = result;
        }
      } else if (alteration.datasetAlterations) {
        datum[key] = (datum[key] as Dataset).applyReadyExternals(alteration.datasetAlterations);
      } else if (alteration.expressionAlterations) {
        const exAlt = (datum[key] as Expression).applyReadyExternals(
          alteration.expressionAlterations,
        );
        if (exAlt instanceof ExternalExpression) {
          datum[key] = exAlt.external;
        } else if (exAlt instanceof LiteralExpression) {
          datum[key] = exAlt.getLiteralValue();
        } else {
          datum[key] = exAlt;
        }
      } else {
        throw new Error('fell through');
      }
    }

    for (const datum of data) {
      for (const key in datum) {
        const v = datum[key];
        if (v instanceof Expression) {
          const simp = v.resolve(datum).simplify();
          datum[key] = simp instanceof ExternalExpression ? simp.external : simp;
        }
      }
    }

    const value = this.valueOf();
    value.data = data;
    return new Dataset(value);
  }

  public sameKeys(other: Dataset): boolean {
    return this.keys.join('|') === other.keys.join('|');
  }

  public getKeyValueForDatum(datum: Datum): string {
    const { keys } = this;
    if (!keys) throw new Error('join lhs must have a key (be a product of a split)');
    return this.keys
      .map(k => {
        let v: any = datum[k];
        if (v && v.start) v = v.start;
        if (v && v.toISOString) v = v.toISOString();
        return v;
      })
      .join('|');
  }

  public getKeyLookup(): Record<string, Datum> {
    const { data } = this;

    const mapping: Record<string, Datum> = Object.create(null);
    for (let i = 0; i < data.length; i++) {
      const datum = data[i];
      mapping[this.getKeyValueForDatum(datum)] = datum;
    }

    return mapping;
  }

  public join(other: Dataset): Dataset {
    return this.leftJoin(other);
  }

  public leftJoin(other: Dataset): Dataset {
    if (!other || !other.data.length) return this;
    const { data, keys, attributes } = this;
    if (!data.length) return this;

    const otherLookup = other.getKeyLookup();

    const newData = data.map(datum => {
      const otherDatum = otherLookup[this.getKeyValueForDatum(datum)];
      if (!otherDatum) return datum;
      return joinDatums(datum, otherDatum);
    });

    return new Dataset({
      keys,
      attributes: AttributeInfo.override(attributes, other.attributes),
      data: newData,
    });
  }

  public fullJoin(other: Dataset): Dataset {
    if (!other || !other.data.length) return this;
    const { data, keys, attributes } = this;
    if (!data.length) return other;

    if (!this.sameKeys(other)) {
      throw new Error('this and other keys must match');
    }

    const myDatumLookup = this.getKeyLookup();
    const otherDatumLookup = other.getKeyLookup();

    const newData = deduplicateSort(
      Object.keys(myDatumLookup).concat(Object.keys(otherDatumLookup)),
    ).map(key => {
      const myDatum = myDatumLookup[key];
      const otherDatum = otherDatumLookup[key];
      if (myDatum) {
        if (otherDatum) {
          return joinDatums(myDatum, otherDatum);
        } else {
          return myDatum;
        }
      } else {
        return otherDatum;
      }
    });

    return new Dataset({
      keys,
      attributes: AttributeInfo.override(attributes, other.attributes),
      data: newData,
    });
  }

  public findDatumByAttribute(attribute: string, value: any): Datum | undefined {
    return SimpleArray.find(this.data, d => generalEqual(d[attribute], value));
  }

  public getColumns(options: FlattenOptions = {}) {
    return this.flatten(options).attributes;
  }

  private _flattenHelper(
    prefix: string,
    order: string,
    nestingName: string,
    nesting: number,
    context: Datum,
    primaryFlatAttributes: AttributeInfo[],
    secondaryFlatAttributes: AttributeInfo[],
    seenAttributes: Record<string, boolean>,
    flatData: Datum[],
  ): void {
    const { attributes, data, keys } = this;

    const datasetAttributes: string[] = [];
    for (const attribute of attributes) {
      if (attribute.type === 'DATASET') {
        datasetAttributes.push(attribute.name);
      } else {
        const flatName = (prefix || '') + attribute.name;
        if (!seenAttributes[flatName]) {
          const flatAttribute = new AttributeInfo({
            name: flatName,
            type: attribute.type,
          });
          if (!secondaryFlatAttributes || (keys && keys.indexOf(attribute.name) > -1)) {
            primaryFlatAttributes.push(flatAttribute);
          } else {
            secondaryFlatAttributes.push(flatAttribute);
          }
          seenAttributes[flatName] = true;
        }
      }
    }

    for (const datum of data) {
      const flatDatum: PseudoDatum = context ? copy(context) : {};
      if (nestingName) flatDatum[nestingName] = nesting;

      let hasDataset = false;
      for (const attribute of attributes) {
        const v = datum[attribute.name];
        if (v instanceof Dataset) {
          hasDataset = true;
          continue;
        }
        const flatName = (prefix || '') + attribute.name;
        flatDatum[flatName] = v;
      }

      if (hasDataset) {
        if (order === 'preorder') flatData.push(flatDatum);
        for (const datasetAttribute of datasetAttributes) {
          let nextPrefix: string = null;
          if (prefix !== null) nextPrefix = prefix + datasetAttribute + '.';
          const dv = datum[datasetAttribute];
          if (dv instanceof Dataset) {
            dv._flattenHelper(
              nextPrefix,
              order,
              nestingName,
              nesting + 1,
              flatDatum,
              primaryFlatAttributes,
              secondaryFlatAttributes,
              seenAttributes,
              flatData,
            );
          }
        }
        if (order === 'postorder') flatData.push(flatDatum);
      } else {
        flatData.push(flatDatum);
      }
    }
  }

  public flatten(options: FlattenOptions = {}): Dataset {
    const prefixColumns = options.prefixColumns;
    const order = options.order; // preorder, inline [default], postorder
    const nestingName = options.nestingName;
    const columnOrdering = options.columnOrdering || 'as-seen';
    if ((options as any).parentName) {
      throw new Error(`parentName option is no longer supported`);
    }
    if ((options as any).orderedColumns) {
      throw new Error(`orderedColumns option is no longer supported use .select() instead`);
    }
    if (columnOrdering !== 'as-seen' && columnOrdering !== 'keys-first') {
      throw new Error(`columnOrdering must be one of 'as-seen' or 'keys-first'`);
    }

    const primaryFlatAttributes: AttributeInfo[] = [];
    const secondaryFlatAttributes: AttributeInfo[] = columnOrdering === 'keys-first' ? [] : null;
    const flatData: Datum[] = [];
    this._flattenHelper(
      prefixColumns ? '' : null,
      order,
      nestingName,
      0,
      null,
      primaryFlatAttributes,
      secondaryFlatAttributes,
      {},
      flatData,
    );
    return new Dataset({
      attributes: primaryFlatAttributes.concat(secondaryFlatAttributes || []),
      data: flatData,
    });
  }

  public toTabular(tabulatorOptions: TabulatorOptions): string {
    const formatter: Formatter = tabulatorOptions.formatter || {};
    const timezone = tabulatorOptions.timezone || Timezone.UTC;
    const finalizer = tabulatorOptions.finalizer || String;
    const separator = tabulatorOptions.separator || ',';
    const attributeTitle = tabulatorOptions.attributeTitle || ((a: AttributeInfo) => a.name);

    let { data, attributes } = this.flatten(tabulatorOptions);

    if (tabulatorOptions.attributeFilter) {
      attributes = attributes.filter(tabulatorOptions.attributeFilter);
    }

    const lines: string[] = [];
    lines.push(attributes.map(c => finalizer(attributeTitle(c))).join(separator));

    for (let i = 0; i < data.length; i++) {
      lines.push(
        Dataset.datumToLine(data[i], attributes, timezone, formatter, finalizer, separator),
      );
    }

    const lineBreak = tabulatorOptions.lineBreak || '\n';
    return (
      lines.join(lineBreak) +
      (tabulatorOptions.finalLineBreak === 'include' && lines.length > 0 ? lineBreak : '')
    );
  }

  public toCSV(tabulatorOptions: TabulatorOptions = {}): string {
    tabulatorOptions.finalizer = Dataset.CSV_FINALIZER;
    tabulatorOptions.separator = tabulatorOptions.separator || ',';
    tabulatorOptions.lineBreak = tabulatorOptions.lineBreak || '\r\n';
    tabulatorOptions.finalLineBreak = tabulatorOptions.finalLineBreak || 'suppress';
    tabulatorOptions.columnOrdering = tabulatorOptions.columnOrdering || 'keys-first';
    return this.toTabular(tabulatorOptions);
  }

  public toTSV(tabulatorOptions: TabulatorOptions = {}): string {
    tabulatorOptions.finalizer = Dataset.TSV_FINALIZER;
    tabulatorOptions.separator = tabulatorOptions.separator || '\t';
    tabulatorOptions.lineBreak = tabulatorOptions.lineBreak || '\r\n';
    tabulatorOptions.finalLineBreak = tabulatorOptions.finalLineBreak || 'suppress';
    tabulatorOptions.columnOrdering = tabulatorOptions.columnOrdering || 'keys-first';
    return this.toTabular(tabulatorOptions);
  }

  public rows(): number {
    const { data, attributes } = this;
    let c = data.length;

    for (const datum of data) {
      for (const attribute of attributes) {
        const v = datum[attribute.name];
        if (v instanceof Dataset) {
          c += v.rows();
        }
      }
    }

    return c;
  }

  public depthFirstTrimTo(n: number): Dataset {
    const mySize = this.rows();
    if (mySize < n) return this;
    const { data, attributes } = this;

    const newData: Datum[] = [];
    for (const datum of data) {
      if (n <= 0) break;
      n--; // Account for self

      const newDatum: Datum = {};
      let newDatumRows = 0;
      for (const attribute of attributes) {
        const attributeName = attribute.name;
        const v = datum[attributeName];
        if (v instanceof Dataset) {
          const vTrim = v.depthFirstTrimTo(n);
          newDatum[attributeName] = vTrim;
          newDatumRows += vTrim.rows();
        } else if (typeof v !== 'undefined') {
          newDatum[attributeName] = v;
        }
      }

      n -= newDatumRows;
      newData.push(newDatum);
    }

    return this.changeData(newData);
  }
}

// eslint-disable-next-line unused-imports/no-unused-vars
const check: Class<DatasetValue, DatasetJS> = Dataset;
