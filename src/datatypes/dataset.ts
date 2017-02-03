/*
 * Copyright 2012-2015 Metamarkets Group Inc.
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

import { isDate, Timezone } from 'chronoshift';
import { Class, Instance, generalEqual, SimpleArray, NamedArray } from 'immutable-class';
import { PlyType, DatasetFullType, FullType, PlyTypeSimple } from '../types';
import * as hasOwnProp from 'has-own-prop';
import { Attributes, AttributeInfo, AttributeJSs } from './attributeInfo';
import { NumberRange } from './numberRange';
import { Set } from './set';
import { StringRange } from './stringRange';
import { TimeRange } from './timeRange';
import { valueFromJS, valueToJSInlineType, datumHasExternal } from './common';
import { Expression, ExpressionExternalAlteration, ExternalExpression, LiteralExpression, Direction } from '../expressions/index';
import { External, TotalContainer } from '../external/baseExternal';

export interface ComputeFn {
  (d: Datum): any;
}

export interface DirectionFn {
  (a: any, b: any): number;
}

export type PlywoodValue = null | boolean | number | string | Date | NumberRange | TimeRange | StringRange | Set | Dataset | External;

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

export interface AlterationFiller {
  (external: External, terminal: boolean): any;
}

export function fillExpressionExternalAlteration(alteration: ExpressionExternalAlteration, filler: AlterationFiller): void {
  for (let k in alteration) {
    let thing = alteration[k];
    if (Array.isArray(thing)) {
      fillDatasetExternalAlterations(thing, filler);
    } else {
      thing.result = filler(thing.external, Boolean(thing.terminal));
    }
  }
}

export function fillDatasetExternalAlterations(alterations: DatasetExternalAlterations, filler: AlterationFiller): void {
  for (let alteration of alterations) {
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

let directionFns: Lookup<DirectionFn> = {
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
  }
};

export interface Column {
  name: string;
  type: string;
  columns?: Column[];
}

function uniqueColumns(columns: Column[]): Column[] {
  let seen: Lookup<boolean> = {};
  let uniqueColumns: Column[] = [];
  for (let column of columns) {
    if (!seen[column.name]) {
      uniqueColumns.push(column);
      seen[column.name] = true;
    }
  }
  return uniqueColumns;
}

function flattenColumns(nestedColumns: Column[], prefixColumns: boolean): Column[] {
  let flatColumns: Column[] = [];
  let i = 0;
  let prefixString = '';
  while (i < nestedColumns.length) {
    let nestedColumn = nestedColumns[i];
    if (nestedColumn.type === 'DATASET') {
      nestedColumns = nestedColumn.columns;
      if (prefixColumns) prefixString += nestedColumn.name + '.';
      i = 0;
    } else {
      flatColumns.push({
        name: prefixString + nestedColumn.name,
        type: nestedColumn.type
      });
      i++;
    }
  }
  return uniqueColumns(flatColumns);
}

function removeLineBreaks(v: string): string {
  return v.replace(/(?:\r\n|\r|\n)/g, ' ');
}

let escapeFnCSV = (v: string) => {
  v = removeLineBreaks(v);
  if (v.indexOf('"') === -1 &&  v.indexOf(",") === -1) return v;
  return `"${v.replace(/"/g, '""')}"`;
};

let escapeFnTSV = (v: string) => {
  return removeLineBreaks(v).replace(/\t/g, "").replace(/"/g, '""');
};

let typeOrder: Lookup<number> = {
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
  'DATASET': 12
};

export interface Formatter extends Lookup<Function | undefined> {
  'NULL'?: (v: any) => string;
  'TIME'?: (v: Date, tz?: Timezone) => string;
  'TIME_RANGE'?: (v: TimeRange, tz?: Timezone) => string;
  'SET/TIME'?: (v: Set, tz?: Timezone) => string;
  'SET/TIME_RANGE'?: (v: Set, tz?: Timezone) => string;
  'STRING'?: (v: string) => string;
  'SET/STRING'?: (v: Set) => string;
  'BOOLEAN'?: (v: boolean) => string;
  'NUMBER'?: (v: number) => string;
  'NUMBER_RANGE'?: (v: NumberRange) => string;
  'SET/NUMBER'?: (v: Set) => string;
  'SET/NUMBER_RANGE'?: (v: Set) => string;
  'DATASET'?: (v: Dataset) => string;
}

const DEFAULT_FORMATTER: Formatter = {
  'NULL': (v: any) => 'NULL',
  'TIME': (v: Date, tz: Timezone) => Timezone.formatDateWithTimezone(v, tz),
  'TIME_RANGE': (v: TimeRange, tz: Timezone) => v.toString(tz),
  'SET/TIME': (v: Set, tz: Timezone) => v.toString(tz),
  'SET/TIME_RANGE': (v: Set, tz: Timezone) => v.toString(tz),
  'STRING': (v: string) => '' + v,
  'SET/STRING': (v: Set) => '' + v,
  'BOOLEAN': (v: boolean) => '' + v,
  'NUMBER': (v: number) => '' + v,
  'NUMBER_RANGE': (v: NumberRange) => '' + v,
  'SET/NUMBER': (v: Set) => '' + v,
  'SET/NUMBER_RANGE': (v: Set) => '' + v,
  'DATASET': (v: Dataset) => 'DATASET'
};

export interface FlattenOptions {
  prefixColumns?: boolean;
  order?: 'preorder' | 'inline' | 'postorder'; //  default: inline
  orderedColumns?: string[];
  nestingName?: string;
  parentName?: string;
}

export type FinalLineBreak = 'include' | 'suppress';

export interface TabulatorOptions extends FlattenOptions {
  separator?: string;
  lineBreak?: string;
  finalLineBreak?: FinalLineBreak;
  formatter?: Formatter;
  finalizer?: (v: string) => string;
  timezone?: Timezone;
}

function isBoolean(b: any) {
  return b === true || b === false;
}

function isNumber(n: any) {
  return n !== null && !isNaN(Number(n));
}

function isString(str: string) {
  return typeof str === "string";
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
  } else if (attributeValue instanceof NumberRange) {
    return new AttributeInfo({ name, type: 'NUMBER_RANGE' });
  } else if (attributeValue instanceof StringRange) {
    return new AttributeInfo({ name, type: 'STRING_RANGE' });
  } else if (attributeValue instanceof TimeRange) {
    return new AttributeInfo({ name, type: 'TIME_RANGE' });
  } else if (attributeValue instanceof Set) {
    return new AttributeInfo({ name, type: attributeValue.getType() });
  } else if (attributeValue instanceof Dataset || attributeValue instanceof External) {
    return new AttributeInfo({ name, type: 'DATASET', datasetType: attributeValue.getFullType().datasetType });
  } else {
    throw new Error(`Could not introspect ${attributeValue}`);
  }
}

function joinDatums(datumA: Datum, datumB: Datum): Datum {
  let newDatum: Datum = Object.create(null);
  for (let k in datumA) {
    newDatum[k] = datumA[k];
  }
  for (let k in datumB) {
    newDatum[k] = datumB[k];
  }
  return newDatum;
}

function copy(obj: Lookup<any>): Lookup<any> {
  let newObj: Lookup<any> = {};
  let k: string;
  for (k in obj) {
    if (hasOwnProp(obj, k)) newObj[k] = obj[k];
  }
  return newObj;
}

export interface DatasetValue {
  attributeOverrides?: Attributes;

  attributes?: Attributes;
  keys?: string[];
  data?: Datum[];
  suppress?: boolean;
}

export interface DatasetJS {
  attributes?: AttributeJSs;
  keys?: string[];
  data?: Datum[];
}

let check: Class<DatasetValue, any>;
export class Dataset implements Instance<DatasetValue, any> {
  static type = 'DATASET';

  static isDataset(candidate: any): candidate is Dataset {
    return candidate instanceof Dataset;
  }

  static datumFromJS(js: Datum): Datum {
    if (typeof js !== 'object') throw new TypeError("datum must be an object");

    let datum: Datum = Object.create(null);
    for (let k in js) {
      if (!hasOwnProp(js, k)) continue;
      datum[k] = valueFromJS(js[k]);
    }

    return datum;
  }

  static datumToJS(datum: Datum): Datum {
    let js: Datum = {};
    for (let k in datum) {
      let v = datum[k];
      if (v && (v as any).suppress) continue;
      js[k] = valueToJSInlineType(v);
    }
    return js;
  }

  static getAttributesFromData(data: Datum[]): Attributes {
    if (!data.length) return [];

    let attributeNamesToIntrospect = Object.keys(data[0]);
    let attributes: Attributes = [];

    for (let datum of data) {
      let attributeNamesStillToIntrospect: string[] = [];
      for (let attributeNameToIntrospect of attributeNamesToIntrospect) {
        let attributeInfo = getAttributeInfo(attributeNameToIntrospect, datum[attributeNameToIntrospect]);
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
    for (let attributeName of attributeNamesToIntrospect) {
      attributes.push(new AttributeInfo({ name: attributeName, type: 'STRING' }));
    }

    attributes.sort((a, b) => {
      let typeDiff = typeOrder[a.type] - typeOrder[b.type];
      if (typeDiff) return typeDiff;
      return a.name.localeCompare(b.name);
    });

    return attributes;
  }

  static parseJSON(text: string): any[] {
    text = text.trim();
    let firstChar = text[0];

    if (firstChar[0] === '[') {
      try {
        return JSON.parse(text);
      } catch (e) {
        throw new Error(`could not parse`);
      }

    } else if (firstChar[0] === '{') { // Also support line json
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

  static fromJS(parameters: any): Dataset {
    if (Array.isArray(parameters)) {
      parameters = { data: parameters };
    }

    if (!Array.isArray(parameters.data)) {
      throw new Error('must have data');
    }

    let value: DatasetValue = {};

    if (hasOwnProp(parameters, 'attributes')) {
      value.attributes = AttributeInfo.fromJSs(parameters.attributes);
    } else if (hasOwnProp(parameters, 'attributeOverrides')) {
      value.attributeOverrides = AttributeInfo.fromJSs(parameters.attributeOverrides);
    }

    value.keys = parameters.keys;
    value.data = parameters.data.map(Dataset.datumFromJS);
    return new Dataset(value);
  }

  public suppress: boolean;
  public attributes: Attributes = null;
  public keys: string[] = null;
  public data: Datum[];

  constructor(parameters: DatasetValue) {
    if (parameters.suppress === true) this.suppress = true;

    if (parameters.keys) {
      this.keys = parameters.keys;
    }
    let data = parameters.data;
    if (!Array.isArray(data)) {
      throw new TypeError("must have a `data` array");
    }
    this.data = data;

    let attributes = parameters.attributes;
    if (!attributes) attributes = Dataset.getAttributesFromData(data);

    let attributeOverrides = parameters.attributeOverrides;
    if (attributeOverrides) {
      attributes = AttributeInfo.override(attributes, attributeOverrides);
    }

    this.attributes = attributes;
  }

  public valueOf(): DatasetValue {
    let value: DatasetValue = {};
    if (this.suppress) value.suppress = true;
    if (this.attributes) value.attributes = this.attributes;
    if (this.keys) value.keys = this.keys;
    value.data = this.data;
    return value;
  }

  public toJS(): any {
    return this.data.map(Dataset.datumToJS);
  }

  public toString(): string {
    return "Dataset(" + this.data.length + ")";
  }

  public toJSON(): any {
    return this.toJS();
  }

  public equals(other: Dataset): boolean {
    return other instanceof Dataset &&
      this.data.length === other.data.length;
      // ToDo: probably add something else here?
  }

  public hide(): Dataset {
    let value = this.valueOf();
    value.suppress = true;
    return new Dataset(value);
  }

  public basis(): boolean {
    let data = this.data;
    return data.length === 1 && Object.keys(data[0]).length === 0;
  }

  public hasExternal(): boolean {
    if (!this.data.length) return false;
    return datumHasExternal(this.data[0]);
  }

  public getFullType(): DatasetFullType {
    let { attributes } = this;
    if (!attributes) throw new Error("dataset has not been introspected");

    let myDatasetType: Lookup<FullType> = {};
    for (let attribute of attributes) {
      let attrName = attribute.name;
      if (attribute.type === 'DATASET') {
        myDatasetType[attrName] = {
          type: 'DATASET',
          datasetType: attribute.datasetType
        };
      } else {
        myDatasetType[attrName] = {
          type: <PlyTypeSimple>attribute.type
        };
      }
    }

    return {
      type: 'DATASET',
      datasetType: myDatasetType
    };
  }

  // Actions
  public select(attrs: string[]): Dataset {
    let attributes = this.attributes;
    let newAttributes: Attributes = [];
    let attrLookup: Lookup<boolean> = Object.create(null);
    for (let attr of attrs) {
      attrLookup[attr] = true;
      let existingAttribute = NamedArray.get(attributes, attr);
      if (existingAttribute) newAttributes.push(existingAttribute);
    }

    let data = this.data;
    let n = data.length;
    let newData = new Array(n);
    for (let i = 0; i < n; i++) {
      let datum = data[i];
      let newDatum = Object.create(null);
      for (let key in datum) {
        if (attrLookup[key]) {
          newDatum[key] = datum[key];
        }
      }
      newData[i] = newDatum;
    }

    let value = this.valueOf();
    value.attributes = newAttributes;
    value.data = newData;
    return new Dataset(value);
  }


  public apply(name: string, ex: Expression): Dataset {
    if (typeof ex === 'function') {
      // ToDo: add better deprecation
      console.warn(`Dataset#apply now takes Expressions use Dataset.applyFn instead`);
      return this.applyFn(name, ex as any, arguments[2]);
    }
    return this.applyFn(name, ex.getFn(), ex.type);
  }

  public applyFn(name: string, exFn: ComputeFn, type: PlyType): Dataset {
    let data = this.data;
    let n = data.length;
    let newData = new Array(n);
    for (let i = 0; i < n; i++) {
      let datum = data[i];
      let newDatum = Object.create(null);
      for (let key in datum) newDatum[key] = datum[key];
      newDatum[name] = exFn(datum);
      newData[i] = newDatum;
    }

    // Hack
    let datasetType: Lookup<FullType> = null;
    if (type === 'DATASET' && newData[0] && newData[0][name]) {
      let thing: any = newData[0][name];
      if (thing instanceof Dataset) {
        datasetType = thing.getFullType().datasetType;
      } else {
        datasetType = {}; // Temp hack, (a hack within a hack), technically this should be dataset type;
      }
    }
    // End Hack

    let value = this.valueOf();
    value.attributes = NamedArray.overrideByName(value.attributes, new AttributeInfo({ name, type, datasetType }));
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
    let value = this.valueOf();
    value.data = value.data.filter(datum => exFn(datum));
    return new Dataset(value);
  }


  public sort(ex: Expression, direction: Direction): Dataset {
    if (typeof ex === 'function') {
      // ToDo: add better deprecation
      console.warn(`Dataset#sort now takes Expressions use Dataset.sortFn instead`);
      throw new Error('poop');
      //return this.sortFn(ex as any, direction);
    }
    return this.sortFn(ex.getFn(), direction);
  }

  public sortFn(exFn: ComputeFn, direction: Direction): Dataset {
    let value = this.valueOf();
    let directionFn = directionFns[direction];
    value.data = this.data.slice().sort((a, b) => {
      return directionFn(exFn(a), exFn(b));
    });
    return new Dataset(value);
  }


  public limit(limit: number): Dataset {
    let data = this.data;
    if (data.length <= limit) return this;
    let value = this.valueOf();
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
    let data = this.data;
    let sum = 0;
    for (let datum of data) {
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
    let count = this.count();
    return count ? (this.sumFn(exFn) / count) : null;
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
    let data = this.data;
    let min = Infinity;
    for (let datum of data) {
      let v = exFn(datum);
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
    let data = this.data;
    let max = -Infinity;
    for (let datum of data) {
      let v = exFn(datum);
      if (max < v) max = v;
    }
    return max;
  }


  public countDistinct(ex: Expression): number {
    if (typeof ex === 'function') {
      // ToDo: add better deprecation
      console.warn(`Dataset#countDistinct now takes Expressions use Dataset.countDistinctFn instead`);
      return this.countDistinctFn(ex as any);
    }
    return this.countDistinctFn(ex.getFn());
  }

  public countDistinctFn(exFn: ComputeFn): number {
    let data = this.data;
    let seen: Lookup<number> = Object.create(null);
    let count = 0;
    for (let datum of data) {
      let v = exFn(datum);
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
    let data = this.data;
    let vs: number[] = [];
    for (let datum of data) {
      let v = exFn(datum);
      if (v != null) vs.push(v);
    }

    vs.sort((a: number, b: number) => a - b);

    let n = vs.length;
    if (quantile === 0) return vs[0];
    if (quantile === 1) return vs[n - 1];
    let rank = n * quantile - 1;

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


  public split(splits: Lookup<Expression>, datasetName: string): Dataset {
    let splitFns: Lookup<ComputeFn> = {};
    for (let k in splits) {
      let ex = splits[k];
      if (typeof ex === 'function') {
        // ToDo: add better deprecation
        console.warn(`Dataset#collect now takes Expressions use Dataset.collectFn instead`);
        return this.split(splits as any, datasetName);
      }
      splitFns[k] = ex.getFn();
    }
    return this.splitFn(splitFns, datasetName);
  }

  public splitFn(splitFns: Lookup<ComputeFn>, datasetName: string): Dataset {
    let { data, attributes } = this;

    let keys = Object.keys(splitFns);
    let numberOfKeys = keys.length;
    let splitFnList = keys.map(k => splitFns[k]);

    let splits: Lookup<Datum> = {};
    let datumGroups: Lookup<Datum[]> = {};
    let finalData: Datum[] = [];
    let finalDataset: Datum[][] = [];

    function addDatum(datum: Datum, valueList: any): void {
      let key = valueList.join(';_PLYw00d_;');
      if (hasOwnProp(datumGroups, key)) {
        datumGroups[key].push(datum);
      } else {
        let newDatum: Datum = Object.create(null);
        for (let i = 0; i < numberOfKeys; i++) {
          newDatum[keys[i]] = valueList[i];
        }
        finalDataset.push(datumGroups[key] = [datum]);
        splits[key] = newDatum;
        finalData.push(newDatum);
      }
    }

    for (let datum of data) {
      let valueList = splitFnList.map(splitFn => splitFn(datum));
      let setIndex = -1;
      for (let i = 0; i < valueList.length; i++) {
        if (Set.isSet(valueList[i])) {
          if (setIndex !== -1) throw new Error(`only one SET value is supported in native split for now`);
          setIndex = i;
        }
      }

      if (setIndex !== -1) {
        let elements = valueList[setIndex].elements;
        let atomicValueList = valueList.slice();
        for (let element of elements) {
          atomicValueList[setIndex] = element;
          addDatum(datum, atomicValueList);
        }
      } else {
        addDatum(datum, valueList);
      }
    }

    for (let i = 0; i < finalData.length; i++) {
      finalData[i][datasetName] = new Dataset({
        suppress: true,
        attributes,
        data: finalDataset[i]
      });
    }

    return new Dataset({
      keys,
      data: finalData
    });
  }

  public introspect(): void {
    console.error('introspection is always done, `.introspect()` method never needs to be called');
  }

  public getReadyExternals(): DatasetExternalAlterations {
    let externalAlterations: DatasetExternalAlterations = [];
    const { data, attributes } = this;

    for (let i = 0; i < data.length; i++) {
      let datum = data[i];
      let normalExternalAlterations: DatasetExternalAlterations = [];
      let valueExternalAlterations: DatasetExternalAlterations = [];
      for (let attribute of attributes) {
        let value = datum[attribute.name];
        if (value instanceof Expression) {
          let subExpressionAlterations = value.getReadyExternals();
          if (Object.keys(subExpressionAlterations).length) {
            normalExternalAlterations.push({
              index: i,
              key: attribute.name,
              expressionAlterations: subExpressionAlterations
            });
          }

        } else if (value instanceof Dataset) {
          let subDatasetAlterations = value.getReadyExternals();
          if (subDatasetAlterations.length) {
            normalExternalAlterations.push({
              index: i,
              key: attribute.name,
              datasetAlterations: subDatasetAlterations
            });
          }

        } else if (value instanceof External) {
          if (!value.suppress) {
            let externalAlteration: DatasetExternalAlteration = {
              index: i,
              key: attribute.name,
              external: value,
              terminal: true
            };

            if (value.mode === 'value') {
              valueExternalAlterations.push(externalAlteration);
            } else {
              normalExternalAlterations.push(externalAlteration);
            }
          }
        }
      }

      if (valueExternalAlterations.length) {
        if (valueExternalAlterations.length === 1) {
          externalAlterations.push(valueExternalAlterations[0]);
        } else {
          externalAlterations.push({
            index: i,
            key: '',
            external: External.uniteValueExternalsIntoTotal(valueExternalAlterations)
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
    let data = this.data;
    for (let alteration of alterations) {
      let datum = data[alteration.index];
      let key = alteration.key;

      if (alteration.external) {
        let result = alteration.result;
        if (result instanceof TotalContainer) {
          let resultDatum = result.datum;
          for (let k in resultDatum) {
            datum[k] = resultDatum[k];
          }
        } else {
          datum[key] = result;
        }
      } else if (alteration.datasetAlterations) {
        datum[key] = (datum[key] as Dataset).applyReadyExternals(alteration.datasetAlterations);
      } else if (alteration.expressionAlterations) {
        let exAlt = (datum[key] as Expression).applyReadyExternals(alteration.expressionAlterations);
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

    for (let datum of data) {
      for (let key in datum) {
        let v = datum[key];
        if (v instanceof Expression) {
          let simp = v.resolve(datum).simplify();
          datum[key] = simp instanceof ExternalExpression ? simp.external : simp;
        }
      }
    }

    let value = this.valueOf();
    value.data = data;
    return new Dataset(value);
  }

  public join(other: Dataset): Dataset {
    if (!other) return this;

    let thisKey = this.keys[0]; // ToDo: temp fix
    if (!thisKey) throw new Error('join lhs must have a key (be a product of a split)');
    let otherKey = other.keys[0]; // ToDo: temp fix
    if (!otherKey) throw new Error('join rhs must have a key (be a product of a split)');

    let thisData = this.data;
    let otherData = other.data;
    let k: string;

    let mapping: Lookup<Datum[]> = Object.create(null);
    for (let i = 0; i < thisData.length; i++) {
      let datum = thisData[i];
      k = String(thisKey ? datum[thisKey] : i);
      mapping[k] = [datum];
    }
    for (let i = 0; i < otherData.length; i++) {
      let datum = otherData[i];
      k = String(otherKey ? datum[otherKey] : i);
      if (!mapping[k]) mapping[k] = [];
      mapping[k].push(datum);
    }

    let newData: Datum[] = [];
    for (let j in mapping) {
      let datums = mapping[j];
      if (datums.length === 1) {
        newData.push(datums[0]);
      } else {
        newData.push(joinDatums(datums[0], datums[1]));
      }
    }
    return new Dataset({ data: newData });
  }

  public findDatumByAttribute(attribute: string, value: any): Datum {
    return SimpleArray.find(this.data, (d) => generalEqual(d[attribute], value));
  }

  public getNestedColumns(): Column[] {
    let nestedColumns: Column[] = [];

    let attributes = this.attributes;

    let subDatasetAdded = false;
    for (let attribute of attributes) {
      let column: Column = {
        name: attribute.name,
        type: attribute.type
      };
      if (attribute.type === 'DATASET') {
        let subDataset = this.data[0][attribute.name]; // ToDo: fix this!
        if (!subDatasetAdded && subDataset instanceof Dataset) {
          subDatasetAdded = true;
          column.columns = subDataset.getNestedColumns();
          nestedColumns.push(column);
        }
      } else {
        nestedColumns.push(column);
      }
    }

    return nestedColumns;
  }

  public getColumns(options: FlattenOptions = {}): Column[] {
    const { prefixColumns, orderedColumns } = options;
    let columns: any = this.getNestedColumns();
    let flatColumns = flattenColumns(columns, prefixColumns);
    return orderedColumns && orderedColumns.length
      ? orderedColumns.map(c => NamedArray.findByName(flatColumns, c)).filter(Boolean) as Column[]
      : flatColumns;
  }

  private _flattenHelper(nestedColumns: Column[], prefix: string, order: string, nestingName: string, parentName: string, nesting: number, context: Datum, flat: PseudoDatum[]): void {
    let nestedColumnsLength = nestedColumns.length;
    if (!nestedColumnsLength) return;

    let data = this.data;
    let datasetColumn = nestedColumns.filter((nestedColumn) => nestedColumn.type === 'DATASET')[0];
    for (let datum of data) {
      let flatDatum: PseudoDatum = context ? copy(context) : {};
      if (nestingName) flatDatum[nestingName] = nesting;
      if (parentName) flatDatum[parentName] = context;

      for (let flattenedColumn of nestedColumns) {
        if (flattenedColumn.type === 'DATASET') continue;
        let flatName = (prefix !== null ? prefix : '') + flattenedColumn.name;
        flatDatum[flatName] = datum[flattenedColumn.name];
      }

      if (datasetColumn) {
        let nextPrefix: string = null;
        if (prefix !== null) nextPrefix = prefix + datasetColumn.name + '.';

        if (order === 'preorder') flat.push(flatDatum);
        (datum[datasetColumn.name] as Dataset)._flattenHelper(datasetColumn.columns, nextPrefix, order, nestingName, parentName, nesting + 1, flatDatum, flat);
        if (order === 'postorder') flat.push(flatDatum);
      }

      if (!datasetColumn) flat.push(flatDatum);
    }
  }

  public flatten(options: FlattenOptions = {}): PseudoDatum[] {
    let prefixColumns = options.prefixColumns;
    let order = options.order; // preorder, inline [default], postorder
    let nestingName = options.nestingName;
    let parentName = options.parentName;
    let nestedColumns = this.getNestedColumns();
    let flatData: PseudoDatum[] = [];
    if (nestedColumns.length) {
      this._flattenHelper(nestedColumns, (prefixColumns ? '' : null), order, nestingName, parentName, 0, null, flatData);
    }
    return flatData;
  }

  public toTabular(tabulatorOptions: TabulatorOptions): string {
    let formatter: Formatter = tabulatorOptions.formatter || {};
    const timezone = tabulatorOptions.timezone || Timezone.UTC;
    const { finalizer } = tabulatorOptions;

    let data = this.flatten(tabulatorOptions);
    let columns = this.getColumns(tabulatorOptions);

    let lines: string[] = [];
    lines.push(columns.map(c => c.name).join(tabulatorOptions.separator || ','));

    for (let i = 0; i < data.length; i++) {
      let datum = data[i];
      lines.push(columns.map(c => {
        const value = datum[c.name];
        const fmtr = value != null ? (formatter[c.type] || DEFAULT_FORMATTER[c.type]) : (formatter['NULL'] || DEFAULT_FORMATTER['NULL']);
        let formatted = String(fmtr(value, timezone));
        let finalized = formatted && finalizer ? finalizer(formatted) : formatted;
        return finalized;
      }).join(tabulatorOptions.separator || ','));
    }

    let lineBreak = tabulatorOptions.lineBreak || '\n';
    return lines.join(lineBreak) + (tabulatorOptions.finalLineBreak === 'include' && lines.length > 0 ? lineBreak : '');
  }

  public toCSV(tabulatorOptions: TabulatorOptions = {}): string {
    tabulatorOptions.finalizer = escapeFnCSV;
    tabulatorOptions.separator = tabulatorOptions.separator || ',';
    tabulatorOptions.lineBreak = tabulatorOptions.lineBreak || '\r\n';
    tabulatorOptions.finalLineBreak = tabulatorOptions.finalLineBreak || 'suppress';
    return this.toTabular(tabulatorOptions);
  }

  public toTSV(tabulatorOptions: TabulatorOptions = {}): string {
    tabulatorOptions.finalizer = escapeFnTSV;
    tabulatorOptions.separator = tabulatorOptions.separator || '\t';
    tabulatorOptions.lineBreak = tabulatorOptions.lineBreak || '\r\n';
    tabulatorOptions.finalLineBreak = tabulatorOptions.finalLineBreak || 'suppress';
    return this.toTabular(tabulatorOptions);
  }

}
check = Dataset;


/*
 two types of events (maybe 3):
 { hello: 'world', x: 5 }  // short for (3):

 1. { __$$type: 'value', value: 5 }
 2. { __$$type: 'init', attributes: AttributeInfo[], keys: string[] }
 3. { __$$type: 'row', row: { hello: "world", x: 5 }, keyProp?: "hello" }
 4. { __$$type: 'within', keyProp: "hello", propValue: "world", attribute: "subDataset", apply: { ... } }
 */

export interface PlyBitFull {
  __$$type: 'value' | 'init' | 'row' | 'within';
  value?: PlywoodValue;
  attributes?: AttributeInfo[];
  keys?: string[];
  row?: Datum;
  keyProp?: string;
  propValue?: PlywoodValue;
  attribute?: string;
  within?: PlyBit;
}

export type PlyBit = PlyBitFull | Datum;

export interface PlywoodValueIterator {
  (): PlyBit | null;
}

interface KeyPlywoodValueIterator {
  attribute: string;
  datasetIterator: PlywoodValueIterator;
}

export function iteratorFactory(value: PlywoodValue): PlywoodValueIterator {
  if (value instanceof Dataset) return datasetIteratorFactory(value);

  let nextBit: PlyBit = { __$$type: 'value', value };
  return () => {
    const ret = nextBit;
    nextBit = null;
    return ret;
  };
}

export function datasetIteratorFactory(dataset: Dataset): PlywoodValueIterator {
  let curRowIndex = -2;
  let curRow: Datum = null;
  let cutRowDatasets: KeyPlywoodValueIterator[] = [];

  function nextSelfRow() {
    curRowIndex++;
    cutRowDatasets = [];
    let row = dataset.data[curRowIndex];
    if (row) {
      curRow = {};
      for (let k in row) {
        let v = row[k];
        if (v instanceof Dataset) {
          cutRowDatasets.push({
            attribute: k,
            datasetIterator: datasetIteratorFactory(v)
          });
        } else {
          curRow[k] = v;
        }
      }
    } else {
      curRow = null;
    }
  }

  return () => {
    if (curRowIndex === -2) { // Initial run
      curRowIndex++;
      return {
        __$$type: 'init',
        attributes: dataset.attributes,
        keys: dataset.keys
      };
    }

    let pb: PlyBit;
    while (cutRowDatasets.length && !pb) {
      pb = cutRowDatasets[0].datasetIterator();
      if (!pb) cutRowDatasets.shift();
    }

    if (pb) {
      return {
        __$$type: 'within',
        attribute: cutRowDatasets[0].attribute,
        within: pb
      };
    }

    nextSelfRow();
    return curRow;
  };
}

export class PlywoodValueBuilder {
  private _value: PlywoodValue = null;
  private _attributes: AttributeInfo[];
  private _keys: string[];
  private _data: Datum[];
  private _curAttribute: string = null;
  private _curValueBuilder: PlywoodValueBuilder = null;

  private _finalizeLastWithin() {
    if (!this._curValueBuilder) return;
    let lastDatum = this._data[this._data.length - 1];
    if (!lastDatum) throw new Error('unexpected within');
    lastDatum[this._curAttribute] = this._curValueBuilder.getValue();
    this._curAttribute = null;
    this._curValueBuilder = null;
  }

  public processBit(bit: PlyBit) {
    if (typeof bit !== 'object') throw new Error(`invalid bit: ${bit}`);
    let fullBit: PlyBitFull = hasOwnProp(bit, '__$$type') ? (bit as PlyBitFull) : { __$$type: 'row', row: bit as Datum };
    switch (fullBit.__$$type) {
      case 'value':
        this._value = fullBit.value;
        this._data = null;
        this._curAttribute = null;
        this._curValueBuilder = null;
        break;

      case 'init':
        this._finalizeLastWithin();
        this._attributes = fullBit.attributes;
        this._keys = fullBit.keys;
        this._data = [];
        break;

      case 'row':
        this._finalizeLastWithin();
        if (!this._data) this._data = [];
        this._data.push(fullBit.row);
        break;

      case 'within':
        if (!this._curValueBuilder) {
          this._curAttribute = fullBit.attribute;
          this._curValueBuilder = new PlywoodValueBuilder();
        }
        this._curValueBuilder.processBit(fullBit.within);
        break;

      default:
        throw new Error(`unexpected __$$type: ${fullBit.__$$type}`);
    }
  }

  public getValue(): PlywoodValue {
    const { _data } = this;
    if (_data) {
      if (this._curValueBuilder) {
        let lastDatum = _data[_data.length - 1];
        if (!lastDatum) throw new Error('unexpected within');
        lastDatum[this._curAttribute] = this._curValueBuilder.getValue();
      }
      return new Dataset({
        attributes: this._attributes,
        keys: this._keys,
        data: _data
      });
    } else {
      return this._value;
    }
  }
}

