/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2017 Imply Data, Inc.
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

import { Timezone } from 'chronoshift';
import * as hasOwnProp from 'has-own-prop';
import { Class, Instance, generalEqual } from 'immutable-class';
import { PlyType } from '../types';
import { getValueType, valueFromJS, valueToJS } from './common';
import { PlywoodValue } from './dataset';
import { NumberRange } from './numberRange';
import { PlywoodRange, Range } from './range';
import { StringRange } from './stringRange';
import { TimeRange } from './timeRange';

export interface SetValue {
  setType: PlyType;
  elements: Array<any>; // These are value any
}

export interface SetJS {
  setType: PlyType;
  elements: Array<any>; // These are JS any
}

function dateString(date: Date): string {
  return date.toISOString();
}

function arrayFromJS(xs: Array<any>, setType: string): Array<any> {
  return xs.map(x => valueFromJS(x, setType));
}

let typeUpgrades: Record<string, string> = {
  'NUMBER': 'NUMBER_RANGE',
  'TIME': 'TIME_RANGE',
  'STRING': 'STRING_RANGE'
};

let check: Class<SetValue, SetJS>;
export class Set implements Instance<SetValue, SetJS> {
  static type = 'SET';
  static EMPTY: Set;

  static unifyElements(elements: Array<PlywoodRange>): Array<PlywoodRange> {
    let newElements: Record<string, PlywoodRange> = Object.create(null);
    for (let accumulator of elements) {
      let newElementsKeys = Object.keys(newElements);
      for (let newElementsKey of newElementsKeys) {
        let newElement = newElements[newElementsKey];
        let unionElement = accumulator.union(newElement);
        if (unionElement) {
          accumulator = unionElement;
          delete newElements[newElementsKey];
        }
      }
      newElements[accumulator.toString()] = accumulator;
    }
    const newElementsKeys = Object.keys(newElements);
    return newElementsKeys.length < elements.length ? newElementsKeys.map(k => newElements[k]) : elements;
  }

  static intersectElements(elements1: Array<PlywoodRange>, elements2: Array<PlywoodRange>): Array<PlywoodRange> {
    let newElements: Array<PlywoodRange> = [];
    for (let element1 of elements1) {
      for (let element2 of elements2) {
        let intersect = element1.intersect(element2);
        if (intersect) newElements.push(intersect);
      }
    }
    return newElements;
  }

  static isSet(candidate: any): candidate is Set {
    return candidate instanceof Set;
  }

  static isAtomicType(type: PlyType): boolean {
    return type && type.indexOf('SET/') === -1;
  }

  static isSetType(type: PlyType): boolean {
    return type && type.indexOf('SET/') === 0;
  }

  static wrapSetType(type: PlyType): PlyType {
    if (!type) return null;
    return Set.isSetType(type) ? type : <PlyType>('SET/' + type);
  }

  static unwrapSetType(type: PlyType): PlyType {
    if (!type) return null;
    return Set.isSetType(type) ? <PlyType>type.substr(4) : type;
  }

  static cartesianProductOf<T>(...args: T[][]): T[][] {
    return args.reduce((a, b) => {
      return [].concat.apply([], a.map((x) => {
        return b.map((y) => {
          return x.concat([y]);
        });
      }));
    }, [[]]);
  }

  static crossBinary(as: any, bs: any, fn: (a: any, b: any) => any): any {
    if (as instanceof Set || bs instanceof Set) {
      const aElements = as instanceof Set ? as.elements : [as];
      const bElements = bs instanceof Set ? bs.elements : [bs];
      const cp = Set.cartesianProductOf(aElements, bElements);
      return Set.fromJS(cp.map((v) => fn(v[0], v[1])));
    } else {
      return fn(as, bs);
    }
  }

  static crossBinaryBoolean(as: any, bs: any, fn: (a: any, b: any) => boolean): boolean {
    if (as instanceof Set || bs instanceof Set) {
      const aElements = as instanceof Set ? as.elements : [as];
      const bElements = bs instanceof Set ? bs.elements : [bs];
      const cp = Set.cartesianProductOf(aElements, bElements);
      return cp.some((v) => fn(v[0], v[1]));
    } else {
      return fn(as, bs);
    }
  }

  static crossUnary(as: any, fn: (a: any) => any): any {
    if (as instanceof Set) {
      const aElements = as instanceof Set ? as.elements : [as];
      return Set.fromJS(aElements.map((a) => fn(a)));
    } else {
      return fn(as);
    }
  }

  static crossUnaryBoolean(as: any, fn: (a: any) => boolean): boolean {
    if (as instanceof Set) {
      const aElements = as instanceof Set ? as.elements : [as];
      return aElements.some((a) => fn(a));
    } else {
      return fn(as);
    }
  }

  static convertToSet(thing: any): Set {
    let thingType = getValueType(thing);
    if (Set.isSetType(thingType)) return thing;
    return Set.fromJS({ setType: thingType, elements: [thing] });
  }

  static unionCover(a: any, b: any): any {
    let aSet = Set.convertToSet(a);
    let bSet = Set.convertToSet(b);
    let aSetType = aSet.setType;
    let bSetType = bSet.setType;

    if (typeUpgrades[aSetType] === bSetType) {
      aSet = aSet.upgradeType();
    } else if (typeUpgrades[bSetType] === aSetType) {
      bSet = bSet.upgradeType();
    } else if (aSetType !== bSetType) {
      return null;
    }

    return aSet.union(bSet).simplifyCover();
  }

  static intersectCover(a: any, b: any): any {
    let aSet = Set.convertToSet(a);
    let bSet = Set.convertToSet(b);
    let aSetType = aSet.setType;
    let bSetType = bSet.setType;

    if (typeUpgrades[aSetType] === bSetType) {
      aSet = aSet.upgradeType();
    } else if (typeUpgrades[bSetType] === aSetType) {
      bSet = bSet.upgradeType();
    } else if (aSetType !== bSetType) {
      return null;
    }

    return aSet.intersect(bSet).simplifyCover();
  }

  static fromPlywoodValue(pv: PlywoodValue) {
    return pv instanceof Set ? pv : Set.fromJS([pv]);
  }

  static fromJS(parameters: Array<any>): Set;
  static fromJS(parameters: SetJS): Set;
  static fromJS(parameters: any): Set {
    if (Array.isArray(parameters)) {
      parameters = { elements: parameters };
    }
    if (typeof parameters !== "object") {
      throw new Error("unrecognizable set");
    }
    let setType = parameters.setType;
    let elements = parameters.elements;
    if (!setType) {
      setType = getValueType(elements.length ? elements[0] : null);
    }
    return new Set({
      setType: setType,
      elements: arrayFromJS(elements, setType)
    });
  }

  public setType: PlyType;
  public elements: Array<any>;

  private keyFn: (v: any) => string;
  private hash: Record<string, any>;

  constructor(parameters: SetValue) {
    let setType = parameters.setType;
    this.setType = setType;
    let keyFn = setType === 'TIME' ? dateString : String;
    this.keyFn = keyFn;

    let elements = parameters.elements;
    let newElements: any[] = null;
    let hash: Record<string, any> = Object.create(null);
    for (let i = 0; i < elements.length; i++) {
      let element = elements[i];
      let key = keyFn(element);
      if (hash[key]) {
        if (!newElements) newElements = elements.slice(0, i);
      } else {
        hash[key] = element;
        if (newElements) newElements.push(element);
      }
    }

    if (newElements) {
      elements = newElements;
    }

    this.elements = elements;
    this.hash = hash;
  }

  public valueOf(): SetValue {
    return {
      setType: this.setType,
      elements: this.elements
    };
  }

  public toJS(): SetJS {
    return {
      setType: this.setType,
      elements: this.elements.map(valueToJS)
    };
  }

  public toJSON(): SetJS {
    return this.toJS();
  }

  public toString(tz?: Timezone): string {
    const { setType } = this;
    let stringFn: (v: any) => string = null;
    if (setType === "NULL") return "null";

    if (setType === "TIME_RANGE") {
      stringFn = (e: any) => e ? e.toString(tz) : 'null';
    } else if (setType === "TIME") {
      stringFn = (e: any) => e ? Timezone.formatDateWithTimezone(e, tz) : 'null';
    } else {
      stringFn = String;
    }

    return `${this.elements.map(stringFn).join(", ")}`;
  }

  public equals(other: Set): boolean {
    return other instanceof Set &&
      this.setType === other.setType &&
      this.elements.length === other.elements.length &&
      this.elements.slice().sort().join('') === other.elements.slice().sort().join('');
  }

  public changeElements(elements: any[]): Set {
    if (this.elements === elements) return this;
    let value = this.valueOf();
    value.elements = elements;
    return new Set(value);
  }

  public cardinality(): int {
    return this.size();
  }

  public size(): int {
    return this.elements.length;
  }

  public empty(): boolean {
    return this.elements.length === 0;
  }

  public isNullSet(): boolean {
    return this.setType === 'NULL';
  }

  public unifyElements(): Set {
    return Range.isRangeType(this.setType) ? this.changeElements(Set.unifyElements(this.elements)) : this;
  }

  public simplifyCover(): PlywoodValue {
    const simpleSet = this.unifyElements().downgradeType();
    const simpleSetElements = simpleSet.elements;
    return simpleSetElements.length === 1 ? simpleSetElements[0] : simpleSet;
  }

  public getType(): PlyType {
    return ('SET/' + this.setType) as PlyType;
  }

  public upgradeType(): Set {
    if (this.setType === 'NUMBER') {
      return Set.fromJS({
        setType: 'NUMBER_RANGE',
        elements: this.elements.map(NumberRange.fromNumber)
      });
    } else if (this.setType === 'TIME') {
      return Set.fromJS({
        setType: 'TIME_RANGE',
        elements: this.elements.map(TimeRange.fromTime)
      });
    } else if (this.setType === 'STRING') {
      return Set.fromJS({
        setType: 'STRING_RANGE',
        elements: this.elements.map(StringRange.fromString)
      });
    } else {
      return this;
    }
  }

  public downgradeType(): Set {
    if (!Range.isRangeType(this.setType)) return this;
    let elements = this.elements;
    let simpleElements: any[] = [];
    for (let element of elements) {
      if (element.degenerate()) {
        simpleElements.push(element.start);
      } else {
        return this;
      }
    }
    return Set.fromJS(simpleElements);
  }

  public extent(): PlywoodRange {
    let setType = this.setType;
    if (hasOwnProp(typeUpgrades, setType)) {
      return this.upgradeType().extent();
    }
    if (!Range.isRangeType(setType)) return null;
    let elements = this.elements;
    let extent: PlywoodRange = elements[0] || null;
    for (let i = 1; i < elements.length; i++) {
      extent = extent.extend(elements[i]);
    }
    return extent;
  }

  public union(other: Set): Set {
    if (this.empty()) return other;
    if (other.empty()) return this;
    if (this.setType !== other.setType) throw new TypeError("can not union sets of different types");
    return this.changeElements(this.elements.concat(other.elements)).unifyElements();
  }

  public intersect(other: Set): Set {
    if (this.empty() || other.empty()) return Set.EMPTY;

    let setType = this.setType;
    if (this.setType !== other.setType) {
      throw new TypeError("can not intersect sets of different types");
    }

    let thisElements = this.elements;
    let newElements: Array<any>;
    if (setType === 'NUMBER_RANGE' || setType === 'TIME_RANGE' || setType === 'STRING_RANGE') {
      let otherElements = other.elements;
      newElements = Set.intersectElements(thisElements, otherElements);
    } else {
      newElements = [];
      for (let el of thisElements) {
        if (!other.contains(el)) continue;
        newElements.push(el);
      }
    }

    return this.changeElements(newElements);
  }

  public overlap(other: Set): boolean {
    if (this.empty() || other.empty()) return false;

    if (this.setType !== other.setType) {
      throw new TypeError("can determine overlap sets of different types");
    }

    let thisElements = this.elements;
    for (let el of thisElements) {
      if (!other.contains(el)) continue;
      return true;
    }

    return false;
  }

  public has(value: any): boolean {
    const key = this.keyFn(value);
    return hasOwnProp(this.hash, key) && generalEqual(this.hash[key], value);
  }

  public contains(value: any): boolean {
    if (value instanceof Set) {
      return value.elements.every((element) => this.contains(element));
    }

    if (Range.isRangeType(this.setType)) {
      if (value instanceof Range && this.has(value)) return true; // Shortcut
      return this.elements.some((element) => (element as PlywoodRange).contains(value));
    } else {
      return this.has(value);
    }
  }

  public add(value: any): Set {
    let setType = this.setType;
    let valueType = getValueType(value);
    if (setType === 'NULL') setType = valueType;
    if (valueType !== 'NULL' && setType !== valueType) throw new Error('value type must match');

    if (this.contains(value)) return this;
    return new Set({
      setType: setType,
      elements: this.elements.concat([value])
    });
  }

  public remove(value: any): Set {
    if (!this.contains(value)) return this;
    let keyFn = this.keyFn;
    let key = keyFn(value);
    return new Set({
      setType: this.setType, // There must be a set type since at least the value is there
      elements: this.elements.filter(element => keyFn(element) !== key)
    });
  }

  public toggle(value: any): Set {
    return this.contains(value) ? this.remove(value) : this.add(value);
  }
}
check = Set;

Set.EMPTY = Set.fromJS([]);
