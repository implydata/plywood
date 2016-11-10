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

import { Class, Instance } from 'immutable-class';
import { PlyType } from '../types';
import { hasOwnProperty } from '../helper/utils';
import { PlywoodRange } from '../datatypes/range';
import { NumberRange } from '../datatypes/numberRange';
import { TimeRange } from '../datatypes/timeRange';
import { getValueType, isSetType, valueToJS, valueFromJS } from './common';
import { StringRange } from './stringRange';
import { isDate } from 'chronoshift';

export interface SetValue {
  setType: string;
  elements: Array<any>; // These are value any
}

export interface SetJS {
  setType: string;
  elements: Array<any>; // These are JS any
}

function dateString(date: Date): string {
  return date.toISOString();
}

function arrayFromJS(xs: Array<any>, setType: string): Array<any> {
  return xs.map(x => valueFromJS(x, setType));
}

function unifyElements(elements: Array<PlywoodRange>): Array<PlywoodRange> {
  let newElements: Lookup<PlywoodRange> = Object.create(null);
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
  return Object.keys(newElements).map(k => newElements[k]);
}

function intersectElements(elements1: Array<PlywoodRange>, elements2: Array<PlywoodRange>): Array<PlywoodRange> {
  let newElements: Array<PlywoodRange> = [];
  for (let element1 of elements1) {
    for (let element2 of elements2) {
      let intersect = element1.intersect(element2);
      if (intersect) newElements.push(intersect);
    }
  }
  return newElements;
}

let typeUpgrades: Lookup<string> = {
  'NUMBER': 'NUMBER_RANGE',
  'TIME': 'TIME_RANGE',
  'STRING': 'STRING_RANGE'
};

let check: Class<SetValue, SetJS>;
export class Set implements Instance<SetValue, SetJS> {
  static type = 'SET';
  static EMPTY: Set;

  static isSet(candidate: any): candidate is Set {
    return candidate instanceof Set;
  }

  static convertToSet(thing: any): Set {
    let thingType = getValueType(thing);
    if (isSetType(thingType)) return thing;
    return Set.fromJS({ setType: thingType, elements: [thing] });
  }

  static generalUnion(a: any, b: any): any {
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

    return aSet.union(bSet).simplify();
  }

  static generalIntersect(a: any, b: any): any {
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

    return aSet.intersect(bSet).simplify();
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

  public setType: string;
  public elements: Array<any>;

  private keyFn: (v: any) => string;
  private hash: Lookup<any>;

  constructor(parameters: SetValue) {
    let setType = parameters.setType;
    this.setType = setType;
    let keyFn = setType === 'TIME' ? dateString : String;
    this.keyFn = keyFn;

    let elements = parameters.elements;
    let newElements: any[] = null;
    let hash: Lookup<any> = Object.create(null);
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

  public toString(): string {
    if (this.setType === "NULL") return "null";
    return `${this.elements.map(String).join(", ")}`;
  }

  public equals(other: Set): boolean {
    return other instanceof Set &&
      this.setType === other.setType &&
      this.elements.length === other.elements.length &&
      this.elements.slice().sort().join('') === other.elements.slice().sort().join('');
  }

  public changeElements(elements: any[]): Set {
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
    const { setType } = this;
    if (setType === 'NUMBER_RANGE' || setType === 'TIME_RANGE' || setType === 'STRING_RANGE') {
      let value = this.valueOf();
      value.elements = unifyElements(value.elements);
      return new Set(value);
    }
    return this;
  }

  public simplify(): any {
    let simpleSet = this.downgradeType();
    let simpleSetElements = simpleSet.elements;
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
    if (this.setType === 'NUMBER_RANGE' || this.setType === 'TIME_RANGE' || this.setType === 'STRING_RANGE') {
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
    } else {
      return this;
    }
  }

  public extent(): PlywoodRange {
    let setType = this.setType;
    if (hasOwnProperty(typeUpgrades, setType)) {
      return this.upgradeType().extent();
    }
    if (setType !== 'NUMBER_RANGE' && setType !== 'TIME_RANGE' && setType !== 'STRING_RANGE') return null;
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

    if (this.setType !== other.setType) {
      throw new TypeError("can not union sets of different types");
    }

    let newElements: Array<any> = this.elements.slice();

    let otherElements = other.elements;
    for (let el of otherElements) {
      if (this.contains(el)) continue;
      newElements.push(el);
    }

    return new Set({
      setType: this.setType,
      elements: newElements
    }).unifyElements();
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
      newElements = intersectElements(thisElements, otherElements);
    } else {
      newElements = [];
      for (let el of thisElements) {
        if (!other.contains(el)) continue;
        newElements.push(el);
      }
    }

    return new Set({
      setType: this.setType,
      elements: newElements
    });
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

  public contains(value: any): boolean {
    const { setType } = this;
    if ((setType === 'NUMBER_RANGE' && typeof value === 'number')
      || (setType === 'TIME_RANGE' && isDate(value))
      || (setType === 'STRING_RANGE' && typeof value === 'string')) {

      return this.containsWithin(value);
    }
    return hasOwnProperty(this.hash, this.keyFn(value));

  }

  public containsWithin(value: any): boolean {
    let elements = this.elements;
    for (let k in elements) {
      if (!hasOwnProperty(elements, k)) continue;
      if ((<NumberRange>elements[k]).contains(value)) return true;
    }
    return false;
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
