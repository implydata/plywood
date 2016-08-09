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

var objectHasOwnProperty = Object.prototype.hasOwnProperty;
function hasOwnProperty(obj: any, key: string): boolean {
  return objectHasOwnProperty.call(obj, key);
}

function repeat(str: string, times: int): string {
  return new Array(times + 1).join(str);
}

function arraysEqual<T>(a: Array<T>, b: Array<T>): boolean {
  if (a === b) return true;
  var length = a.length;
  if (length !== b.length) return false;
  for (var i = 0; i < length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function dictEqual(dictA: Lookup<any>, dictB: Lookup<any>): boolean {
  if (dictA === dictB) return true;
  if (!dictA !== !dictB) return false;
  var keys = Object.keys(dictA);
  if (keys.length !== Object.keys(dictB).length) return false;
  for (var key of keys) {
    if (dictA[key] !== dictB[key]) return false;
  }
  return true;
}

export function parseJSON(text: string): any[] {
  text = text.trim();
  var firstChar = text[0];

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

export function find<T>(array: T[], fn: (value: T, index: int, array: T[]) => boolean): T {
  for (let i = 0, n = array.length; i < n; i++) {
    let a = array[i];
    if (fn.call(array, a, i)) return a;
  }
  return null;
}

export function findIndex<T>(array: T[], fn: (value: T, index: int, array: T[]) => boolean): int {
  for (let i = 0, n = array.length; i < n; i++) {
    let a = array[i];
    if (fn.call(array, a, i)) return i;
  }
  return -1;
}

export interface Nameable {
  name: string;
}

export function findByName<T extends Nameable>(array: T[], name: string): T {
  return find(array, (x) => x.name === name);
}

export function findIndexByName<T extends Nameable>(array: T[], name: string): int {
  return findIndex(array, (x) => x.name === name);
}

export function overrideByName<T extends Nameable>(things: T[], thingOverride: T): T[] {
  var overrideName = thingOverride.name;
  var added = false;
  things = things.map(t => {
    if (t.name === overrideName) {
      added = true;
      return thingOverride;
    } else {
      return t;
    }
  });
  if (!added) things.push(thingOverride);
  return things;
}

export function overridesByName<T extends Nameable>(things: T[], thingOverrides: T[]): T[] {
  for (var thingOverride of thingOverrides) {
    things = overrideByName(things, thingOverride);
  }
  return things;
}


export function shallowCopy<T>(thing: Lookup<T>): Lookup<T> {
  var newThing: Lookup<T> = {};
  for (var k in thing) {
    if (hasOwnProperty(thing, k)) newThing[k] = thing[k];
  }
  return newThing;
}

export function deduplicateSort(a: string[]): string[] {
  a = a.sort();
  var newA: string[] = [];
  var last: string = null;
  for (let v of a) {
    if (v !== last) newA.push(v);
    last = v;
  }
  return newA
}

export function mapLookup<T, U>(thing: Lookup<T>, fn: (x: T) => U): Lookup<U> {
  var newThing: Lookup<U> = Object.create(null);
  for (var k in thing) {
    if (hasOwnProperty(thing, k)) newThing[k] = fn(thing[k]);
  }
  return newThing;
}

export function emptyLookup(lookup: Lookup<any>): boolean {
  for (var k in lookup) {
    if (hasOwnProperty(lookup, k)) return false;
  }
  return true;
}

export function nonEmptyLookup(lookup: Lookup<any>): boolean {
  return !emptyLookup(lookup);
}

export function expressionLookupFromJS(expressionJSs: Lookup<ExpressionJS>): Lookup<Expression> {
  var expressions: Lookup<Expression> = Object.create(null);
  for (var name in expressionJSs) {
    if (!hasOwnProperty(expressionJSs, name)) continue;
    expressions[name] = Expression.fromJSLoose(expressionJSs[name]);
  }
  return expressions;
}

export function expressionLookupToJS(expressions: Lookup<Expression>): Lookup<ExpressionJS> {
  var expressionsJSs: Lookup<ExpressionJS> = {};
  for (var name in expressions) {
    if (!hasOwnProperty(expressions, name)) continue;
    expressionsJSs[name] = expressions[name].toJS();
  }
  return expressionsJSs;
}

