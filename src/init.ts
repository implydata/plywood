/// <reference path="../typings/q/Q.d.ts" />
/// <reference path="../typings/d3/d3.d.ts" />
/// <reference path="../definitions/higher-object.d.ts" />
/// <reference path="../definitions/chronology.d.ts" />
/// <reference path="../definitions/locator.d.ts" />
/// <reference path="../definitions/requester.d.ts" />
/// <reference path="../definitions/druid.d.ts" />
"use strict";

/*========================================*\
 *                                        *
 *              WITCH CRAFT               *
 *                                        *
\*========================================*/

/*
 ~~ Description of Witchcraft ~~

 As of this writing (and my understanding[1]) TypeScript has two module modes: internal and external[2]

 External modules have a 1-1 correspondence with generated JS files and they use can use `import` / `require`
 to load each other and also 3rd party modules.
 Because it relies on require in node it will not work if there are two files, each with a class in it,
 that are interdependent

 example:  dataset.split(...) => Set  and  set.label('blah') => Dataset

 Because so many classes in Plywood are interdependent and writing the entire program as one file would suck:
 external modules are a no go.

 Internal modules have a nicer syntax and can be split across files and then compiled into one file.
 The modules are "meant" for the web environment where their external dependencies just live in the global scope.
 The only downside is the inability to use traditional `require` for loading other (3rd party) modules.

 The solution / witchcraft:
 Internal modules are used and require is defined as just a function (see ../definitions/require.d.ts).
 Required modules are also declared above allowing their type information to be used.
 The file ./exports.ts manually defines the `module` and sets `module.exports`.
 The file build order is specified in ../compile-tsc (this file is first, exports.ts is last).
 Please look at compile-tsc and exports.ts to get the full picture.
 Also checkout ../build/plywood.js to understand what it ends up looking as.

 Footnotes:
 [1] If I am wrong and there is a better way to do this PLEASE let me know; I will buy you a beer - VO
 [2] http://www.typescriptlang.org/Handbook#modules-pitfalls-of-modules

 */

interface Lookup<T> { [key: string]: T }
type int = number;
interface Dummy {}

interface DELETE_START {} // This is just a marker for the declaration post processor

declare function require(file: string): any;
declare var module: { exports: any; };

var HigherObject = <HigherObject.Base>require("higher-object");
var q = <typeof Q>require("q");
var Q_delete_me_ = q;
var D3 = <typeof d3>require("d3");
var d3_delete_me_ = D3;
var chronology = <typeof Chronology>require("chronology");
var Chronology_delete_me_ = chronology;

// --------------------------------------------------------

interface PEGParserOptions {
  cache?: boolean;
  allowedStartRules?: string;
  output?: string;
  optimize?: string;
  plugins?: any;
  [key: string]: any;
}

interface PEGParser {
  parse: (str: string, options?: PEGParserOptions) => any;
}

interface PEGParserFactory {
  (plywood: any): PEGParser;
}

// --------------------------------------------------------

var dummyObject: Dummy = {};

var objectHasOwnProperty = Object.prototype.hasOwnProperty;
function hasOwnProperty(obj: any, key: string): boolean {
  return objectHasOwnProperty.call(obj, key);
}

function repeat(str: string, times: int): string {
  return new Array(times + 1).join(str);
}

function deduplicateSort(a: string[]): string[] {
  a = a.sort();
  var newA: string[] = [];
  var last: string = null;
  for (let v of a) {
    if (v !== last) newA.push(v);
    last = v;
  }
  return newA
}

function multiMerge<T>(elements: T[], mergeFn: (a: T, b: T) => T): T[] {
  var newElements: T[] = [];
  for (let accumulator of elements) {
    let tempElements: T[] = [];
    for (let newElement of newElements) {
      var mergeElement = mergeFn(accumulator, newElement);
      if (mergeElement) {
        accumulator = mergeElement;
      } else {
        tempElements.push(newElement);
      }
    }
    tempElements.push(accumulator);
    newElements = tempElements;
  }
  return newElements;
}

function arraysEqual<T>(a: Array<T>, b: Array<T>): boolean {
  var length = a.length;
  if (length !== b.length) return false;
  for (var i = 0; i < length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function higherArraysEqual(a: Array<any>, b: Array<any>): boolean {
  var length = a.length;
  if (length !== b.length) return false;
  for (var i = 0; i < length; i++) {
    if (!a[i].equals(b[i])) return false;
  }
  return true;
}

var expressionParser: PEGParser;
var sqlParser: PEGParser;

interface DELETE_END {} // This is just a marker for the declaration post processor

module Plywood {
  export var version = '###_VERSION_###';

  export var isInstanceOf = HigherObject.isInstanceOf;
  export var isHigherObject = HigherObject.isHigherObject;

  export import ImmutableClass = HigherObject.ImmutableClass;
  export import ImmutableInstance = HigherObject.ImmutableInstance;

  export import Timezone = Chronology.Timezone;
  export import Duration = Chronology.Duration;

  export interface Datum {
    [attribute: string]: any;
  }

  export function safeAdd(num: number, delta: number): number {
    var stringDelta = String(delta);
    var dotIndex = stringDelta.indexOf(".");
    if (dotIndex === -1 || stringDelta.length === 18) {
      return num + delta;
    } else {
      var scale = Math.pow(10, stringDelta.length - dotIndex - 1);
      return (num * scale + delta * scale) / scale;
    }
  }

  export function find<T>(array: T[], fn: (value: T, index: int, array: T[]) => boolean): T {
    for (let i = 0, n = array.length; i < n; i++) {
      let a = array[i];
      if (fn.call(array, a, i)) return a;
    }
    return null;
  }

  export function continuousFloorExpression(variable: string, floorFn: string, size: number, offset: number): string {
    var expr = variable;
    if (offset !== 0) {
      expr = expr + " - " + offset;
    }
    if (offset !== 0 && size !== 1) {
      expr = "(" + expr + ")";
    }
    if (size !== 1) {
      expr = expr + " / " + size;
    }
    expr = floorFn + "(" + expr + ")";
    if (size !== 1) {
      expr = expr + " * " + size;
    }
    if (offset !== 0) {
      expr = expr + " + " + offset;
    }
    return expr;
  }
}
