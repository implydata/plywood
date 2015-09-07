/// <reference path="../typings/q/Q.d.ts" />
/// <reference path="../typings/d3/d3.d.ts" />
/// <reference path="../typings/immutable-class.d.ts" />
/// <reference path="../typings/chronoshift.d.ts" />
/// <reference path="../typings/locator.d.ts" />
/// <reference path="../typings/requester.d.ts" />
/// <reference path="../typings/druid.d.ts" />
"use strict";

interface Lookup<T> { [key: string]: T }
type int = number;
interface Dummy {}

interface DELETE_START {} // This is just a marker for the declaration post processor

declare function require(file: string): any;
declare var module: { exports: any; };

var ImmutableClass = <ImmutableClass.Base>require("immutable-class");
var q = <typeof Q>require("q");
var Q_delete_me_ = q;
var D3 = <typeof d3>require("d3");
var d3_delete_me_ = D3;
var chronoshift = <typeof Chronoshift>require("chronoshift");
var Chronoshift_delete_me_ = chronoshift;

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

  export var isInstanceOf = ImmutableClass.isInstanceOf;
  export var isImmutableClass = ImmutableClass.isImmutableClass;

  export import Class = ImmutableClass.Class;
  export import Instance = ImmutableClass.Instance;

  export import Timezone = Chronoshift.Timezone;
  export import Duration = Chronoshift.Duration;

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
