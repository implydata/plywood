/// <reference path="../typings/q/Q.d.ts" />
/// <reference path="../typings/immutable-class.d.ts" />
/// <reference path="../typings/chronoshift.d.ts" />
/// <reference path="../typings/locator.d.ts" />
/// <reference path="../typings/requester.d.ts" />
/// <reference path="../typings/druid/druid.d.ts" />

interface Lookup<T> { [key: string]: T }
type int = number;
interface Dummy {}

interface DELETE_START {} // This is just a marker for the declaration post processor

declare function require(file: string): any;
declare var module: { exports: any; };

var ImmutableClass = <ImmutableClass.Base>require("immutable-class");
var q = <typeof Q>require("q");
var Q_delete_me_ = q;
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
  (plywood: any, chronoshift: any): PEGParser;
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


var expressionParser: PEGParser;
var plyqlParser: PEGParser;

interface DELETE_END {} // This is just a marker for the declaration post processor

module Plywood {
  export const version = '###_VERSION_###';

  export var isInstanceOf = ImmutableClass.isInstanceOf;
  export var isImmutableClass = ImmutableClass.isImmutableClass;
  export var generalEqual = ImmutableClass.generalEqual;
  export var immutableEqual = ImmutableClass.immutableEqual;
  export var immutableArraysEqual = ImmutableClass.immutableArraysEqual;
  export var immutableLookupsEqual = ImmutableClass.immutableLookupsEqual;

  export import Class = ImmutableClass.Class;
  export import Instance = ImmutableClass.Instance;

  export import Timezone = Chronoshift.Timezone;
  export import Duration = Chronoshift.Duration;

  export import WallTime = Chronoshift.WallTime;

  export var parseISODate = Chronoshift.parseISODate;

  // The default timezone within which dates in expressions are parsed
  export var defaultParserTimezone: Timezone = Timezone.UTC;

  export type PlywoodValue = boolean | number | string | Date | NumberRange | TimeRange | Set | Dataset | External;

  export interface PseudoDatum {
    [attribute: string]: any;
  }

  export interface Datum {
    [attribute: string]: PlywoodValue;
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
