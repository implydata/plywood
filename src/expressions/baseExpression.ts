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

import * as Promise from 'any-promise';
import * as hasOwnProp from 'has-own-prop';
import { Timezone, Duration, parseISODate } from 'chronoshift';
import { Instance, isImmutableClass, SimpleArray } from 'immutable-class';
import { ReadableStream, PassThrough } from 'readable-stream';
import { shallowCopy } from '../helper/utils';
import { promiseWhile } from '../helper/promiseWhile';
import { PlyType, DatasetFullType, PlyTypeSingleValue, FullType, PlyTypeSimple, Environment } from '../types';
import { fillExpressionExternalAlteration } from '../datatypes/index';
import { iteratorFactory, PlyBit } from '../datatypes/valueStream';
import { LiteralExpression } from './literalExpression';
import { RefExpression } from './refExpression';
import { ExternalExpression } from './externalExpression';

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
import { CountExpression } from './countExpression';
import { CountDistinctExpression } from './countDistinctExpression';
import { CustomAggregateExpression } from './customAggregateExpression';
import { CustomTransformExpression } from './customTransformExpression';
import { DivideExpression } from './divideExpression';
import { ExtractExpression } from './extractExpression';
import { FallbackExpression } from './fallbackExpression';
import { FilterExpression } from './filterExpression';
import { GreaterThanExpression } from './greaterThanExpression';
import { GreaterThanOrEqualExpression } from './greaterThanOrEqualExpression';
import { InExpression } from './inExpression';
import { IsExpression } from './isExpression';
import { JoinExpression } from './joinExpression';
import { LengthExpression } from './lengthExpression';
import { LessThanExpression } from './lessThanExpression';
import { LessThanOrEqualExpression } from './lessThanOrEqualExpression';
import { IndexOfExpression } from './indexOfExpression';
import { LookupExpression } from './lookupExpression';
import { LimitExpression } from './limitExpression';
import { MatchExpression } from './matchExpression';
import { MaxExpression } from './maxExpression';
import { MinExpression } from './minExpression';
import { MultiplyExpression } from './multiplyExpression';
import { NotExpression } from './notExpression';
import { NumberBucketExpression } from './numberBucketExpression';
import { OrExpression } from './orExpression';
import { OverlapExpression } from './overlapExpression';
import { PowerExpression } from './powerExpression';
import { QuantileExpression } from './quantileExpression';
import { SelectExpression } from './selectExpression';
import { SortExpression, Direction } from './sortExpression';
import { SplitExpression } from './splitExpression';
import { SubstrExpression } from './substrExpression';
import { SubtractExpression } from './subtractExpression';
import { SumExpression } from './sumExpression';
import { TimeBucketExpression } from './timeBucketExpression';
import { TimeFloorExpression } from './timeFloorExpression';
import { TimePartExpression } from './timePartExpression';
import { TimeRangeExpression } from './timeRangeExpression';
import { TimeShiftExpression } from './timeShiftExpression';
import { TransformCaseExpression } from './transformCaseExpression';

import { SQLDialect } from '../dialect/baseDialect';
import { repeat, emptyLookup, deduplicateSort } from '../helper/utils';
import { Dataset, Datum, PlywoodValue, NumberRange, Range, Set, StringRange, TimeRange, DatasetExternalAlterations } from '../datatypes/index';

import { isSetType, getFullTypeFromDatum, introspectDatum, failIfIntrospectNeededInDatum } from '../datatypes/common';
import { ComputeFn } from '../datatypes/dataset';
import { External, ExternalJS } from '../external/baseExternal';

export interface ComputeOptions extends Environment {
  maxQueries?: number;
  maxRows?: number;
  maxComputeCycles?: number;
}

export interface AlterationFillerPromise {
  (external: External, terminal: boolean): Promise<any>;
}

function fillExpressionExternalAlterationAsync(alteration: ExpressionExternalAlteration, filler: AlterationFillerPromise): Promise<ExpressionExternalAlteration> {
  let tasks: Promise<any>[] = [];
  fillExpressionExternalAlteration(alteration, (external, terminal) => {
    tasks.push(filler(external, terminal));
    return null;
  });

  return Promise.all(tasks).then((results) => {
    let i = 0;
    fillExpressionExternalAlteration(alteration, () => {
      let res = results[i];
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

export type ExpressionExternalAlteration = Lookup<ExpressionExternalAlterationSimple | DatasetExternalAlterations>;

export interface BooleanExpressionIterator {
  (ex: Expression, index?: int, depth?: int, nestDiff?: int): boolean;
}

export interface VoidExpressionIterator {
  (ex: Expression, index?: int, depth?: int, nestDiff?: int): void;
}

export interface SubstitutionFn {
  (ex: Expression, index?: int, depth?: int, nestDiff?: int, typeContext?: DatasetFullType): Expression;
}

export interface ExpressionMatchFn {
  (ex: Expression): boolean;
}

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

export type Alterations = Lookup<Expression>;

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
}

export interface ExpressionJS {
  op?: string;
  type?: PlyType;
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
}

export interface ExtractAndRest {
  extract: Expression;
  rest: Expression;
}

export type IfNotFound = "throw" | "leave" | "null";

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
  if (!dataset) dataset = new Dataset({ data: [{}] });
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
    type
  });
}

export function i$(name: string, nest?: number, type?: PlyType): RefExpression {
  if (typeof name !== 'string') throw new TypeError('$() argument must be a string');
  if (typeof nest === 'string') {
    type = nest as PlyType;
    nest = 0;
  }

  return new RefExpression({
    name,
    nest: nest != null ? nest : 0,
    type,
    ignoreCase: true
  });
}


export function r(value: any): LiteralExpression {
  if (value instanceof External) throw new TypeError('r() can not accept externals');
  if (Array.isArray(value)) value = Set.fromJS(value);
  return LiteralExpression.fromJS({ op: 'literal', value: value });
}

export function toJS(thing: any): any {
  return (thing && typeof thing.toJS === 'function') ? thing.toJS() : thing;
}

function chainVia(op: string, expressions: Expression[], zero: Expression): Expression {
  let n = expressions.length;
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
  static plyqlParser: PEGParser;
  static defaultParserTimezone: Timezone = Timezone.UTC; // The default timezone within which dates in expressions are parsed


  static isExpression(candidate: any): candidate is Expression {
    return candidate instanceof Expression;
  }

  static expressionLookupFromJS(expressionJSs: Lookup<ExpressionJS>): Lookup<Expression> {
    let expressions: Lookup<Expression> = Object.create(null);
    for (let name in expressionJSs) {
      if (!hasOwnProp(expressionJSs, name)) continue;
      expressions[name] = Expression.fromJSLoose(expressionJSs[name]);
    }
    return expressions;
  }

  static expressionLookupToJS(expressions: Lookup<Expression>): Lookup<ExpressionJS> {
    let expressionsJSs: Lookup<ExpressionJS> = {};
    for (let name in expressions) {
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

    let original = Expression.defaultParserTimezone;
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
   * Parses SQL statements into a plywood expressions
   * @param str The SQL to parse
   * @param timezone The timezone within which to evaluate any untimezoned date strings
   */
  static parseSQL(str: string, timezone?: Timezone): SQLParse {
    let original = Expression.defaultParserTimezone;
    if (timezone) Expression.defaultParserTimezone = timezone;
    try {
      return Expression.plyqlParser.parse(str);
    } catch (e) {
      // Re-throw to add the stacktrace
      throw new Error(`SQL parse error: ${e.message} on '${str}'`);
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
            throw new Error("unknown object"); //ToDo: better error
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
        throw new Error("unrecognizable expression");
    }

    return Expression.fromJS(expressionJS);
  }

  static inOrIs(lhs: Expression, value: any): Expression {
    let literal = r(value);
    let literalType = literal.type;
    let returnExpression: Expression = null;
    if (literalType === 'NUMBER_RANGE' || literalType === 'TIME_RANGE' || literalType === 'STRING_RANGE' || isSetType(literalType)) {
      returnExpression = lhs.in(literal);
    } else {
      returnExpression = lhs.is(literal);
    }
    return returnExpression.simplify();
  }

  static jsNullSafetyUnary(inputJS: string, ifNotNull: (str: string) => string): string {
    return `(_=${inputJS},(_==null?null:${ifNotNull('_')}))`;
  }

  static jsNullSafetyBinary(lhs: string, rhs: string, combine: (lhs: string, rhs: string) => string, lhsCantBeNull?: boolean, rhsCantBeNull?: boolean): string {
    if (lhsCantBeNull) {
      if (rhsCantBeNull) {
        return `(${combine(lhs, rhs)})`;
      } else {
        return `(_=${rhs},(_==null)?null:(${combine(lhs, '_')}))`;
      }
    } else {
      if (rhsCantBeNull) {
        return `(_=${lhs},(_==null)?null:(${combine('_', rhs)}))`;
      } else {
        return `(_=${rhs},_2=${lhs},(_==null||_2==null)?null:(${combine('_', '_2')})`;
      }
    }
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

  static classMap: Lookup<typeof Expression> = {};
  static register(ex: typeof Expression): void {
    let op = (<any>ex).op.replace(/^\w/, (s: string) => s.toLowerCase());
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
      type: js.type
    };
  }

  /**
   * Deserializes the expression JSON
   * @param expressionJS
   */
  static fromJS(expressionJS: ExpressionJS): Expression {
    if (!expressionJS) throw new Error('must have expressionJS');
    if (!hasOwnProp(expressionJS, "op")) {
      if (hasOwnProp(expressionJS, "action")) {
        expressionJS = shallowCopy(expressionJS);
        expressionJS.op = expressionJS.action;
        delete expressionJS.action;
        expressionJS.operand = { op: 'ref', name: '_' };
      } else {
        throw new Error("op must be defined");
      }
    }

    // Back compat.
    if (expressionJS.op === 'custom') {
      expressionJS = shallowCopy(expressionJS);
      expressionJS.op = 'customAggregate';
    }

    let op = expressionJS.op;
    if (typeof op !== "string") {
      throw new Error("op must be a string");
    }

    // Back compat.
    if (op === 'chain') {
      const actions = expressionJS.actions || [expressionJS.action];
      return Expression.fromJS(expressionJS.expression).performActions(actions.map(Expression.fromJS));
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

  constructor(parameters: ExpressionValue, dummy: any = null) {
    this.op = parameters.op;
    if (dummy !== dummyObject) {
      throw new TypeError("can not call `new Expression` directly use Expression.fromJS instead");
    }
    if (parameters.simple) this.simple = true;
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
    let value: ExpressionValue = { op: this.op };
    if (this.simple) value.simple = true;
    return value;
  }

  /**
   * Serializes the expression into a simple JS object that can be passed to JSON.serialize
   */
  public toJS(): ExpressionJS {
    return {
      op: this.op
    };
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
  public equals(other: Expression): boolean {
    return other instanceof Expression &&
      this.op === other.op &&
      this.type === other.type;
  }

  /**
   * Check that the expression can potentially have the desired type
   * If wanted type is 'SET' then any SET/* type is matched
   * @param wantedType The type that is wanted
   */
  public canHaveType(wantedType: string): boolean {
    let { type } =  this;
    if (!type) return true;
    if (wantedType === 'SET') {
      return isSetType(type);
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
    let value = this.valueOf();
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
    let externals: External[] = [];
    this.forEach((ex: Expression) => {
      if (ex instanceof ExternalExpression) externals.push(ex.external.getBase());
    });
    return External.deduplicateExternals(externals);
  }

  public getRawExternals(): External[] {
    let externals: External[] = [];
    this.forEach((ex: Expression) => {
      if (ex instanceof ExternalExpression) externals.push(ex.external.getRaw());
    });
    return External.deduplicateExternals(externals);
  }

  public getReadyExternals(): ExpressionExternalAlteration {
    let indexToSkip: Lookup<boolean> = {};
    let externalsByIndex: ExpressionExternalAlteration = {};

    this.every((ex: Expression, index: int) => {
      if (ex instanceof ExternalExpression) {
        if (indexToSkip[index]) return null;
        if (!ex.external.suppress) {
          externalsByIndex[index] = {
            external: ex.external,
            terminal: true
          };
        }

      } else if (ex instanceof ChainableExpression) {
        let h = ex._headExternal();
        if (h) {
          if (h.allGood) {
            externalsByIndex[index + h.offset] = { external: h.external };
            return true;
          } else {
            indexToSkip[index + h.offset] = true;
            return null;
          }
        }

      } else if (ex instanceof LiteralExpression && ex.type === 'DATASET') {
        let datasetExternals = ex.value.getReadyExternals();
        if (datasetExternals.length) externalsByIndex[index] = datasetExternals;
        return null;
      }
      return null;
    });
    return externalsByIndex;
  }

  public applyReadyExternals(alterations: ExpressionExternalAlteration): Expression {
    return this.substitute((ex, index) => {
      let alteration = alterations[index];
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
      allGood = allGood && (ex.op === 'filter' ? ex.argumentsResolvedWithoutExternals() : ex.argumentsResolved());
      ex = ex.operand;
      offset++;
    }

    if (ex instanceof ExternalExpression) {
      return {
        allGood,
        external: ex.external,
        offset
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
    let freeReferences: string[] = [];
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
    let freeReferenceIndexes: number[] = [];
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
    let freeReferenceIndexes = this.getFreeReferenceIndexes();
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

  public _everyHelper(iter: BooleanExpressionIterator, thisArg: any, indexer: Indexer, depth: int, nestDiff: int): boolean {
    let pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
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
      let v = iter.call(this, ex, index, depth, nestDiff);
      return (v == null) ? null : !v;
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
  public substitute(substitutionFn: SubstitutionFn, typeContext: DatasetFullType = null): Expression {
    return this._substituteHelper(substitutionFn, { index: 0 }, 0, 0, typeContext).expression;
  }

  public _substituteHelper(substitutionFn: SubstitutionFn, indexer: Indexer, depth: int, nestDiff: int, typeContext: DatasetFullType): ExpressionTypeContext {
    let sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff, typeContext);
    if (sub) {
      indexer.index += this.expressionCount();
      return {
        expression: sub,
        typeContext: sub.updateTypeContextIfNeeded(typeContext)
      };
    } else {
      indexer.index++;
    }

    return {
      expression: this,
      typeContext: this.updateTypeContextIfNeeded(typeContext)
    };
  }

  public abstract getFn(): ComputeFn

  public fullyDefined(): boolean {
    return true;
  }

  public abstract calc(datum: Datum): PlywoodValue

  public abstract getJS(datumVar: string): string

  public getJSFn(datumVar = 'd[]'): string {
    const { type } = this;
    let jsEx = this.getJS(datumVar);
    let body: string;
    if (type === 'NUMBER' || type === 'NUMBER_RANGE' || type === 'TIME') {
      body = `_=${jsEx};return isNaN(_)?null:_`;
    } else {
      body = `return ${jsEx};`;
    }
    return `function(${datumVar.replace('[]', '')}){var _,_2;${body}}`;
  }

  public abstract getSQL(dialect: SQLDialect): string

  public extractFromAnd(matchFn: ExpressionMatchFn): ExtractAndRest {
    if (this.type !== 'BOOLEAN') return null;
    if (matchFn(this)) {
      return {
        extract: this,
        rest: Expression.TRUE
      };
    } else {
      return {
        extract: Expression.TRUE,
        rest: this
      };
    }
  }

  public breakdownByDataset(tempNamePrefix: string): DatasetBreakdown {
    throw new Error('todo');
    // let nameIndex = 0;
    // let singleDatasetActions: ApplyAction[] = [];
    //
    // let externals = this.getBaseExternals();
    // if (externals.length < 2) {
    //   throw new Error('not a multiple dataset expression');
    // }
    //
    // let combine = this.substitute(ex => {
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
    //     singleDatasetActions.push(new ApplyAction({
    //       name: tempName,
    //       expression: ex
    //     }));
    //   }
    //
    //   return new RefExpression({
    //     name: tempName,
    //     nest: 0
    //   });
    // });
    // return {
    //   combineExpression: combine,
    //   singleDatasetActions: singleDatasetActions
    // };
  }

  public getLiteralValue(): any {
    return null;
  }

  public bumpStringLiteralToSetString(): Expression {
    return this;
  }

  public upgradeToType(targetType: PlyType): Expression {
    return this;
  }

  public isAction(): boolean {
    return false;
  }

  public performAction(action: Expression): Expression {
    if (!action.isAction()) throw new Error(`${action} is not an action`);
    return action.substitute((ex) => ex.equals(Expression._) ? this : null);
  }

  public performActions(actions: Expression[]): Expression {
    let ex: Expression = this;
    for (let action of actions) ex = ex.performAction(action);
    return ex;
  }

  // ------------------------------------------------------------------------
  // API behaviour

  private _mkChain<T extends ChainableUnaryExpression>(ExpressionClass: any, exs: any[]): T {
    let cur: any = this;
    for (let ex of exs) {
      cur = new ExpressionClass({
        operand: cur,
        expression: ex instanceof Expression ? ex : Expression.fromJSLoose(ex)
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

  public match(re: string) {
    return new MatchExpression({ operand: this, regexp: getString(re) });
  }

  public in(start: Date, end: Date): InExpression;
  public in(start: number, end: number): InExpression;
  public in(start: string, end: string): InExpression;
  public in(ex: any): InExpression;
  public in(ex: any, snd?: any): InExpression {
    if (arguments.length === 2) {
      ex = getValue(ex);
      snd = getValue(snd);

      if (typeof ex === 'string') {
        let parse = parseISODate(ex, Expression.defaultParserTimezone);
        if (parse) ex = parse;
      }

      if (typeof snd === 'string') {
        let parse = parseISODate(snd, Expression.defaultParserTimezone);
        if (parse) snd = parse;
      }

      if (typeof ex === 'number' && typeof snd === 'number') {
        ex = new NumberRange({ start: ex, end: snd });
      } else if (ex.toISOString && snd.toISOString) {
        ex = new TimeRange({ start: ex, end: snd });
      } else if (typeof ex === 'string' && typeof snd === 'string') {
        ex = new StringRange({ start: ex, end: snd });
      } else {
        throw new Error('uninterpretable IN parameters');
      }
    }
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new InExpression({ operand: this, expression: ex });
  }

  public overlap(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new OverlapExpression({ operand: this, expression: ex.bumpStringLiteralToSetString() });
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

  // String manipulation

  public substr(position: number, len: number) {
    return new SubstrExpression({ operand: this, position: getNumber(position), len: getNumber(len) });
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
    return new TransformCaseExpression({ operand: this, transformType: getString(transformType) as CaseType });
  }

  public customTransform(custom: string, outputType?: PlyTypeSingleValue) {
    if (!custom) throw new Error("Must provide an extraction function name for custom transform");
    outputType = outputType !== undefined ? getString(outputType) as PlyTypeSingleValue : null;
    return new CustomTransformExpression({ operand: this, custom: getString(custom), outputType });
  }

  // Number manipulation

  public numberBucket(size: number, offset = 0) {
    return new NumberBucketExpression({ operand: this, size: getNumber(size), offset: getNumber(offset) });
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
    if (timezone && !(timezone instanceof Timezone)) timezone = Timezone.fromJS(getString(timezone));
    return new TimeBucketExpression({ operand: this, duration, timezone });
  }

  public timeFloor(duration: any, timezone?: any) {
    if (!(duration instanceof Duration)) duration = Duration.fromJS(getString(duration));
    if (timezone && !(timezone instanceof Timezone)) timezone = Timezone.fromJS(getString(timezone));
    return new TimeFloorExpression({ operand: this, duration, timezone });
  }

  public timeShift(duration: any, step?: number, timezone?: any) {
    if (!(duration instanceof Duration)) duration = Duration.fromJS(getString(duration));
    step = typeof step !== 'undefined' ? getNumber(step) : null;
    if (timezone && !(timezone instanceof Timezone)) timezone = Timezone.fromJS(getString(timezone));
    return new TimeShiftExpression({ operand: this, duration, step, timezone });
  }

  public timeRange(duration: any, step?: number, timezone?: any) {
    if (!(duration instanceof Duration)) duration = Duration.fromJS(getString(duration));
    step = typeof step !== 'undefined' ? getNumber(step) : null;
    if (timezone && !(timezone instanceof Timezone)) timezone = Timezone.fromJS(getString(timezone));
    return new TimeRangeExpression({ operand: this, duration, step, timezone });
  }

  public timePart(part: string, timezone?: any) {
    if (timezone && !(timezone instanceof Timezone)) timezone = Timezone.fromJS(getString(timezone));
    return new TimePartExpression({ operand: this, part: getString(part), timezone });
  }

  public cast(outputType: PlyType) {
    return new CastExpression({ operand: this, outputType: getString(outputType) as PlyTypeSimple });
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
  public split(ex: any, name: string, dataName?: string): SplitExpression;
  public split(splits: any, name?: string, dataName?: string): SplitExpression {
    // Determine if use case #2
    if (arguments.length === 3 ||
      (arguments.length === 2 && splits && (typeof splits === 'string' || typeof splits.op === 'string'))) {
      name = getString(name);
      let realSplits = Object.create(null);
      realSplits[name] = splits;
      splits = realSplits;
    } else {
      dataName = name;
    }

    let parsedSplits: Splits = Object.create(null);
    for (let k in splits) {
      if (!hasOwnProp(splits, k)) continue;
      let ex = splits[k];
      parsedSplits[k] = ex instanceof Expression ? ex : Expression.fromJSLoose(ex);
    }

    dataName = dataName ? getString(dataName) : getDataName(this);
    if (!dataName) throw new Error("could not guess data name in `split`, please provide one explicitly");
    return new SplitExpression({ operand: this, splits: parsedSplits, dataName: dataName });
  }

  /**
   * Evaluate some expression on every datum in the dataset. Record the result as `name`
   * @param name The name of where to store the results
   * @param ex The expression to evaluate
   */
  public apply(name: string, ex: any) {
    if (arguments.length < 2) throw new Error('invalid arguments to .apply, did you forget to specify a name?');
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new ApplyExpression({ operand: this, name: getString(name), expression: ex });
  }

  public sort(ex: any, direction?: Direction) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new SortExpression({ operand: this, expression: ex, direction: direction ? (getString(direction) as Direction) : null });
  }

  public limit(value: number) {
    return new LimitExpression({ operand: this, value: getNumber(value) });
  }

  public select(attributes: string[]): SelectExpression;
  public select(...attributes: any[]): SelectExpression {
    attributes = (attributes.length === 1 && Array.isArray(attributes[0])) ? attributes[0] : attributes.map(getString);
    return new SelectExpression({ operand: this, attributes });
  }


  // Aggregate expressions

  public count() {
    if (arguments.length) throw new Error('.count() should not have arguments, did you want to .filter().count() ?');
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

  public quantile(ex: any, value: number) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new QuantileExpression({ operand: this, expression: ex, value: getNumber(value) });
  }

  public collect(ex: any) {
    if (!(ex instanceof Expression)) ex = Expression.fromJSLoose(ex);
    return new CollectExpression({ operand: this, expression: ex });
  }

  public customAggregate(custom: string) {
    return new CustomAggregateExpression({ operand: this, custom: getString(custom) });
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
    if (typeof environment.timezone === 'string') environment = { timezone: Timezone.fromJS(environment.timezone as any) };

    return this.substitute((ex) => {
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
    return this.substitute((ex: Expression, index: int, depth: int, nestDiff: int, typeContext: DatasetFullType) => {
      if (ex instanceof RefExpression) {
        return ex.changeInTypeContext(typeContext);
      }
      return null;
    }, typeContext);
  }

  public updateTypeContext(typeContext: DatasetFullType, extra?: any): DatasetFullType {
    return typeContext;
  }

  public updateTypeContextIfNeeded(typeContext: DatasetFullType | null, extra?: any): DatasetFullType | null {
    return typeContext ? this.updateTypeContext(typeContext, extra) : null;
  }


  /**
   * Resolves one level of dependencies that refer outside of this expression.
   * @param context The context containing the values to resolve to
   * @param ifNotFound If the reference is not in the context what to do? "throw", "leave", "null"
   * @return The resolved expression
   */
  public resolve(context: Datum, ifNotFound: IfNotFound = 'throw'): Expression {
    let expressions: Lookup<Expression> = Object.create(null);
    for (let k in context) {
      if (!hasOwnProp(context, k)) continue;
      let value = context[k];
      if (value instanceof External) {
        expressions[k] = new ExternalExpression({ external: <External>value });
      } else if (value instanceof Expression) {
        expressions[k] = value;
      } else {
        expressions[k] = new LiteralExpression({ value });
      }
    }

    return this.resolveWithExpressions(expressions, ifNotFound);
  }

  public resolveWithExpressions(expressions: Lookup<Expression>, ifNotFound: IfNotFound = 'throw'): Expression {
    return this.substitute((ex: Expression, index: int, depth: int, nestDiff: int) => {
      if (ex instanceof RefExpression) {
        const { nest, ignoreCase, name } = ex;
        if (nestDiff === nest) {
          let foundExpression: Expression = null;
          let valueFound = false;
          let property = ignoreCase ? RefExpression.findPropertyCI(expressions, name) : RefExpression.findProperty(expressions, name);
          if (property != null) {
            foundExpression = expressions[property];
            valueFound = true;
          }

          if (foundExpression instanceof ExternalExpression) {
            let mode = foundExpression.external.mode;

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
      return (ex instanceof RefExpression) ? ex.nest <= nestDiff : null; // Search within
    });
  }

  public resolvedWithoutExternals(): boolean {
    return this.every((ex, index, depth, nestDiff) => {
      if (ex instanceof ExternalExpression) return false;
      return (ex instanceof RefExpression) ? ex.nest <= nestDiff : null; // Search within
    });
  }

  public noRefs(): boolean {
    return this.every((ex) => {
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
    return this.substitute((ex) => {
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
      let distributedEx = ex.distribute();
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
    return this.defineEnvironment(environment)
      .referenceCheck(context)
      .resolve(context)
      .simplify();
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

    let simulatedQueryGroups: any[] = [];
    readyExpression._computeResolvedSimulate(options, simulatedQueryGroups);
    return simulatedQueryGroups;
  }

  private _computeResolvedSimulate(options: ComputeOptions, simulatedQueryGroups: any[][]): PlywoodValue {
    const {
      maxComputeCycles = 5,
      maxQueries = 500
    } = options;

    let ex: Expression = this;
    let readyExternals = ex.getReadyExternals();

    let computeCycles = 0;
    let queries = 0;

    while (Object.keys(readyExternals).length > 0 && computeCycles < maxComputeCycles && queries < maxQueries) {
      let simulatedQueryGroup: any[] = [];
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
      readyExternals = ex.getReadyExternals();
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

    introspectDatum(context)
      .then((introspectedContext: Datum) => {
        let readyExpression = this._initialPrepare(introspectedContext, options);
        if (readyExpression instanceof ExternalExpression) {
          // Top level externals need to be unsuppressed
          //readyExpression = readyExpression.unsuppress();
          let s = readyExpression.external.queryValueStream(true);
          s.pipe(pt);
          s.on('error', (e: Error) => pt.emit('error', e));
          return;
        }

        readyExpression._computeResolved(options)
          .then((v) => {
            const i = iteratorFactory(v as Dataset);
            let bit: PlyBit;
            while (bit = i()) {
              pt.write(bit);
            }
            pt.end();
          });
      })
      .catch((e) => {
        pt.emit('error', e);
      });

    return pt;
  }

  private _computeResolved(options: ComputeOptions): Promise<PlywoodValue> {
    const {
      maxComputeCycles = 5,
      maxQueries = 500
    } = options;

    let ex: Expression = this;
    let readyExternals = ex.getReadyExternals();

    let computeCycles = 0;
    let queries = 0;

    return promiseWhile(
      () => Object.keys(readyExternals).length > 0 && computeCycles < maxComputeCycles && queries < maxQueries,
      () => {
        return fillExpressionExternalAlterationAsync(readyExternals, (external, terminal) => {
          if (queries < maxQueries) {
            queries++;
            return external.queryValue(terminal);
          } else {
            queries++;
            return Promise.resolve(null); // Query limit reached, don't do any more queries.
          }
        })
          .then((readyExternalsFilled) => {
            ex = ex.applyReadyExternals(readyExternalsFilled);
            readyExternals = ex.getReadyExternals();
            computeCycles++;
          });
      }
    )
      .then(() => {
        if (!ex.isOp('literal')) throw new Error(`something went wrong, did not get literal: ${ex}`);
        return ex.getLiteralValue();
      });
  }

}


export abstract class ChainableExpression extends Expression {
  static jsToValue(js: ExpressionJS): ExpressionValue {
    let value = Expression.jsToValue(js);
    value.operand = js.operand ? Expression.fromJS(js.operand) : Expression._;
    return value;
  }

  public operand: Expression;

  constructor(value: ExpressionValue, dummy: any = null) {
    super(value, dummy);
    this.operand = value.operand || Expression._;
  }

  protected _checkOperandTypes(...neededTypes: string[]) {
    let operandType = this.operand.type;
    if (operandType && operandType !== 'NULL' && neededTypes.indexOf(operandType) === -1) {
      if (neededTypes.length === 1) {
        throw new Error(`${this.op} must have operand of type ${neededTypes[0]} (is ${operandType})`);
      } else {
        throw new Error(`${this.op} must have operand of type ${neededTypes.join(' or ')} (is ${operandType})`);
      }
    }
  }

  protected _bumpOperandToTime() {
    if (this.operand.type === 'STRING') {
      this.operand = this.operand.upgradeToType('TIME');
    }
  }

  public valueOf(): ExpressionValue {
    let value = super.valueOf();
    value.operand = this.operand;
    return value;
  }

  public toJS(): ExpressionJS {
    let js = super.toJS();
    if (!this.operand.equals(Expression._)) {
      js.operand = this.operand.toJS();
    }
    return js;
  }

  protected _toStringParameters(indent?: int): string[] {
    return [];
  }

  public toString(indent?: int): string {
    return `${this.operand.toString(indent)}.${this.op}(${this._toStringParameters(indent).join(',')})`;
  }

  public equals(other: ChainableExpression): boolean {
    return super.equals(other) &&
      this.operand.equals(other.operand);
  }

  public changeOperand(operand: Expression): this {
    if (this.operand === operand || this.operand.equals(operand)) return this;

    let value = this.valueOf();
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

  public isAction(): boolean {
    return this.operand.equals(Expression._);
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
    this.getArgumentExpressions().forEach(ex => sum += ex.expressionCount());
    return sum;
  }

  public argumentsResolved(): boolean {
    return this.getArgumentExpressions().every((ex) => ex.resolved());
  }

  public argumentsResolvedWithoutExternals(): boolean {
    return this.getArgumentExpressions().every((ex) => ex.resolvedWithoutExternals());
  }

  public getFn(): ComputeFn {
    // ToDo: this should be moved into Expression
    return (d: Datum) => this.calc(d);
  }


  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    throw runtimeAbstract();
  }

  public fullyDefined(): boolean {
    return this.operand.isOp('literal');
  }

  public calc(datum: Datum): PlywoodValue {
    return this._calcChainableHelper(this.operand.calc(datum));
  }


  protected _getJSChainableHelper(operandJS: string): string {
    throw runtimeAbstract();
  }

  public getJS(datumVar: string): string {
    return this._getJSChainableHelper(this.operand.getJS(datumVar));
  }


  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    throw runtimeAbstract();
  }

  public getSQL(dialect: SQLDialect): string {
    return this._getSQLChainableHelper(dialect, this.operand.getSQL(dialect));
  }


  public pushIntoExternal(): ExternalExpression | null {
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

  public _everyHelper(iter: BooleanExpressionIterator, thisArg: any, indexer: Indexer, depth: int, nestDiff: int): boolean {
    let pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
    if (pass != null) {
      return pass;
    } else {
      indexer.index++;
    }
    depth++;

    let operand = this.operand;
    if (!operand._everyHelper(iter, thisArg, indexer, depth, nestDiff)) return false;

    let nestDiffNext = nestDiff + Number(this.isNester());
    return this.getArgumentExpressions().every((ex) => ex._everyHelper(iter, thisArg, indexer, depth, nestDiffNext));
  }

  public _substituteHelper(substitutionFn: SubstitutionFn, indexer: Indexer, depth: int, nestDiff: int, typeContext: DatasetFullType): ExpressionTypeContext {
    let sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff, typeContext);
    if (sub) {
      indexer.index += this.expressionCount();
      return {
        expression: sub,
        typeContext: sub.updateTypeContextIfNeeded(typeContext)
      };
    } else {
      indexer.index++;
    }
    depth++;

    const operandSubs = this.operand._substituteHelper(substitutionFn, indexer, depth, nestDiff, typeContext);
    const updatedThis = this.changeOperand(operandSubs.expression);

    return {
      expression: updatedThis,
      typeContext: updatedThis.updateTypeContextIfNeeded(operandSubs.typeContext)
    };
  }

}


export abstract class ChainableUnaryExpression extends ChainableExpression {
  static jsToValue(js: ExpressionJS): ExpressionValue {
    let value = ChainableExpression.jsToValue(js);
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
    let expressionType = this.expression.type;
    if (expressionType && expressionType !== 'NULL' && neededTypes.indexOf(expressionType) === -1) {
      if (neededTypes.length === 1) {
        throw new Error(`${this.op} must have expression of type ${neededTypes[0]} (is ${expressionType})`);
      } else {
        throw new Error(`${this.op} must have expression of type ${neededTypes.join(' or ')} (is ${expressionType})`);
      }
    }
  }

  protected _checkOperandExpressionTypesAlign() {
    let operandType = this.operand.type;
    let expressionType = this.expression.type;
    if (!operandType || operandType === 'NULL' || !expressionType || expressionType === 'NULL' || operandType === expressionType) return;
    throw new Error(`${this.op} must have matching types (are ${operandType}, ${expressionType})`);
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
    let value = super.valueOf();
    value.expression = this.expression;
    return value;
  }

  public toJS(): ExpressionJS {
    let js = super.toJS();
    js.expression = this.expression.toJS();
    return js;
  }

  protected _toStringParameters(indent?: int): string[] {
    return [this.expression.toString(indent)];
  }

  public toString(indent?: int): string {
    // ToDo: handle indent
    return `${this.operand.toString(indent)}.${this.op}(${this._toStringParameters(indent).join(',')})`;
  }

  public equals(other: ChainableUnaryExpression): boolean {
    return super.equals(other) &&
      this.expression.equals(other.expression);
  }

  public changeExpression(expression: Expression): this {
    if (this.expression === expression || this.expression.equals(expression)) return this;

    let value = this.valueOf();
    value.expression = expression;
    delete value.simple;
    return Expression.fromValue(value) as any;
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    throw runtimeAbstract();
  }

  public fullyDefined(): boolean {
    return this.operand.isOp('literal') && this.expression.isOp('literal');
  }

  public calc(datum: Datum): PlywoodValue {
    return this._calcChainableUnaryHelper(this.operand.calc(datum), this.isNester() ? null : this.expression.calc(datum));
  }


  protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string {
    throw runtimeAbstract();
  }

  public getJS(datumVar: string): string {
    return this._getJSChainableUnaryHelper(this.operand.getJS(datumVar), this.expression.getJS(datumVar));
  }


  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    throw runtimeAbstract();
  }

  public getSQL(dialect: SQLDialect): string {
    return this._getSQLChainableUnaryHelper(dialect, this.operand.getSQL(dialect), this.expression.getSQL(dialect));
  }

  public getExpressionList(): Expression[] {
    const { op, operand, expression } = this;
    let expressionList = [expression];
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
        expression: (expression as ChainableUnaryExpression).operand
      }),
      expression: (expression as ChainableUnaryExpression).expression
    });
  }

  public associateRightIfSimpler(): this | null {
    if (!this.isAssociative()) return null;
    const { op, operand, expression } = this;
    if (op !== operand.op) return null;
    const MyClass: any = this.constructor;

    const simpleExpression = new MyClass({
      operand: (operand as ChainableUnaryExpression).expression,
      expression: expression
    }).simplify();

    if (simpleExpression instanceof LiteralExpression) {
      return new MyClass({
        operand: (operand as ChainableUnaryExpression).operand,
        expression: simpleExpression
      }).simplify();
    } else {
      return null;
    }
  }

  public pushIntoExternal(): ExternalExpression | null {
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

    let simpleOperand = this.operand.simplify();
    let simpleExpression = this.expression.simplify();
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
    let assLeft = (simpler as ChainableUnaryExpression).associateLeft();
    if (assLeft) return assLeft.simplify();

    if (simpler instanceof ChainableUnaryExpression) {
      let specialSimpler = simpler.specialSimplify();
      if (specialSimpler !== simpler) {
        return specialSimpler.simplify();
      } else {
        simpler = specialSimpler;
      }

      if (simpler instanceof ChainableUnaryExpression) {
        // Try to associate right and pick that if that is simpler
        let assRight = simpler.associateRightIfSimpler();
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

  public _substituteHelper(substitutionFn: SubstitutionFn, indexer: Indexer, depth: int, nestDiff: int, typeContext: DatasetFullType): ExpressionTypeContext {
    let sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff);
    if (sub) {
      indexer.index += this.expressionCount();
      return {
        expression: sub,
        typeContext: sub.updateTypeContextIfNeeded(typeContext)
      };
    } else {
      indexer.index++;
    }
    depth++;

    const operandSubs = this.operand._substituteHelper(substitutionFn, indexer, depth, nestDiff, typeContext);
    const nestDiffNext = nestDiff + Number(this.isNester());
    const expressionSubs = this.expression._substituteHelper(substitutionFn, indexer, depth, nestDiffNext, this.isNester() ? operandSubs.typeContext : typeContext);
    const updatedThis = this.changeOperand(operandSubs.expression).changeExpression(expressionSubs.expression);

    return {
      expression: updatedThis,
      typeContext: updatedThis.updateTypeContextIfNeeded(operandSubs.typeContext, expressionSubs.typeContext)
    };
  }
}
