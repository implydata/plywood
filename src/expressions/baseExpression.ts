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

import * as Q from 'q';
import { Timezone, Duration, parseISODate } from 'chronoshift';
import { Instance, isInstanceOf, isImmutableClass, SimpleArray } from 'immutable-class';
import { promiseWhile } from '../helper/promiseWhile';
import { PlyType, DatasetFullType, PlyTypeSingleValue, FullType, PlyTypeSimple } from '../types';
import { fillExpressionExternalAlteration } from '../datatypes/index';
import { LiteralExpression } from './literalExpression';
import { ChainExpression } from './chainExpression';
import { RefExpression } from './refExpression';
import { ExternalExpression } from './externalExpression';
import { SQLDialect } from '../dialect/baseDialect';
import {
  Action,
  AbsoluteAction,
  ApplyAction,
  AverageAction,
  CardinalityAction,
  CastAction,
  CollectAction,
  ContainsAction,
  CountAction,
  CountDistinctAction,
  CustomAggregateAction,
  CustomTransformAction,
  ExtractAction,
  FallbackAction,
  FilterAction,
  GreaterThanAction,
  GreaterThanOrEqualAction,
  InAction,
  IndexOfAction,
  IsAction,
  JoinAction,
  LengthAction,
  LessThanAction,
  LessThanOrEqualAction,
  LimitAction,
  LookupAction,
  MatchAction,
  MaxAction,
  MinAction,
  NotAction,
  NumberBucketAction,
  OverlapAction,
  QuantileAction,
  SelectAction,
  SortAction,
  SplitAction,
  SubstrAction,
  SumAction,
  TimeBucketAction,
  TimeFloorAction,
  TimePartAction,
  TimeRangeAction,
  TimeShiftAction,
  TransformCaseAction,
  Environment
} from '../actions/index';
import { hasOwnProperty, repeat, emptyLookup, deduplicateSort } from '../helper/utils';
import { Dataset, Datum, PlywoodValue, NumberRange, Range, Set, StringRange, TimeRange, DatasetExternalAlterations } from '../datatypes/index';
import { ActionJS, CaseType, Splits } from '../actions/baseAction';
import { Direction } from '../actions/sortAction';
import { isSetType, getFullTypeFromDatum, introspectDatum, failItIntrospectNeededInDatum } from '../datatypes/common';
import { ComputeFn } from '../datatypes/dataset';
import { External, ExternalJS } from '../external/baseExternal';

export interface AlterationFillerPromise {
  (external: External, terminal: boolean): Q.Promise<any>;
}

function fillExpressionExternalAlterationAsync(alteration: ExpressionExternalAlteration, filler: AlterationFillerPromise): Q.Promise<ExpressionExternalAlteration> {
  let tasks: Q.Promise<any>[] = [];
  fillExpressionExternalAlteration(alteration, (external, terminal) => {
    tasks.push(filler(external, terminal));
    return null;
  });

  return Q.all(tasks).then((results) => {
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
  (ex?: Expression, index?: int, depth?: int, nestDiff?: int): boolean;
}

export interface VoidExpressionIterator {
  (ex?: Expression, index?: int, depth?: int, nestDiff?: int): void;
}

export interface SubstitutionFn {
  (ex?: Expression, index?: int, depth?: int, nestDiff?: int): Expression;
}

export interface ExpressionMatchFn {
  (ex?: Expression): boolean;
}

export interface ActionMatchFn {
  (action?: Action): boolean;
}

export interface ActionSubstitutionFn {
  (preEx?: Expression, action?: Action): Expression;
}

export interface DatasetBreakdown {
  singleDatasetActions: ApplyAction[];
  combineExpression: Expression;
}

export interface Digest {
  expression: Expression;
  undigested: ApplyAction;
}

export interface Indexer {
  index: int;
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

export interface ExpressionValue {
  op?: string;
  type?: PlyType;
  simple?: boolean;
  value?: any;
  name?: string;
  nest?: int;
  external?: External;
  expression?: Expression;
  actions?: Action[];
  ignoreCase?: boolean;

  remote?: boolean;
}

export interface ExpressionJS {
  op: string;
  type?: PlyType;
  value?: any;
  name?: string;
  nest?: int;
  external?: ExternalJS;
  expression?: ExpressionJS;
  action?: ActionJS;
  actions?: ActionJS[];
  ignoreCase?: boolean;
}

export interface ExtractAndRest {
  extract: Expression;
  rest: Expression;
}

export type IfNotFound = "throw" | "leave" | "null";

export interface SubstituteActionOptions {
  onceInChain?: boolean;
}

function getDataName(ex: Expression): string {
  if (ex instanceof RefExpression) {
    return ex.name;
  } else if (ex instanceof ChainExpression) {
    return getDataName(ex.expression);
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
  if (External.isExternal(value)) throw new TypeError('r can not accept externals');
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
  if (!Expression.isExpression(acc)) acc = Expression.fromJSLoose(acc);
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

  static expressionParser: PEGParser;
  static plyqlParser: PEGParser;
  static defaultParserTimezone: Timezone = Timezone.UTC; // The default timezone within which dates in expressions are parsed


  static isExpression(candidate: any): candidate is Expression {
    return isInstanceOf(candidate, Expression);
  }

  static expressionLookupFromJS(expressionJSs: Lookup<ExpressionJS>): Lookup<Expression> {
    let expressions: Lookup<Expression> = Object.create(null);
    for (let name in expressionJSs) {
      if (!hasOwnProperty(expressionJSs, name)) continue;
      expressions[name] = Expression.fromJSLoose(expressionJSs[name]);
    }
    return expressions;
  }

  static expressionLookupToJS(expressions: Lookup<Expression>): Lookup<ExpressionJS> {
    let expressionsJSs: Lookup<ExpressionJS> = {};
    for (let name in expressions) {
      if (!hasOwnProperty(expressions, name)) continue;
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
        } else if (Expression.isExpression(param)) {
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
        } else if (hasOwnProperty(param, 'start') && hasOwnProperty(param, 'end')) {
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
    let literal = new LiteralExpression({
      op: 'literal',
      value: value
    });

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
    let op = (<any>ex).name.replace('Expression', '').replace(/^\w/, (s: string) => s.toLowerCase());
    Expression.classMap[op] = ex;
  }

  /**
   * Deserializes the expression JSON
   * @param expressionJS
   */
  static fromJS(expressionJS: ExpressionJS): Expression {
    if (!hasOwnProperty(expressionJS, "op")) {
      throw new Error("op must be defined");
    }
    let op = expressionJS.op;
    if (typeof op !== "string") {
      throw new Error("op must be a string");
    }
    let ClassFn = Expression.classMap[op];
    if (!ClassFn) {
      throw new Error(`unsupported expression op '${op}'`);
    }

    return ClassFn.fromJS(expressionJS);
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

  public toString(indent?: int): string {
    return 'BaseExpression';
  }

  /**
   * Validate that two expressions are equal in their meaning
   * @param other
   */
  public equals(other: Expression): boolean {
    return Expression.isExpression(other) &&
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
      if (ex instanceof RefExpression) return ex.isRemote();
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

      } else if (ex instanceof ChainExpression) {
        let exExpression = ex.expression;
        if (exExpression instanceof ExternalExpression) {
          let actionsLookGood = ex.actions.every(action => {
            return action.action === 'filter' ? action.resolvedWithoutExternals() : action.resolved();
          });
          if (actionsLookGood) {
            externalsByIndex[index + 1] = { external: exExpression.external };
            return true;
          }
          // this will look further but we should not be looking into the expression of this chain
          indexToSkip[index + 1] = true;
          return null;
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
   * @param thisArg The this for the substitution function
   */
  public substitute(substitutionFn: SubstitutionFn, thisArg?: any): Expression {
    return this._substituteHelper(substitutionFn, thisArg, { index: 0 }, 0, 0);
  }

  public _substituteHelper(substitutionFn: SubstitutionFn, thisArg: any, indexer: Indexer, depth: int, nestDiff: int): Expression {
    let sub = substitutionFn.call(thisArg, this, indexer.index, depth, nestDiff);
    if (sub) {
      indexer.index += this.expressionCount();
      return sub;
    } else {
      indexer.index++;
    }

    return this;
  }

  public substituteAction(actionMatchFn: ActionMatchFn, actionSubstitutionFn: ActionSubstitutionFn, options: SubstituteActionOptions = {}, thisArg?: any): Expression {
    return this.substitute((ex: Expression) => {
      if (ex instanceof ChainExpression) {
        let actions = ex.actions;
        for (let i = 0; i < actions.length; i++) {
          let action = actions[i];
          if (actionMatchFn.call(this, action)) {
            let newEx = actionSubstitutionFn.call(this, ex.headActions(i), action);
            if (newEx) {
              for (let j = i + 1; j < actions.length; j++) newEx = newEx.performAction(actions[j]);
              if (options.onceInChain) return newEx;
              return newEx.substituteAction(actionMatchFn, actionSubstitutionFn, options, this);
            }
          }
        }
      }
      return null;
    }, thisArg);
  }

  public abstract getFn(): ComputeFn

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
    let nameIndex = 0;
    let singleDatasetActions: ApplyAction[] = [];

    let externals = this.getBaseExternals();
    if (externals.length < 2) {
      throw new Error('not a multiple dataset expression');
    }

    let combine = this.substitute(ex => {
      let externals = ex.getBaseExternals();
      if (externals.length !== 1) return null;

      let existingApply = SimpleArray.find(singleDatasetActions, (apply) => apply.expression.equals(ex));

      let tempName: string;
      if (existingApply) {
        tempName = existingApply.name;
      } else {
        tempName = tempNamePrefix + (nameIndex++);
        singleDatasetActions.push(new ApplyAction({
          action: 'apply',
          name: tempName,
          expression: ex
        }));
      }

      return new RefExpression({
        op: 'ref',
        name: tempName,
        nest: 0
      });
    });
    return {
      combineExpression: combine,
      singleDatasetActions: singleDatasetActions
    };
  }

  public actionize(containingAction: string): Action[] {
    return null;
  }

  public getExpressionPattern(actionType: string): Expression[] {
    let actions = this.actionize(actionType);
    return actions ? actions.map((action) => action.expression) : null;
  }

  /**
   * Returns the first action
   * Returns null there are no actions
   */
  public firstAction(): Action {
    return null;
  }

  /**
   * Returns the last action
   * Returns null there are no actions
   */
  public lastAction(): Action {
    return null;
  }

  /**
   * Returns an expression containing up to `n` actions
   */
  public headActions(n: int): Expression {
    return this;
  }

  /**
   * Returns an expression without the last action.
   * Returns null if an action can not be poped
   */
  public popAction(): Expression {
    return null;
  }

  public getLiteralValue(): any {
    return null;
  }

  public bumpStringLiteralToTime(): Expression {
    return this;
  }

  public bumpStringLiteralToSetString(): Expression {
    return this;
  }

  public upgradeToType(targetType: PlyType): Expression {
    return this;
  }

  // ------------------------------------------------------------------------
  // API behaviour

  // Action constructors
  public performAction(action: Action, markSimple?: boolean): ChainExpression {
    return <ChainExpression>this.performActions([action], markSimple);
  }

  public performActions(actions: Action[], markSimple?: boolean): Expression {
    if (!actions.length) return this;
    return new ChainExpression({
      expression: this,
      actions: actions,
      simple: Boolean(markSimple)
    });
  }

  private _performMultiAction(action: string, exs: any[]): ChainExpression {
    if (!exs.length) throw new Error(`${action} action must have at least one argument`);
    let ret: any = this; // A slight type hack but it works because we know that we will go through the loop
    for (let ex of exs) {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      let ActionConstructor = Action.classMap[action] as any;
      ret = ret.performAction(new ActionConstructor({ expression: ex }));
    }
    return ret;
  }

  // Basic arithmetic

  public add(...exs: any[]): ChainExpression {
    return this._performMultiAction('add', exs);
  }

  public subtract(...exs: any[]): ChainExpression {
    return this._performMultiAction('subtract', exs);
  }

  public negate(): ChainExpression {
    return Expression.ZERO.subtract(this);
  }

  public multiply(...exs: any[]): ChainExpression {
    return this._performMultiAction('multiply', exs);
  }

  public divide(...exs: any[]): ChainExpression {
    return this._performMultiAction('divide', exs);
  }

  public reciprocate(): ChainExpression {
    return Expression.ONE.divide(this);
  }

  public sqrt(): ChainExpression {
    return this.power(0.5);
  }

  public power(...exs: any[]): ChainExpression {
    return this._performMultiAction('power', exs);
  }

  public fallback(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new FallbackAction({ expression: ex }));
  }

  // Boolean predicates

  public is(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new IsAction({ expression: ex }));
  }

  public isnt(ex: any): ChainExpression {
    return this.is(ex).not();
  }

  public lessThan(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new LessThanAction({ expression: ex }));
  }

  public lessThanOrEqual(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new LessThanOrEqualAction({ expression: ex }));
  }

  public greaterThan(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new GreaterThanAction({ expression: ex }));
  }

  public greaterThanOrEqual(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new GreaterThanOrEqualAction({ expression: ex }));
  }

  public contains(ex: any, compare?: string): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    if (compare) compare = getString(compare);
    return this.performAction(new ContainsAction({ expression: ex, compare }));
  }

  public match(re: string): ChainExpression {
    return this.performAction(new MatchAction({ regexp: getString(re) }));
  }

  public in(start: Date, end: Date): ChainExpression;
  public in(start: number, end: number): ChainExpression;
  public in(start: string, end: string): ChainExpression;
  public in(ex: any): ChainExpression;
  public in(ex: any, snd?: any): ChainExpression {
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
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new InAction({ expression: ex }));
  }

  public overlap(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.bumpStringLiteralToSetString().performAction(new OverlapAction({ expression: ex.bumpStringLiteralToSetString() }));
  }

  public not(): ChainExpression {
    return this.performAction(new NotAction({}));
  }

  public and(...exs: any[]): ChainExpression {
    return this._performMultiAction('and', exs);
  }

  public or(...exs: any[]): ChainExpression {
    return this._performMultiAction('or', exs);
  }

  // String manipulation

  public substr(position: number, length: number): ChainExpression {
    return this.performAction(new SubstrAction({ position: getNumber(position), length: getNumber(length) }));
  }

  public extract(re: string): ChainExpression {
    return this.performAction(new ExtractAction({ regexp: getString(re) }));
  }

  public concat(...exs: any[]): ChainExpression {
    return this._performMultiAction('concat', exs);
  }

  public lookup(lookup: string): ChainExpression {
    return this.performAction(new LookupAction({ lookup: getString(lookup) }));
  }

  public indexOf(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new IndexOfAction({ expression: ex }));
  }

  public transformCase(transformType: CaseType): ChainExpression {
    return this.performAction(new TransformCaseAction({ transformType: getString(transformType) as CaseType }));
  }

  public customTransform(custom: string, outputType?: PlyTypeSingleValue): ChainExpression {
    if (!custom) throw new Error("Must provide an extraction function name for custom transform");
    outputType = outputType !== undefined ? getString(outputType) as PlyTypeSingleValue : null;
    return this.performAction(new CustomTransformAction({ custom: getString(custom), outputType }));
  }

  // Number manipulation

  public numberBucket(size: number, offset = 0): ChainExpression {
    return this.performAction(new NumberBucketAction({ size: getNumber(size), offset: getNumber(offset) }));
  }

  public absolute(): ChainExpression {
    return this.performAction(new AbsoluteAction({}));
  }

  public length(): ChainExpression {
    return this.performAction(new LengthAction({}));
  }

  // Time manipulation

  public timeBucket(duration: any, timezone?: any): ChainExpression {
    if (!Duration.isDuration(duration)) duration = Duration.fromJS(getString(duration));
    if (timezone && !Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(getString(timezone));
    return this.bumpStringLiteralToTime().performAction(new TimeBucketAction({ duration, timezone }));
  }

  public timeFloor(duration: any, timezone?: any): ChainExpression {
    if (!Duration.isDuration(duration)) duration = Duration.fromJS(getString(duration));
    if (timezone && !Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(getString(timezone));
    return this.bumpStringLiteralToTime().performAction(new TimeFloorAction({ duration, timezone }));
  }

  public timeShift(duration: any, step: number, timezone?: any): ChainExpression {
    if (!Duration.isDuration(duration)) duration = Duration.fromJS(getString(duration));
    if (timezone && !Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(getString(timezone));
    return this.bumpStringLiteralToTime().performAction(new TimeShiftAction({ duration, step: getNumber(step), timezone }));
  }

  public timeRange(duration: any, step: number, timezone?: any): ChainExpression {
    if (!Duration.isDuration(duration)) duration = Duration.fromJS(getString(duration));
    if (timezone && !Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(getString(timezone));
    return this.bumpStringLiteralToTime().performAction(new TimeRangeAction({ duration, step: getNumber(step), timezone }));
  }

  public timePart(part: string, timezone?: any): ChainExpression {
    if (timezone && !Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(getString(timezone));
    return this.bumpStringLiteralToTime().performAction(new TimePartAction({ part: getString(part), timezone }));
  }

  public cast(outputType: PlyType): ChainExpression {
    return this.performAction(new CastAction({ outputType: getString(outputType) as PlyTypeSimple }));
  }

  // Set operations

  public cardinality() {
    return this.performAction(new CardinalityAction({}));
  }

  // Split Apply Combine based transformations

  /**
   * Filter the dataset with a boolean expression
   * Only works on expressions that return DATASET
   * @param ex A boolean expression to filter on
   */
  public filter(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new FilterAction({ expression: ex }));
  }

  public split(splits: any, dataName?: string): ChainExpression;
  public split(ex: any, name: string, dataName?: string): ChainExpression;
  public split(splits: any, name?: string, dataName?: string): ChainExpression {
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
      if (!hasOwnProperty(splits, k)) continue;
      let ex = splits[k];
      parsedSplits[k] = Expression.isExpression(ex) ? ex : Expression.fromJSLoose(ex);
    }

    dataName = dataName ? getString(dataName) : getDataName(this);
    if (!dataName) throw new Error("could not guess data name in `split`, please provide one explicitly");
    return this.performAction(new SplitAction({ splits: parsedSplits, dataName: dataName }));
  }

  /**
   * Evaluate some expression on every datum in the dataset. Record the result as `name`
   * @param name The name of where to store the results
   * @param ex The expression to evaluate
   */
  public apply(name: string, ex: any): ChainExpression {
    if (arguments.length < 2) throw new Error('invalid arguments to .apply, did you forget to specify a name?');
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new ApplyAction({ name: getString(name), expression: ex }));
  }

  public sort(ex: any, direction: Direction = 'ascending'): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new SortAction({ expression: ex, direction: (getString(direction) as Direction) }));
  }

  public limit(limit: number): ChainExpression {
    return this.performAction(new LimitAction({ limit: getNumber(limit) }));
  }

  public select(...attributes: string[]): ChainExpression {
    attributes = attributes.map(getString);
    return this.performAction(new SelectAction({ attributes }));
  }


  // Aggregate expressions

  public count(): ChainExpression {
    if (arguments.length) throw new Error('.count() should not have arguments, did you want to .filter().count()?');
    return this.performAction(new CountAction({}));
  }

  public sum(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new SumAction({ expression: ex }));
  }

  public min(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new MinAction({ expression: ex }));
  }

  public max(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new MaxAction({ expression: ex }));
  }

  public average(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new AverageAction({ expression: ex }));
  }

  public countDistinct(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new CountDistinctAction({ expression: ex }));
  }

  public quantile(ex: any, quantile: number): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new QuantileAction({ expression: ex, quantile: getNumber(quantile) }));
  }

  public collect(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new CollectAction({ expression: ex }));
  }

  public custom(custom: string): ChainExpression {
    return this.performAction(new CustomAggregateAction({ custom: getString(custom) }));
  }

  public customAggregate(custom: string): ChainExpression {
    return this.performAction(new CustomAggregateAction({ custom: getString(custom) }));
  }

  // Undocumented (for now)

  public join(ex: any): ChainExpression {
    if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
    return this.performAction(new JoinAction({ expression: ex }));
  }

  /**
   * Rewrites the expression with all the references typed correctly and resolved to the correct parental level
   * @param environment The environment that will be defined
   */
  public defineEnvironment(environment: Environment): Expression {
    if (!environment.timezone) environment = { timezone: Timezone.UTC };

    // Allow strings as well
    if (typeof environment.timezone === 'string') environment = { timezone: Timezone.fromJS(environment.timezone as any) };

    return this.substituteAction(
      (action) => action.needsEnvironment(),
      (preEx, action) => preEx.performAction(action.defineEnvironment(environment))
    );
  }

  /**
   * Rewrites the expression with all the references typed correctly and resolved to the correct parental level
   * @param context The datum within which the check is happening
   */
  public referenceCheck(context: Datum): Expression {
    return this.referenceCheckInTypeContext(getFullTypeFromDatum(context));
  }

  /**
   * Check if the expression is defined in the given type context
   * @param typeContext The FullType context within which to resolve
   */
  public definedInTypeContext(typeContext: DatasetFullType): boolean {
    try {
      let alterations: Alterations = {};
      this._fillRefSubstitutions(typeContext, { index: 0 }, alterations); // This returns the final type
    } catch (e) {
      return false;
    }
    return true;
  }

  /**
   * Rewrites the expression with all the references typed correctly and resolved to the correct parental level
   * @param typeContext The FullType context within which to resolve
   */
  public referenceCheckInTypeContext(typeContext: DatasetFullType): Expression {
    let alterations: Alterations = {};
    this._fillRefSubstitutions(typeContext, { index: 0 }, alterations); // This returns the final type
    if (emptyLookup(alterations)) return this;
    return this.substitute((ex: Expression, index: int): Expression => alterations[index] || null);
  }

  /**
   * Checks for references and returns the list of alterations that need to be made to the expression
   * @param typeContext the context inherited from the parent
   * @param indexer the index along the tree to maintain
   * @param alterations the accumulation of the alterations to be made (output)
   * @returns the resolved type of the expression
   */
  public _fillRefSubstitutions(typeContext: DatasetFullType, indexer: Indexer, alterations: Alterations): FullType {
    indexer.index++;
    return typeContext;
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
      if (!hasOwnProperty(context, k)) continue;
      let value = context[k];
      if (External.isExternal(value)) {
        expressions[k] = new ExternalExpression({ external: <External>value });
      } else if (Expression.isExpression(value)) {
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

  /**
   * Decompose instances of $data.average($x) into $data.sum($x) / $data.count()
   * @param countEx and optional expression to use in a sum instead of a count
   */
  public decomposeAverage(countEx?: Expression): Expression {
    return this.substituteAction(
      (action) => {
        return action.action === 'average';
      },
      (preEx: Expression, action: Action) => {
        let expression = action.expression;
        return preEx.sum(expression).divide(countEx ? preEx.sum(countEx) : preEx.count());
      }
    );
  }

  /**
   * Apply the distributive law wherever possible to aggregates
   * Turns $data.sum($x - 2 * $y) into $data.sum($x) - 2 * $data.sum($y)
   */
  public distribute(): Expression {
    return this.substituteAction(
      (action) => {
        return action.canDistribute();
      },
      (preEx: Expression, action: Action) => {
        let distributed = action.distribute(preEx);
        if (!distributed) throw new Error('distribute returned null');
        return distributed;
      }
    );
  }

  /**
   * Returns the maximum number of possible values this expression can return in a split context
   */
  public abstract maxPossibleSplitValues(): number

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
   * @param environment The environment for the default of the expression
   */
  public simulate(context: Datum = {}, environment: Environment = {}): PlywoodValue {
    failItIntrospectNeededInDatum(context);

    let readyExpression = this._initialPrepare(context, environment);
    if (readyExpression instanceof ExternalExpression) {
      // Top level externals need to be unsuppressed
      readyExpression = (<ExternalExpression>readyExpression).unsuppress();
    }

    return readyExpression._computeResolvedSimulate([]);
  }


  /**
   * Simulates computing the expression and returns the quereis that would have been made
   * @param context The context within which to compute the expression
   * @param environment The environment for the default of the expression
   */
  public simulateQueryPlan(context: Datum = {}, environment: Environment = {}): any[][] {
    failItIntrospectNeededInDatum(context);

    let readyExpression = this._initialPrepare(context, environment);
    if (readyExpression instanceof ExternalExpression) {
      // Top level externals need to be unsuppressed
      readyExpression = (<ExternalExpression>readyExpression).unsuppress();
    }

    let simulatedQueryGroups: any[] = [];
    readyExpression._computeResolvedSimulate(simulatedQueryGroups);
    return simulatedQueryGroups;
  }

  public _computeResolvedSimulate(simulatedQueryGroups: any[][]): PlywoodValue {
    let ex: Expression = this;
    let readyExternals = ex.getReadyExternals();
    let i = 0;

    while (Object.keys(readyExternals).length > 0 && i < 10) {
      let simulatedQueryGroup: any[] = [];
      fillExpressionExternalAlteration(readyExternals, (external, terminal) => external.simulateValue(terminal, simulatedQueryGroup));

      simulatedQueryGroups.push(simulatedQueryGroup);
      ex = ex.applyReadyExternals(readyExternals);
      readyExternals = ex.getReadyExternals();
      i++;
    }
    return ex.getLiteralValue();
  }


  /**
   * Computes a general asynchronous expression
   * @param context The context within which to compute the expression
   * @param environment The environment for the default of the expression
   */
  public compute(context: Datum = {}, environment: Environment = {}): Q.Promise<PlywoodValue> {
    return Q(null)
      .then(() => {
        return introspectDatum(context);
      })
      .then((introspectedContext: Datum) => {
        let readyExpression = this._initialPrepare(introspectedContext, environment);
        if (readyExpression instanceof ExternalExpression) {
          // Top level externals need to be unsuppressed
          readyExpression = readyExpression.unsuppress();
        }
        return readyExpression._computeResolved();
      });
  }

  public _computeResolved(): Q.Promise<PlywoodValue> {
    let ex: Expression = this;
    let readyExternals = ex.getReadyExternals();
    let i = 0;

    return promiseWhile(
      () => Object.keys(readyExternals).length > 0 && i < 10,
      () => {
        return fillExpressionExternalAlterationAsync(readyExternals, (external, terminal) => external.queryValue(terminal))
          .then((readyExternalsFilled) => {
            ex = ex.applyReadyExternals(readyExternalsFilled);
            readyExternals = ex.getReadyExternals();
            i++;
          });
      }
    )
      .then(() => {
        return ex.getLiteralValue();
      });
  }

}
