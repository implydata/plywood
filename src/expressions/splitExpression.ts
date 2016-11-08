/*
 * Copyright 2016-2016 Imply Data, Inc.
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

import { r, ExpressionJS, ExpressionValue, Expression, ChainableExpression, Splits, SplitsJS, SubstitutionFn, Indexer, ExpressionTypeContext } from './baseExpression';
import { PlyType, DatasetFullType, SimpleFullType, FullType } from '../types';
import { Aggregate } from './mixins/aggregate';
import { SQLDialect } from '../dialect/baseDialect';
import { Datum, PlywoodValue, Dataset } from '../datatypes/dataset';
import { unwrapSetType } from '../datatypes/common';
import { hasOwnProperty } from '../helper/utils';
import { immutableLookupsEqual } from 'immutable-class';
import { isSetType } from '../datatypes/common';

export class SplitExpression extends ChainableExpression implements Aggregate {
  static op = "Split";
  static fromJS(parameters: ExpressionJS): SplitExpression {
    let value = ChainableExpression.jsToValue(parameters);

    let splits: SplitsJS;
    if (parameters.expression && parameters.name) {
      splits = { [parameters.name]: parameters.expression };
    } else {
      splits = parameters.splits;
    }
    value.splits = Expression.expressionLookupFromJS(splits);

    value.dataName = parameters.dataName;
    return new SplitExpression(value);
  }

  public keys: string[];
  public splits: Splits;
  public dataName: string;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("split");
    this._checkOperandTypes('DATASET');

    let splits = parameters.splits;
    if (!splits) throw new Error('must have splits');
    this.splits = splits;
    this.keys = Object.keys(splits).sort();
    if (!this.keys.length) throw new Error('must have at least one split');
    this.dataName = parameters.dataName;

    this.type = 'DATASET';
  }

  public valueOf(): ExpressionValue {
    let value = super.valueOf();
    value.splits = this.splits;
    value.dataName = this.dataName;
    return value;
  }

  public toJS(): ExpressionJS {
    let { splits } = this;

    let js = super.toJS();
    if (this.isMultiSplit()) {
      js.splits = Expression.expressionLookupToJS(splits);
    } else {
      for (let name in splits) {
        js.name = name;
        js.expression = splits[name].toJS();
      }
    }
    js.dataName = this.dataName;
    return js;
  }

  public equals(other: SplitExpression): boolean {
    return super.equals(other) &&
      immutableLookupsEqual(this.splits, other.splits) &&
      this.dataName === other.dataName;
  }

  public changeSplits(splits: Splits): SplitExpression {
    if (immutableLookupsEqual(this.splits, splits)) return this;
    let value = this.valueOf();
    value.splits = splits;
    return new SplitExpression(value);
  }

  public numSplits(): number {
    return this.keys.length;
  }

  public isMultiSplit(): boolean {
    return this.numSplits() > 1;
  }

  protected _toStringParameters(indent?: int): string[] {
    if (this.isMultiSplit()) {
      let { splits } = this;
      let splitStrings: string[] = [];
      for (let name in splits) {
        splitStrings.push(`${name}: ${splits[name]}`);
      }
      return [splitStrings.join(', '), this.dataName];
    } else {
      return [this.firstSplitExpression().toString(), this.firstSplitName(), this.dataName];
    }
  }

  public updateTypeContext(typeContext: DatasetFullType): DatasetFullType {
    let newDatasetType: Lookup<FullType> = {};
    this.mapSplits((name, expression) => {
      newDatasetType[name] = {
        type: unwrapSetType(expression.type)
      } as any;
    });
    newDatasetType[this.dataName] = typeContext;

    return {
      parent: typeContext.parent,
      type: 'DATASET',
      datasetType: newDatasetType
    };
  }

  public firstSplitName(): string {
    return this.keys[0];
  }

  public firstSplitExpression(): Expression {
    return this.splits[this.firstSplitName()];
  }

  public getArgumentExpressions(): Expression[] {
    return this.mapSplits((name, ex) => ex);
  }

  public mapSplits<T>(fn: (name: string, expression?: Expression) => T): T[] {
    let { splits, keys } = this;
    let res: T[] = [];
    for (let k of keys) {
      let v = fn(k, splits[k]);
      if (typeof v !== 'undefined') res.push(v);
    }
    return res;
  }

  public mapSplitExpressions<T>(fn: (expression: Expression, name?: string) => T): Lookup<T> {
    let { splits, keys } = this;
    let ret: Lookup<T> = Object.create(null);
    for (let key of keys) {
      ret[key] = fn(splits[key], key);
    }
    return ret;
  }

  public calc(datum: Datum): PlywoodValue {
    let { operand, dataName } = this;
    let splitFns = this.mapSplitExpressions((ex) => ex.getFn());
    const operandValue = operand.calc(datum);
    return operandValue ? (operandValue as Dataset).split(splitFns, dataName) : null;
  }

  public getSQL(dialect: SQLDialect): string {
    let groupBys = this.mapSplits((name, expression) => expression.getSQL(dialect));
    return `GROUP BY ${groupBys.join(', ')}`;
  }

  public getSelectSQL(dialect: SQLDialect): string[] {
    return this.mapSplits((name, expression) => `${expression.getSQL(dialect)} AS ${dialect.escapeName(name)}`);
  }

  public getShortGroupBySQL(): string {
    return 'GROUP BY ' + Object.keys(this.splits).map((d, i) => i + 1).join(', ');
  }

  public fullyDefined(): boolean {
    return this.operand.isOp('literal') && this.mapSplits((name, expression) => expression.resolved()).every(Boolean);
  }

  public simplify(): Expression {
    if (this.simple) return this;

    let simpleOperand = this.operand.simplify();
    let simpleSplits = this.mapSplitExpressions((ex) => ex.simplify());
    let simpler: Expression = this.changeOperand(simpleOperand).changeSplits(simpleSplits);
    if (simpler.fullyDefined()) return r(this.calc({}));

    if (simpler instanceof ChainableExpression) {
      const pushedInExternal = simpler.pushIntoExternal();
      if (pushedInExternal) return pushedInExternal;
    }

    return simpler.markSimple();
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
    const nestDiffNext = nestDiff + 1;
    const splitsSubs = this.mapSplitExpressions((ex) => {
      return ex._substituteHelper(substitutionFn, indexer, depth, nestDiffNext, operandSubs.typeContext).expression;
    });
    const updatedThis = this.changeOperand(operandSubs.expression).changeSplits(splitsSubs);

    return {
      expression: updatedThis,
      typeContext: updatedThis.updateTypeContextIfNeeded(operandSubs.typeContext)
    };
  }

  public transformExpressions(fn: (expression: Expression, name?: string) => Expression) {
    return this.changeSplits(this.mapSplitExpressions(fn));
  }

  public filterFromDatum(datum: Datum): Expression {
    return Expression.and(this.mapSplits((name, expression) => {
      if (isSetType(expression.type)) {
        return r(datum[name]).in(expression);
      } else {
        return expression.is(r(datum[name]));
      }
    })).simplify();
  }

  public hasKey(key: string): boolean {
    return hasOwnProperty(this.splits, key);
  }

  public isLinear(): boolean {
    let { splits, keys } = this;
    for (let k of keys) {
      let split = splits[k];
      if (isSetType(split.type)) return false;
    }
    return true;
  }

  public maxBucketNumber(): number {
    let { splits, keys } = this;
    let num = 1;
    for (let key of keys) {
      num *= splits[key].maxPossibleSplitValues();
    }
    return num;
  }
}

Expression.applyMixins(SplitExpression, [Aggregate]);
Expression.register(SplitExpression);
