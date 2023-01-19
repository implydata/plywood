/*
 * Copyright 2016-2020 Imply Data, Inc.
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

import * as hasOwnProp from 'has-own-prop';
import { immutableLookupsEqual } from 'immutable-class';

import { Dataset, Datum, PlywoodValue, Set } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { DatasetFullType, FullType } from '../types';

import {
  ChainableExpression,
  Expression,
  ExpressionJS,
  ExpressionTypeContext,
  ExpressionValue,
  Indexer,
  r,
  Splits,
  SplitsJS,
  SubstitutionFn,
} from './baseExpression';
import { Aggregate } from './mixins/aggregate';
import { SqlRefExpression } from './sqlRefExpression';

export class SplitExpression extends ChainableExpression implements Aggregate {
  static op = 'Split';
  static fromJS(parameters: ExpressionJS): SplitExpression {
    const value = ChainableExpression.jsToValue(parameters);

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
    this._ensureOp('split');
    this._checkOperandTypes('DATASET');

    const splits = parameters.splits;
    if (!splits) throw new Error('must have splits');
    this.splits = splits;
    this.keys = Object.keys(splits).sort();
    if (!this.keys.length) throw new Error('must have at least one split');
    this.dataName = parameters.dataName;

    this.type = 'DATASET';
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.splits = this.splits;
    value.dataName = this.dataName;
    return value;
  }

  public toJS(): ExpressionJS {
    const { splits } = this;

    const js = super.toJS();
    if (this.isMultiSplit()) {
      js.splits = Expression.expressionLookupToJS(splits);
    } else {
      for (const name in splits) {
        js.name = name;
        js.expression = splits[name].toJS();
      }
    }
    js.dataName = this.dataName;
    return js;
  }

  public equals(other: SplitExpression | undefined): boolean {
    return (
      super.equals(other) &&
      immutableLookupsEqual(this.splits, other.splits) &&
      this.dataName === other.dataName
    );
  }

  public changeSplits(splits: Splits): SplitExpression {
    if (immutableLookupsEqual(this.splits, splits)) return this;
    const value = this.valueOf();
    value.splits = splits;
    return new SplitExpression(value);
  }

  public numSplits(): number {
    return this.keys.length;
  }

  public isMultiSplit(): boolean {
    return this.numSplits() > 1;
  }

  protected _toStringParameters(_indent?: int): string[] {
    if (this.isMultiSplit()) {
      const { splits } = this;
      const splitStrings: string[] = [];
      for (const name in splits) {
        splitStrings.push(`${name}: ${splits[name]}`);
      }
      return [splitStrings.join(', '), this.dataName];
    } else {
      return [this.firstSplitExpression().toString(), this.firstSplitName(), this.dataName];
    }
  }

  public updateTypeContext(typeContext: DatasetFullType): DatasetFullType {
    const newDatasetType: Record<string, FullType> = {};
    this.mapSplits((name, expression) => {
      newDatasetType[name] = {
        type: Set.unwrapSetType(expression.type),
      } as any;
    });
    newDatasetType[this.dataName] = typeContext;

    return {
      parent: typeContext.parent,
      type: 'DATASET',
      datasetType: newDatasetType,
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
    const { splits, keys } = this;
    const res: T[] = [];
    for (const k of keys) {
      const v = fn(k, splits[k]);
      if (typeof v !== 'undefined') res.push(v);
    }
    return res;
  }

  public mapSplitExpressions<T>(
    fn: (expression: Expression, name?: string) => T,
  ): Record<string, T> {
    const { splits, keys } = this;
    const ret: Record<string, T> = Object.create(null);
    for (const key of keys) {
      ret[key] = fn(splits[key], key);
    }
    return ret;
  }

  public addSplits(splits: Splits): SplitExpression {
    const newSplits = this.mapSplitExpressions(ex => ex);
    for (const k in splits) {
      newSplits[k] = splits[k];
    }

    return this.changeSplits(newSplits);
  }

  public calc(datum: Datum): PlywoodValue {
    const { operand, splits, dataName } = this;
    const operandValue = operand.calc(datum);
    return operandValue ? (operandValue as Dataset).split(splits, dataName) : null;
  }

  public getSQL(_dialect: SQLDialect): string {
    throw new Error('can not convert split expression to SQL directly');
  }

  public getSelectSQL(dialect: SQLDialect): string[] {
    return this.mapSplits((name, expression) => {
      if (
        expression instanceof SqlRefExpression &&
        ['IP', 'SET/IP'].includes(expression.type) &&
        !(expression.sql.includes('IP_SEARCH') || expression.sql.includes('IP_MATCH'))
      ) {
        return `${dialect.ipStringifyExpression(
          expression.getSQL(dialect),
        )} AS ${dialect.escapeName(name)}`;
      } else {
        return `${expression.getSQL(dialect)} AS ${dialect.escapeName(name)}`;
      }
    });
  }

  public getGroupBySQL(dialect: SQLDialect): string[] {
    return this.mapSplits((name, expression) => expression.getSQL(dialect));
  }

  public getShortGroupBySQL(): string[] {
    return Object.keys(this.splits).map((d, i) => String(i + 1));
  }

  public fullyDefined(): boolean {
    return (
      this.operand.isOp('literal') &&
      this.mapSplits((name, expression) => expression.resolved()).every(Boolean)
    );
  }

  public simplify(): Expression {
    if (this.simple) return this;

    const simpleOperand = this.operand.simplify();
    const simpleSplits = this.mapSplitExpressions(ex => ex.simplify());
    const simpler: Expression = this.changeOperand(simpleOperand).changeSplits(simpleSplits);
    if (simpler.fullyDefined()) return r(simpler.calc({}));

    if (simpler instanceof ChainableExpression) {
      const pushedInExternal = simpler.pushIntoExternal();
      if (pushedInExternal) return pushedInExternal;
    }

    return simpler.markSimple();
  }

  public _substituteHelper(
    substitutionFn: SubstitutionFn,
    indexer: Indexer,
    depth: int,
    nestDiff: int,
    typeContext: DatasetFullType,
  ): ExpressionTypeContext {
    const sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff);
    if (sub) {
      indexer.index += this.expressionCount();
      return {
        expression: sub,
        typeContext: sub.updateTypeContextIfNeeded(typeContext),
      };
    } else {
      indexer.index++;
    }
    depth++;

    const operandSubs = this.operand._substituteHelper(
      substitutionFn,
      indexer,
      depth,
      nestDiff,
      typeContext,
    );
    const nestDiffNext = nestDiff + 1;
    const splitsSubs = this.mapSplitExpressions(ex => {
      return ex._substituteHelper(
        substitutionFn,
        indexer,
        depth,
        nestDiffNext,
        operandSubs.typeContext,
      ).expression;
    });
    const updatedThis = this.changeOperand(operandSubs.expression).changeSplits(splitsSubs);

    return {
      expression: updatedThis,
      typeContext: updatedThis.updateTypeContextIfNeeded(operandSubs.typeContext),
    };
  }

  public transformExpressions(fn: (expression: Expression, name?: string) => Expression) {
    return this.changeSplits(this.mapSplitExpressions(fn));
  }

  public filterFromDatum(datum: Datum): Expression {
    return Expression.and(
      this.mapSplits((name, expression) => {
        if (Set.isSetType(expression.type)) {
          return r(datum[name]).overlap(expression);
        } else {
          return expression.is(r(datum[name]));
        }
      }),
    ).simplify();
  }

  public hasKey(key: string): boolean {
    return hasOwnProp(this.splits, key);
  }

  public isLinear(): boolean {
    const { splits, keys } = this;
    for (const k of keys) {
      const split = splits[k];
      if (Set.isSetType(split.type)) return false;
    }
    return true;
  }

  public maxBucketNumber(): number {
    const { splits, keys } = this;
    let num = 1;
    for (const key of keys) {
      num *= splits[key].maxPossibleSplitValues();
    }
    return num;
  }
}

Expression.applyMixins(SplitExpression, [Aggregate]);
Expression.register(SplitExpression);
