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

import { Action, ActionJS, ActionValue, Splits, SplitsJS } from './baseAction';
import { PlyType, DatasetFullType, SimpleFullType, FullType } from '../types';
import { Expression, Indexer, Alterations, r, SubstitutionFn, LiteralExpression } from '../expressions/index';
import { SQLDialect } from '../dialect/baseDialect';
import { Datum, ComputeFn } from '../datatypes/dataset';
import { unwrapSetType } from '../datatypes/common';
import { hasOwnProperty } from '../helper/utils';
import { immutableLookupsEqual } from 'immutable-class';
import { isSetType } from '../datatypes/common';

export class SplitAction extends Action {
  static fromJS(parameters: ActionJS): SplitAction {
    let value: ActionValue = {
      action: parameters.action
    };
    let splits: SplitsJS;
    if (parameters.expression && parameters.name) {
      splits = { [parameters.name]: parameters.expression };
    } else {
      splits = parameters.splits;
    }
    value.splits = Expression.expressionLookupFromJS(splits);
    value.dataName = parameters.dataName;
    return new SplitAction(value);
  }

  public keys: string[];
  public splits: Splits;
  public dataName: string;

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    let splits = parameters.splits;
    if (!splits) throw new Error('must have splits');
    this.splits = splits;
    this.keys = Object.keys(splits).sort();
    if (!this.keys.length) throw new Error('must have at least one split');
    this.dataName = parameters.dataName;
    this._ensureAction("split");
  }

  public valueOf(): ActionValue {
    let value = super.valueOf();
    value.splits = this.splits;
    value.dataName = this.dataName;
    return value;
  }

  public toJS(): ActionJS {
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

  public equals(other: SplitAction): boolean {
    return super.equals(other) &&
      immutableLookupsEqual(this.splits, other.splits) &&
      this.dataName === other.dataName;
  }

  protected _toStringParameters(expressionString: string): string[] {
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

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return 'DATASET';
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'DATASET';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    let newDatasetType: Lookup<FullType> = {};
    this.mapSplits((name, expression) => {
      let fullType = expression._fillRefSubstitutions(typeContext, indexer, alterations) as SimpleFullType;
      newDatasetType[name] = {
        type: unwrapSetType(fullType.type)
      } as any;
    });
    newDatasetType[this.dataName] = typeContext;

    return {
      parent: typeContext.parent,
      type: 'DATASET',
      datasetType: newDatasetType,
      remote: false
    };
  }

  public getFn(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
    let { dataName } = this;
    let splitFns = this.mapSplitExpressions((ex) => ex.getFn());
    return (d: Datum) => {
      let inV = inputFn(d);
      return inV ? inV.split(splitFns, dataName) : null;
    };
  }

  public getSQL(inputType: PlyType, inputSQL: string, dialect: SQLDialect): string {
    let groupBys = this.mapSplits((name, expression) => expression.getSQL(dialect));
    return `GROUP BY ${groupBys.join(', ')}`;
  }

  public getSelectSQL(dialect: SQLDialect): string[] {
    return this.mapSplits((name, expression) => `${expression.getSQL(dialect)} AS ${dialect.escapeName(name)}`);
  }

  public getShortGroupBySQL(): string {
    return 'GROUP BY ' + Object.keys(this.splits).map((d, i) => i + 1).join(', ');
  }

  public expressionCount(): int {
    let count = 0;
    this.mapSplits((k, expression) => {
      count += expression.expressionCount();
    });
    return count;
  }

  public fullyDefined(): boolean {
    return false; // Do not try to simplify for now
  }

  public simplify(): Action {
    if (this.simple) return this;

    let simpleSplits = this.mapSplitExpressions((ex) => ex.simplify());

    let value = this.valueOf();
    value.splits = simpleSplits;
    value.simple = true;
    return new SplitAction(value);
  }

  public getExpressions(): Expression[] {
    return this.mapSplits((name, ex) => ex);
  }

  public _substituteHelper(substitutionFn: SubstitutionFn, thisArg: any, indexer: Indexer, depth: int, nestDiff: int): Action {
    let nestDiffNext = nestDiff + 1;
    let hasChanged = false;
    let subSplits = this.mapSplitExpressions((ex) => {
      let subExpression = ex._substituteHelper(substitutionFn, thisArg, indexer, depth, nestDiffNext);
      if (subExpression !== ex) hasChanged = true;
      return subExpression;
    });
    if (!hasChanged) return this;
    let value = this.valueOf();
    value.splits = subSplits;
    return new SplitAction(value);
  }

  public isNester(): boolean {
    return true;
  }

  public numSplits(): number {
    return this.keys.length;
  }

  public isMultiSplit(): boolean {
    return this.numSplits() > 1;
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

  public transformExpressions(fn: (expression: Expression, name?: string) => Expression): SplitAction {
    let { splits, keys } = this;
    let newSplits: Lookup<Expression> = Object.create(null);
    let changed = false;
    for (let key of keys) {
      let ex = splits[key];
      let transformed = fn(ex, key);
      if (transformed !== ex) changed = true;
      newSplits[key] = transformed;
    }
    if (!changed) return this;
    let value = this.valueOf();
    value.splits = newSplits;
    return new SplitAction(value);
  }

  public firstSplitName(): string {
    return this.keys[0];
  }

  public firstSplitExpression(): Expression {
    return this.splits[this.firstSplitName()];
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

  public isAggregate() {
    return true;
  }

  protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
    //if (!expression.resolved()) return null;
    if (literalExpression.value === null) return Expression.NULL;
    let dataset = literalExpression.value;

    let splitFns = this.mapSplitExpressions((ex) => ex.getFn());
    dataset = dataset.split(splitFns, this.dataName, null);

    return r(dataset);
  }
}

Action.register(SplitAction);
