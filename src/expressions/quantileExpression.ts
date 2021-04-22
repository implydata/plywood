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

import { Dataset, PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect';
import {
  ChainableUnaryExpression,
  Expression,
  ExpressionJS,
  ExpressionValue,
} from './baseExpression';
import { Aggregate } from './mixins/aggregate';
import { RefExpression } from './refExpression';

export class QuantileExpression extends ChainableUnaryExpression implements Aggregate {
  static op = 'Quantile';
  static fromJS(parameters: ExpressionJS): QuantileExpression {
    let value = ChainableUnaryExpression.jsToValue(parameters);
    value.value = parameters.value || (parameters as any).quantile;
    value.tuning = parameters.tuning;
    return new QuantileExpression(value);
  }

  public value: number;
  public tuning: string;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('quantile');
    this._checkOperandTypes('DATASET');
    this._checkExpressionTypes('NUMBER');
    this.value = parameters.value;
    this.tuning = parameters.tuning;
    this.type = 'NUMBER';
  }

  public valueOf(): ExpressionValue {
    let value = super.valueOf();
    value.value = this.value;
    value.tuning = this.tuning;
    return value;
  }

  public toJS(): ExpressionJS {
    let js = super.toJS();
    js.value = this.value;
    if (this.tuning) js.tuning = this.tuning;
    return js;
  }

  public equals(other: QuantileExpression | undefined): boolean {
    return super.equals(other) && this.value === other.value && this.tuning === other.tuning;
  }

  protected _toStringParameters(indent?: int): string[] {
    let params = [this.expression.toString(indent), String(this.value)];
    if (this.tuning) params.push(Expression.safeString(this.tuning));
    return params;
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return operandValue ? (operandValue as Dataset).quantile(this.expression, this.value) : null;
  }

  protected _getSQLChainableUnaryHelper(
    dialect: SQLDialect,
    operandSQL: string,
    expressionSQL: string,
  ): string {
    const { expression } = this;
    return dialect.quantileExpression(
      dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL, 'NULL'),
      this.value,
      expression instanceof RefExpression ? expression.name : undefined,
    );
  }
}

Expression.applyMixins(QuantileExpression, [Aggregate]);
Expression.register(QuantileExpression);
