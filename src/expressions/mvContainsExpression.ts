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

import { PlywoodValue, Set } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';

import {
  ChainableUnaryExpression,
  Expression,
  ExpressionJS,
  ExpressionValue,
} from './baseExpression';

export class MvContainsExpression extends ChainableUnaryExpression {
  static op = 'MvContains';
  static fromJS(parameters: ExpressionJS): MvContainsExpression {
    const value = ChainableUnaryExpression.jsToValue(parameters);
    return new MvContainsExpression(value);
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._checkOperandTypes('SET/STRING');
    this._checkExpressionTypes('STRING', 'SET/STRING');

    this._ensureOp('mvContains');

    this.type = 'BOOLEAN';
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    return js;
  }

  public equals(other: MvContainsExpression | undefined): boolean {
    return super.equals(other);
  }

  protected _toStringParameters(indent?: int): string[] {
    return [this.expression.toString(indent)];
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return operandValue.contains(expressionValue);
  }

  protected _getSQLChainableUnaryHelper(
    dialect: SQLDialect,
    operandSQL: string,
    expressionSQL: string,
  ): string {
    return dialect.mvContainsExpression(operandSQL, expressionSQL);
  }
}

Expression.register(MvContainsExpression);
