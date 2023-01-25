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

import { generalArraysEqual } from 'immutable-class';

import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';

import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';

export class MvContainsExpression extends ChainableExpression {
  static op = 'MvContains';
  static fromJS(parameters: ExpressionJS): MvContainsExpression {
    const value = ChainableExpression.jsToValue(parameters);
    value.mvArray = parameters.mvArray;
    return new MvContainsExpression(value);
  }

  public mvArray: string[];

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('mvContains');
    this._checkOperandTypes('STRING');
    this.mvArray = parameters.mvArray;
    this.type = 'BOOLEAN';
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.mvArray = this.mvArray;
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    js.mvArray = this.mvArray;
    return js;
  }

  public equals(other: MvContainsExpression | undefined): boolean {
    return super.equals(other) && generalArraysEqual(this.mvArray, other.mvArray);
  }

  protected _toStringParameters(_indent?: int): string[] {
    return this.mvArray;
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    const operandArray =
      typeof operandValue === 'string'
        ? [operandValue]
        : Array.isArray(operandValue)
        ? [...operandValue]
        : [];
    return this.mvArray.some(element => operandArray.includes(element));
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return dialect.mvContainsExpression(operandSQL, this.mvArray);
  }
}

Expression.register(MvContainsExpression);
