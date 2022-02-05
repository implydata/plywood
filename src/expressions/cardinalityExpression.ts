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

import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';

export class CardinalityExpression extends ChainableExpression {
  static op = 'Cardinality';
  static fromJS(parameters: ExpressionJS): CardinalityExpression {
    return new CardinalityExpression(ChainableExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('cardinality');
    this._checkOperandTypes(
      'BOOLEAN',
      'STRING',
      'STRING_RANGE',
      'NUMBER',
      'NUMBER_RANGE',
      'TIME',
      'TIME_RANGE',
    );
    this.type = 'NUMBER';
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    if (operandValue == null) return null;
    return operandValue instanceof Set ? operandValue.cardinality() : 1;
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return `cardinality(${operandSQL})`;
  }
}

Expression.register(CardinalityExpression);
