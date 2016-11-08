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

import { r, ExpressionJS, ExpressionValue, Expression, ChainableExpression } from './baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { PlywoodValue, Set } from '../datatypes/index';

export class CardinalityExpression extends ChainableExpression {
  static op = "Cardinality";
  static fromJS(parameters: ExpressionJS): CardinalityExpression {
    return new CardinalityExpression(ChainableExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("cardinality");
    this._checkOperandTypes('SET/STRING', 'SET/STRING_RANGE', 'SET/NUMBER', 'SET/NUMBER_RANGE', 'SET/TIME', 'SET/TIME_RANGE');
    this.type = 'NUMBER';
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    // if (Array.isArray(inV)) return inV.length; // this is to allow passing an array into .compute()
    return operandValue ? (operandValue as Set).cardinality() : operandValue;
  }

  protected _getJSChainableHelper(operandJS: string): string {
    return Expression.jsNullSafetyUnary(operandJS, (input: string) => `${input}.length`);
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return `cardinality(${operandSQL})`;
  }
}

Expression.register(CardinalityExpression);
