/*
 * Copyright 2016-2018 Imply Data, Inc.
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
import { Range } from '../datatypes/range';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionValue, r } from './baseExpression';
import { LiteralExpression } from './literalExpression';

export class GreaterThanOrEqualExpression extends ChainableUnaryExpression {
  static op = "GreaterThanOrEqual";
  static fromJS(parameters: ExpressionJS): GreaterThanOrEqualExpression {
    return new GreaterThanOrEqualExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("greaterThanOrEqual");
    this._checkOperandTypes('NUMBER', 'TIME', 'STRING');
    this._checkExpressionTypes('NUMBER', 'TIME', 'STRING');
    this._bumpOperandExpressionToTime();
    this._checkOperandExpressionTypesAlign();
    this.type = 'BOOLEAN';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    if (operandValue === null || expressionValue === null) return null;
    return Set.crossBinaryBoolean(operandValue, expressionValue, (a, b) => a >= b);
  }

  protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string {
    return `(${operandJS}>=${expressionJS})`;
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    return `(${operandSQL}>=${expressionSQL})`;
  }

  protected specialSimplify(): Expression {
    const { operand, expression } = this;

    if (expression instanceof LiteralExpression) { // x >= 7
      return operand.overlap(r(Range.fromJS({ start: expression.value, end: null, bounds: '[)' })));
    }

    if (operand instanceof LiteralExpression) { // 7 >= x
      return expression.overlap(r(Range.fromJS({ start: null, end: operand.value, bounds: '(]' })));
    }

    return this;
  }
}

Expression.register(GreaterThanOrEqualExpression);
