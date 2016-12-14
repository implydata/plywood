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



import { r, BaseExpressionJS, ExpressionValue, Expression, ChainableUnaryExpression } from './baseExpression';

import { SQLDialect } from '../dialect/baseDialect';
import { PlywoodValue } from '../datatypes/index';

export class SubtractExpression extends ChainableUnaryExpression {
  static op = "Subtract";
  static fromJS(parameters: BaseExpressionJS): SubtractExpression {
    return new SubtractExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("subtract");
    this._checkOperandTypes('NUMBER');
    this._checkExpressionTypes('NUMBER');
    this.type = 'NUMBER';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
      return (operandValue || 0) - (expressionValue || 0);
  }

  protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string {
    return `(${operandJS}-${expressionJS})`;
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    return `(${operandSQL}-${expressionSQL})`;
  }

  protected specialSimplify(): Expression {
    const { operand, expression } = this;
    if (expression.equals(Expression.ZERO)) return operand;
    return this;
  }
}

Expression.register(SubtractExpression);
