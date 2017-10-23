/*
 * Copyright 2016-2017 Imply Data, Inc.
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
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';

export class PowerExpression extends ChainableUnaryExpression {
  static op = "Power";
  static fromJS(parameters: ExpressionJS): PowerExpression {
    return new PowerExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("power");
    this._checkOperandTypes('NUMBER');
    this._checkExpressionTypes('NUMBER');
    this.type = Set.isSetType(this.operand.type) ? this.operand.type : this.expression.type;
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    if (operandValue == null || expressionValue == null) return null;
    return Set.crossBinary(operandValue, expressionValue, (a, b) => {
      const pow = Math.pow(a, b);
      return isNaN(pow) ? null : pow;
    });
  }

  protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string {
    return `Math.pow(${operandJS},${expressionJS})`;
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    return `POWER(${operandSQL},${expressionSQL})`;
  }

  protected specialSimplify(): Expression {
    const { operand, expression } = this;
    if (expression.equals(Expression.ZERO)) return Expression.ONE;
    if (expression.equals(Expression.ONE)) return operand;
    return this;
  }

}

Expression.register(PowerExpression);
