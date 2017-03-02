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

import { r, ExpressionJS, ExpressionValue, Expression, ChainableUnaryExpression } from './baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { PlywoodValue } from '../datatypes/index';

export class FallbackExpression extends ChainableUnaryExpression {
  static op = "Fallback";
  static fromJS(parameters: ExpressionJS): FallbackExpression {
    return new FallbackExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue = {}) {
    super(parameters, dummyObject);
    this._ensureOp("fallback");
    this._checkOperandExpressionTypesAlign();
    this.type = this.operand.type || this.expression.type;
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return (operandValue !== null && !isNaN(operandValue)) ? operandValue : expressionValue;
  }

  protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string {
    return `(_ = ${operandJS}, ((_ === null || isNaN(_)) ? ${expressionJS} : _))`;
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    return `COALESCE(${operandSQL}, ${expressionSQL})`;
  }

  public specialSimplify(): Expression {
    const { operand, expression } = this;

    // X.fallback(null)
    if (expression.equals(Expression.NULL)) return operand;

    // null.fallback(X)
    if (operand.equals(Expression.NULL)) return expression;

    // X.fallback(X)
    if (operand.equals(expression)) return operand;

    // r(x).fallback(X) where x != null
    if (operand.getLiteralValue() != null) return operand;

    return this;
  }

}

Expression.register(FallbackExpression);
