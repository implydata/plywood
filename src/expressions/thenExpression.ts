/*
 * Copyright 2017-2017 Imply Data, Inc.
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
import { PlywoodValue, Set } from '../datatypes/index';

export class ThenExpression extends ChainableUnaryExpression {
  static op = "Then";
  static fromJS(parameters: ExpressionJS): ThenExpression {
    return new ThenExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue = {}) {
    super(parameters, dummyObject);
    this._ensureOp("then");
    this._checkOperandTypes('BOOLEAN');
    this.type = this.expression.type;
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return operandValue ? expressionValue : null;
  }

  protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string {
    return `((_=${operandJS}),(_?${expressionJS}:null))`;
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    return dialect.ifThenElseNullExpression(operandSQL, expressionSQL);
  }

  public specialSimplify(): Expression {
    const { operand, expression } = this;

    // X.then(null)
    if (expression.equals(Expression.NULL)) return operand;

    // null.then(X)
    if (operand.equals(Expression.NULL)) return Expression.NULL;

    // false.then(X)
    if (operand.equals(Expression.FALSE)) return Expression.NULL;

    // true.then(X)
    if (operand.equals(Expression.TRUE)) return expression;

    return this;
  }

}

Expression.register(ThenExpression);
