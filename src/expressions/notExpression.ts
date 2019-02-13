/*
 * Copyright 2016-2019 Imply Data, Inc.
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


import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';

export class NotExpression extends ChainableExpression {
  static op = "Not";
  static fromJS(parameters: ExpressionJS): NotExpression {
    return new NotExpression(ChainableExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("not");
    this._checkOperandTypes('BOOLEAN');
    this.type = 'BOOLEAN';
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    return !operandValue;
  }

  protected _getJSChainableHelper(operandJS: string): string {
    return `!(${operandJS})`;
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return `NOT(${operandSQL})`;
  }

  protected specialSimplify(): Expression {
    const { operand } = this;
    if (operand instanceof NotExpression) return operand.operand;
    return this;
  }
}

Expression.register(NotExpression);
