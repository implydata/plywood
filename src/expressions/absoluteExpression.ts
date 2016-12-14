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

import { r, BaseExpressionJS, ExpressionValue, Expression, ChainableExpression } from './baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { PlywoodValue } from '../datatypes/index';

export class AbsoluteExpression extends ChainableExpression {
  static op = "Absolute";
  static fromJS(parameters: BaseExpressionJS): AbsoluteExpression {
    return new AbsoluteExpression(ChainableExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("absolute");
    this.type = 'NUMBER';
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    return operandValue ? Math.abs(operandValue) : null;
  }

  protected _getJSChainableHelper(operandJS: string): string {
    return `Math.abs(${operandJS})`;
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return `ABS(${operandSQL})`;
  }

  public specialSimplify(): Expression {
    const { operand } = this;
    if (operand instanceof AbsoluteExpression) return operand;
    return this;
  }
}

Expression.register(AbsoluteExpression);