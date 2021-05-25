/*
 * Copyright 2020 Imply Data, Inc.
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
import {
  ChainableUnaryExpression,
  Expression,
  ExpressionJS,
  ExpressionValue,
} from './baseExpression';

export class LogExpression extends ChainableUnaryExpression {
  static op = 'Log';
  static fromJS(parameters: ExpressionJS): LogExpression {
    return new LogExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('log');
    this._checkOperandTypes('NUMBER');
    this._checkExpressionTypes('NUMBER');
    this.type = Set.isSetType(this.operand.type) ? this.operand.type : this.expression.type;
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    if (operandValue == null || expressionValue == null) return null;
    return Set.crossBinary(operandValue, expressionValue, (a, b) => {
      const log = Math.log(a) / Math.log(b);
      return isNaN(log) ? null : log;
    });
  }

  protected _getSQLChainableUnaryHelper(
    dialect: SQLDialect,
    operandSQL: string,
    expressionSQL: string,
  ): string {
    return dialect.logExpression(expressionSQL, operandSQL);
  }

  protected specialSimplify(): Expression {
    const { operand } = this;
    if (operand.equals(Expression.ONE)) return Expression.ZERO;
    return this;
  }
}

Expression.register(LogExpression);
