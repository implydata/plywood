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
import {
  ChainableUnaryExpression,
  Expression,
  ExpressionJS,
  ExpressionValue,
} from './baseExpression';

export class AddExpression extends ChainableUnaryExpression {
  static op = 'Add';
  static fromJS(parameters: ExpressionJS): AddExpression {
    return new AddExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('add');
    this._checkOperandTypes('NUMBER');
    this._checkExpressionTypes('NUMBER');
    this.type = 'NUMBER';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    if (operandValue === null || expressionValue === null) return null;
    return Set.crossBinary(operandValue, expressionValue, (a, b) => a + b);
  }

  protected _getSQLChainableUnaryHelper(
    dialect: SQLDialect,
    operandSQL: string,
    expressionSQL: string,
  ): string {
    return `(${operandSQL}+${expressionSQL})`;
  }

  public isCommutative(): boolean {
    return true;
  }

  public isAssociative(): boolean {
    return true;
  }

  protected specialSimplify(): Expression {
    const { operand, expression } = this;
    if (expression.equals(Expression.ZERO)) return operand;
    return this;
  }
}

Expression.register(AddExpression);
