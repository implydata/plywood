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

import { r, ExpressionJS, ExpressionValue, Expression, ChainableUnaryExpression } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
import { SQLDialect } from '../dialect/baseDialect';
import { PlywoodValue, Dataset } from '../datatypes/index';
import { LiteralExpression } from './literalExpression';
import { AddExpression } from './addExpression';
import { SubtractExpression } from './subtractExpression';
import { MultiplyExpression } from './multiplyExpression';

export class SumExpression extends ChainableUnaryExpression implements Aggregate {
  static op = "Sum";
  static fromJS(parameters: ExpressionJS): SumExpression {
    return new SumExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("sum");
    this._checkOperandTypes('DATASET');
    this._checkExpressionTypes('NUMBER');
    this.type = 'NUMBER';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return operandValue ? (operandValue as Dataset).sum(this.expression) : null;
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    return `SUM(${dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL)})`;
  }

  public distribute(): Expression {
    const { operand, expression } = this;

    if (expression instanceof LiteralExpression) {
      let value = expression.value;
      return operand.count().multiply(value).simplify();
    }

    // X.sum(lhs + rhs)
    if (expression instanceof AddExpression) {
      const { operand: lhs, expression: rhs } = expression;
      return operand.sum(lhs).distribute().add(operand.sum(rhs).distribute()).simplify();
    }

    // X.sum(lhs - rhs)
    if (expression instanceof SubtractExpression) {
      const { operand: lhs, expression: rhs } = expression;
      return operand.sum(lhs).distribute().subtract(operand.sum(rhs).distribute()).simplify();
    }

    // X.sum(lhs * rhs)
    if (expression instanceof MultiplyExpression) {
      const { operand: lhs, expression: rhs } = expression;
      if (rhs instanceof LiteralExpression) {
        return operand.sum(lhs).distribute().multiply(rhs).simplify();
      }
    }

    return this;
  }
}

Expression.applyMixins(SumExpression, [Aggregate]);
Expression.register(SumExpression);
