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


import { Dataset, PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ApplyExpression } from './applyExpression';
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
import { RefExpression } from './refExpression';
import { SortExpression } from './sortExpression';
import { SplitExpression } from './splitExpression';

export class FilterExpression extends ChainableUnaryExpression {
  static op = "Filter";
  static fromJS(parameters: ExpressionJS): FilterExpression {
    let value = ChainableUnaryExpression.jsToValue(parameters);
    return new FilterExpression(value);
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("filter");
    this._checkExpressionTypes('BOOLEAN');
    this.type = 'DATASET';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return operandValue ? (operandValue as Dataset).filter(this.expression) : null;
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    return `${operandSQL} WHERE ${expressionSQL}`;
  }

  public isNester(): boolean {
    return true;
  }

  public fullyDefined(): boolean {
    return this.operand.isOp('literal') && this.expression.resolved();
  }

  public specialSimplify(): Expression {
    const { operand, expression } = this;

    // X.filter(True)
    if (expression.equals(Expression.TRUE)) return operand;

    // X.filter(A).filter(expression)
    if (operand instanceof FilterExpression) {
      const { operand: x, expression: a } = operand;
      return x.filter(a.and(expression));
    }

    // X.apply(...).filter(...)
    if (operand instanceof ApplyExpression) {
      return expression.getFreeReferences().indexOf(operand.name) === -1 ? this.swapWithOperand() : this;
    }

    // X.split(splits, dataName).filter(...)
    if (operand instanceof SplitExpression && operand.isLinear()) {
      const { operand: x, splits, dataName } = operand;
      const newFilter = expression.substitute((ex) => {
        if (ex instanceof RefExpression && splits[ex.name]) return splits[ex.name];
        return null;
      });
      return x.filter(newFilter).split(splits, dataName);
    }

    // X.sort(...).filter(...)
    if (operand instanceof SortExpression) return this.swapWithOperand();

    return this;
  }
}

Expression.register(FilterExpression);
