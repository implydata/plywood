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
  r,
} from './baseExpression';

const IS_OR_OVERLAP: Record<string, boolean> = {
  is: true,
  overlap: true,
};

export class OrExpression extends ChainableUnaryExpression {
  static op = 'Or';
  static fromJS(parameters: ExpressionJS): OrExpression {
    return new OrExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  static merge(ex1: Expression, ex2: Expression): Expression | null {
    if (ex1.equals(ex2)) return ex1;

    if (!IS_OR_OVERLAP[ex1.op] || !IS_OR_OVERLAP[ex2.op]) return null;
    const { operand: lhs1, expression: rhs1 } = ex1 as ChainableUnaryExpression;
    const { operand: lhs2, expression: rhs2 } = ex2 as ChainableUnaryExpression;

    if (!lhs1.equals(lhs2) || !rhs1.isOp('literal') || !rhs2.isOp('literal')) return null;

    let union = Set.unionCover(rhs1.getLiteralValue(), rhs2.getLiteralValue());
    if (union === null) return null;

    return lhs1.overlap(r(union)).simplify();
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('or');
    this._checkOperandTypes('BOOLEAN');
    this._checkExpressionTypes('BOOLEAN');
    this.type = 'BOOLEAN';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return operandValue || expressionValue;
  }

  protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string {
    return `(${operandJS}||${expressionJS})`;
  }

  protected _getSQLChainableUnaryHelper(
    dialect: SQLDialect,
    operandSQL: string,
    expressionSQL: string,
  ): string {
    return `(${operandSQL} OR ${expressionSQL})`;
  }

  public isCommutative(): boolean {
    return true;
  }

  public isAssociative(): boolean {
    return true;
  }

  protected specialSimplify(): Expression {
    const { operand, expression } = this;
    if (expression.equals(Expression.TRUE)) return Expression.TRUE;
    if (expression.equals(Expression.FALSE)) return operand;

    if (operand instanceof OrExpression) {
      let orExpressions = operand.getExpressionList();
      for (let i = 0; i < orExpressions.length; i++) {
        let orExpression = orExpressions[i];
        let mergedExpression = OrExpression.merge(orExpression, expression);
        if (mergedExpression) {
          orExpressions[i] = mergedExpression;
          return Expression.or(orExpressions).simplify();
        }
      }
    } else {
      let mergedExpression = OrExpression.merge(operand, expression);
      if (mergedExpression) return mergedExpression;
    }

    return this;
  }
}

Expression.register(OrExpression);
