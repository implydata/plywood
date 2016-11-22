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
import { PlywoodValue, Dataset } from '../datatypes/index';
import { RefExpression } from './refExpression';

export type Direction = 'ascending' | 'descending';

export class SortExpression extends ChainableUnaryExpression {
  static DESCENDING: Direction = 'descending';
  static ASCENDING: Direction = 'ascending';
  static DEFAULT_DIRECTION: Direction = 'ascending';

  static op = "Sort";
  static fromJS(parameters: ExpressionJS): SortExpression {
    let value = ChainableUnaryExpression.jsToValue(parameters);
    value.direction = parameters.direction;
    return new SortExpression(value);
  }

  public direction: Direction;

  constructor(parameters: ExpressionValue = {}) {
    super(parameters, dummyObject);
    this._ensureOp("sort");
    this._checkOperandTypes('DATASET');

    if (!this.expression.isOp('ref')) {
      throw new Error(`must be a reference expression: ${this.expression}`);
    }

    let direction = parameters.direction || SortExpression.DEFAULT_DIRECTION;
    if (direction !== SortExpression.DESCENDING && direction !== SortExpression.ASCENDING) {
      throw new Error(`direction must be '${SortExpression.DESCENDING}' or '${SortExpression.ASCENDING}'`);
    }
    this.direction = direction;

    this.type = 'DATASET';
  }

  public valueOf(): ExpressionValue {
    let value = super.valueOf();
    value.direction = this.direction;
    return value;
  }

  public toJS(): ExpressionJS {
    let js = super.toJS();
    js.direction = this.direction;
    return js;
  }

  public equals(other: SortExpression): boolean {
    return super.equals(other) &&
      this.direction === other.direction;
  }

  protected _toStringParameters(indent?: int): string[] {
    return [this.expression.toString(indent), this.direction];
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return operandValue ? (operandValue as Dataset).sort(this.expression, this.direction) : null;
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    let dir = this.direction === SortExpression.DESCENDING ? 'DESC' : 'ASC';
    return `ORDER BY ${expressionSQL} ${dir}`;
  }

  public refName(): string {
    let expression = this.expression;
    return (expression instanceof RefExpression) ? expression.name : null;
  }

  public isNester(): boolean {
    return true;
  }

  public fullyDefined(): boolean {
    return this.operand.isOp('literal') && this.expression.resolved();
  }

  public changeDirection(direction: Direction): SortExpression {
    if (this.direction === direction) return this;
    let value = this.valueOf();
    value.direction = direction;
    return new SortExpression(value);
  }

  public toggleDirection(): SortExpression {
    return this.changeDirection(this.direction === SortExpression.ASCENDING ? SortExpression.DESCENDING : SortExpression.ASCENDING);
  }

  public specialSimplify(): Expression {
    const { operand, expression } = this;

    // X.sort(Y, d1).sort(Y, d2)
    if (operand instanceof SortExpression && operand.expression.equals(expression)) return this.changeOperand(operand.operand);

    return this;
  }
}

Expression.register(SortExpression);
