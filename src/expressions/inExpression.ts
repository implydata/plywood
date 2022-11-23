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

import { NumberRange, PlywoodValue, Range, Set, StringRange, TimeRange } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';

import {
  ChainableUnaryExpression,
  Expression,
  ExpressionJS,
  ExpressionValue,
} from './baseExpression';
import { LiteralExpression } from './literalExpression';
import { OverlapExpression } from './overlapExpression';

export class InExpression extends ChainableUnaryExpression {
  static op = 'In';
  static fromJS(parameters: ExpressionJS): InExpression {
    const value = ChainableUnaryExpression.jsToValue(parameters);

    // Back compat.
    if (Range.isRangeType(value.expression.type)) {
      console.warn(
        'InExpression should no longer be used for ranges use OverlapExpression instead',
      );
      value.op = 'overlap';
      return new OverlapExpression(value) as any;
    }

    return new InExpression(value);
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('in');

    const operandType = this.operand.type;
    const expression = this.expression;
    if (operandType) {
      if (
        !(
          operandType === 'NULL' ||
          expression.type === 'NULL' ||
          (!Set.isSetType(operandType) && expression.canHaveType('SET'))
        )
      ) {
        throw new TypeError(
          `in expression ${this} has a bad type combination ${operandType} IN ${
            expression.type || '*'
          }`,
        );
      }
    } else {
      if (
        !(
          expression.canHaveType('NUMBER_RANGE') ||
          expression.canHaveType('STRING_RANGE') ||
          expression.canHaveType('TIME_RANGE') ||
          expression.canHaveType('SET')
        )
      ) {
        throw new TypeError(`in expression has invalid expression type ${expression.type}`);
      }
    }
    this.type = 'BOOLEAN';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    if (!expressionValue) return null;
    return expressionValue.contains(operandValue);
  }

  protected _getSQLChainableUnaryHelper(
    dialect: SQLDialect,
    operandSQL: string,
    _expressionSQL: string,
  ): string {
    const expression = this.expression;
    const expressionType = expression.type;
    switch (expressionType) {
      case 'NUMBER_RANGE':
      case 'TIME_RANGE':
        if (expression instanceof LiteralExpression) {
          const range: NumberRange | TimeRange = expression.value;
          return dialect.inExpression(
            operandSQL,
            dialect.numberOrTimeToSQL(range.start),
            dialect.numberOrTimeToSQL(range.end),
            range.bounds,
          );
        }
        throw new Error(`can not convert action to SQL ${this}`);

      case 'STRING_RANGE':
        if (expression instanceof LiteralExpression) {
          const stringRange: StringRange = expression.value;
          return dialect.inExpression(
            operandSQL,
            dialect.escapeLiteral(stringRange.start),
            dialect.escapeLiteral(stringRange.end),
            stringRange.bounds,
          );
        }
        throw new Error(`can not convert action to SQL ${this}`);

      case 'SET/NUMBER_RANGE':
      case 'SET/TIME_RANGE':
        if (expression instanceof LiteralExpression) {
          const setOfRange: Set = expression.value;
          return (
            '(' +
            setOfRange.elements
              .map((range: NumberRange | TimeRange) => {
                return dialect.inExpression(
                  operandSQL,
                  dialect.numberOrTimeToSQL(range.start),
                  dialect.numberOrTimeToSQL(range.end),
                  range.bounds,
                );
              })
              .join(' OR ') +
            ')'
          );
        }
        throw new Error(`can not convert action to SQL ${this}`);
      case 'SET/STRING':
        if (expression instanceof LiteralExpression) {
          const setOfRange: Set = expression.value;
          return (
            '(' +
            setOfRange.elements
              .map((element: string) => {
                return dialect.inExpression(
                  operandSQL,
                  dialect.escapeLiteral(element),
                  dialect.escapeLiteral(element),
                  '[]',
                );
              })
              .join(' OR ') +
            ')'
          );
        }
        throw new Error(`can not convert action to SQL ${this}`);
      default:
        throw new Error(`can not convert action to SQL ${this}`);
    }
  }

  public specialSimplify(): Expression {
    const { operand, expression } = this;

    // NotSet.in(Y) => NotSet.is(Y)
    if (operand.type && !Set.isSetType(operand.type)) return operand.is(expression);

    return this;
  }
}

Expression.register(InExpression);
