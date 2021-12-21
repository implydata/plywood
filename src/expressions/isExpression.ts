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

import { NumberRange, PlywoodValue, Set, TimeRange } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import {
  ChainableUnaryExpression,
  Expression,
  ExpressionJS,
  ExpressionValue,
  r,
} from './baseExpression';
import { FallbackExpression } from './fallbackExpression';
import { IndexOfExpression } from './indexOfExpression';
import { LiteralExpression } from './literalExpression';
import { NumberBucketExpression } from './numberBucketExpression';
import { ThenExpression } from './thenExpression';
import { TimeBucketExpression } from './timeBucketExpression';

export class IsExpression extends ChainableUnaryExpression {
  static op = 'Is';
  static fromJS(parameters: ExpressionJS): IsExpression {
    return new IsExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('is');
    this._checkOperandExpressionTypesAlign();
    this.type = 'BOOLEAN';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return Set.crossBinaryBoolean(
      operandValue,
      expressionValue,
      (a, b) => a === b || Boolean(a && a.equals && a.equals(b)),
    );
  }

  protected _getSQLChainableUnaryHelper(
    dialect: SQLDialect,
    operandSQL: string,
    expressionSQL: string,
  ): string {
    let expressionSet = this.expression.getLiteralValue();
    if (expressionSet instanceof Set) {
      if (expressionSet.empty()) return 'FALSE';

      switch (this.expression.type) {
        case 'SET/STRING':
        case 'SET/NUMBER':
          let nullCheck: string = null;
          if (expressionSet.has(null)) {
            nullCheck = `(${operandSQL} IS NULL)`;
            expressionSet = expressionSet.remove(null);
          }

          let inCheck = `${operandSQL} IN (${expressionSet.elements
            .map((v: any) => (typeof v === 'number' ? v : dialect.escapeLiteral(v)))
            .join(',')})`;
          return nullCheck ? `(${nullCheck} OR ${inCheck})` : inCheck;

        default:
          return (
            '(' +
            expressionSet.elements
              .map(e => dialect.isNotDistinctFromExpression(operandSQL, r(e).getSQL(dialect)))
              .join(' OR ') +
            ')'
          );
      }
    } else {
      return dialect.isNotDistinctFromExpression(operandSQL, expressionSQL);
    }
  }

  public isCommutative(): boolean {
    return true;
  }

  protected specialSimplify(): Expression {
    const { operand, expression } = this;

    // X = X
    if (operand.equals(expression)) return Expression.TRUE;

    const literalValue = expression.getLiteralValue();

    if (literalValue != null) {
      if (Set.isSet(literalValue)) {
        const setElements = literalValue.elements;

        // X.is(Set({})) => false
        if (setElements.length === 0) {
          return LiteralExpression.FALSE;
        }

        // X.is(Set({Y})) => X.is(Y)
        if (setElements.length === 1) {
          return operand.is(r(setElements[0]));
        }
      }

      // X.indexOf(Y).is(-1)
      if (operand instanceof IndexOfExpression && literalValue === -1) {
        const { operand: x, expression: y } = operand;
        return x.contains(y).not();
      }

      // X.timeBucket(duration, timezone).is(TimeRange())
      if (
        operand instanceof TimeBucketExpression &&
        literalValue instanceof TimeRange &&
        operand.timezone
      ) {
        const { operand: x, duration, timezone } = operand;
        if (
          literalValue.start !== null &&
          TimeRange.timeBucket(literalValue.start, duration, timezone).equals(literalValue)
        ) {
          return x.overlap(expression);
        } else {
          return Expression.FALSE;
        }
      }

      // X.numberBucket(size, offset).is(NumberRange())
      if (operand instanceof NumberBucketExpression && literalValue instanceof NumberRange) {
        const { operand: x, size, offset } = operand;
        if (
          literalValue.start !== null &&
          NumberRange.numberBucket(literalValue.start, size, offset).equals(literalValue)
        ) {
          return x.overlap(expression);
        } else {
          return Expression.FALSE;
        }
      }

      // X.then(Y).is(Z) where Y literal
      if (operand instanceof ThenExpression) {
        const { operand: x, expression: y } = operand;
        if (y.isOp('literal')) {
          return y.equals(expression) ? x.is(Expression.TRUE) : x.isnt(Expression.TRUE);
        }
      }
    }

    return this;
  }
}

Expression.register(IsExpression);
