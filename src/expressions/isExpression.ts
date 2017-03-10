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
import { LiteralExpression } from './literalExpression';
import { PlywoodValue, TimeRange, NumberRange, Set } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { IndexOfExpression } from './indexOfExpression';
import { TimeBucketExpression } from './timeBucketExpression';
import { NumberBucketExpression } from './numberBucketExpression';
import { FallbackExpression } from './fallbackExpression';

export class IsExpression extends ChainableUnaryExpression {
  static op = "Is";
  static fromJS(parameters: ExpressionJS): IsExpression {
    return new IsExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("is");
    this._checkOperandExpressionTypesAlign();
    this.type = 'BOOLEAN';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return Set.crossBinaryBoolean(operandValue, expressionValue, (a, b) => a === b || Boolean(a && a.equals && a.equals(b)));
  }

  protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string {
    const { expression } = this;
    if (expression instanceof LiteralExpression) {
      if (Set.isSetType(expression.type)) {
        let valueSet: Set = expression.value;
        return `${JSON.stringify(valueSet.elements)}.indexOf(${operandJS})>-1`;
      }
    }
    return `(${operandJS}===${expressionJS})`;
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    const expressionSet = this.expression.getLiteralValue();
    if (expressionSet instanceof Set) {
      return expressionSet.elements.map((e) => dialect.isNotDistinctFromExpression(operandSQL, r(e).getSQL(dialect))).join(' OR ');
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
      // X.is(Set({Y})) => X.is(Y)
      if (Set.isSet(literalValue) && literalValue.elements.length === 1) {
        return operand.is(r(literalValue.elements[0]));
      }

      // X.indexOf(Y) = -1
      if (operand instanceof IndexOfExpression && literalValue === -1) {
        const { operand: x, expression: y } = operand;
        return x.contains(y).not();
      }

      // X.timeBucket(duration, timezone) = TimeRange()
      if (operand instanceof TimeBucketExpression && literalValue instanceof TimeRange && operand.timezone) {
        const { operand: x, duration, timezone } = operand;
        if (literalValue.start !== null && TimeRange.timeBucket(literalValue.start, duration, timezone).equals(literalValue)) {
          return x.in(expression);
        } else {
          return Expression.FALSE;
        }
      }

      // X.numberBucket(size, offset) = NumberRange()
      if (operand instanceof NumberBucketExpression && literalValue instanceof NumberRange) {
        const { operand: x, size, offset } = operand;
        if (literalValue.start !== null && NumberRange.numberBucket(literalValue.start, size, offset).equals(literalValue)) {
          return x.in(expression);
        } else {
          return Expression.FALSE;
        }
      }

      // X.fallback(Y) = Z where Y literal, Y != Z
      if (operand instanceof FallbackExpression) {
        const { operand: x, expression: y } = operand;
        if (y.isOp('literal') && !y.equals(expression)) {
          return this.changeOperand(x);
        }
      }

    }

    return this;
  }
}

Expression.register(IsExpression);
