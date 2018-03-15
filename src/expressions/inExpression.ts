/*
 * Copyright 2016-2018 Imply Data, Inc.
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

import { PlywoodValue, Range, Set } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
import { OverlapExpression } from './overlapExpression';

export class InExpression extends ChainableUnaryExpression {
  static op = "In";
  static fromJS(parameters: ExpressionJS): InExpression {
    const value = ChainableUnaryExpression.jsToValue(parameters);

    // Back compat.
    if (Range.isRangeType(value.expression.type)) {
      console.warn('InExpression should no longer be used for ranges use OverlapExpression instead');
      value.op = 'overlap';
      return (new OverlapExpression(value) as any);
    }

    return new InExpression(value);
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("in");

    let operandType = this.operand.type;
    let expression = this.expression;
    if (operandType) {
      if (!(
          operandType === 'NULL' ||
          expression.type === 'NULL' ||
          (!Set.isSetType(operandType) && expression.canHaveType('SET'))
        )) {
        throw new TypeError(`in expression ${this} has a bad type combination ${operandType} IN ${expression.type || '*'}`);
      }
    } else {
      if (!(expression.canHaveType('NUMBER_RANGE') || expression.canHaveType('STRING_RANGE') || expression.canHaveType('TIME_RANGE') || expression.canHaveType('SET'))) {
        throw new TypeError(`in expression has invalid expression type ${expression.type}`);
      }
    }
    this.type = 'BOOLEAN';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    if (!expressionValue) return null;
    return (<any>expressionValue).contains(operandValue);
  }

  protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string {
    throw new Error(`can not convert ${this} to JS function`);
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    throw new Error(`can not convert action to SQL ${this}`);
  }

  public specialSimplify(): Expression {
    const { operand, expression } = this;

    // NotSet.in(Y) => NotSet.is(Y)
    if (operand.type && !Set.isSetType(operand.type)) return operand.is(expression);

    return this;
  }
}

Expression.register(InExpression);
