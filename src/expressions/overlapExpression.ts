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
import { PlywoodValue, Set, Range } from '../datatypes/index';

export class OverlapExpression extends ChainableUnaryExpression {
  static op = "Overlap";
  static fromJS(parameters: ExpressionJS): OverlapExpression {
    return new OverlapExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("overlap");
    if (!this.expression.canHaveType('SET')) {
      throw new Error(`${this.op} must have an expression of type SET (is: ${this.expression.type})`);
    }

    let oType = this.operand.type;
    let eType = this.expression.type;
    if (oType && eType && oType !== 'NULL' && oType !== 'SET/NULL' && eType !== 'NULL' && eType !== 'SET/NULL') {
      if (Set.wrapSetType(oType) !== Set.wrapSetType(eType)) {
        throw new Error(`overlap expression has type mismatch between ${oType} and ${eType}`);
      }
    }

    this.type = 'BOOLEAN';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    if (expressionValue == null) return null;
    return operandValue instanceof Set ? operandValue.overlap(expressionValue) : expressionValue.contains(operandValue);
  }

  public isCommutative(): boolean {
    return true;
  }

  protected specialSimplify(): Expression {
    const { operand, expression } = this;

    // X.overlap({})
    if (expression.equals(Expression.EMPTY_SET)) return Expression.FALSE;

    // NonRangeOrSet.overlap(NonRangeOrSet)
    if (!Range.isRangeType(operand.type) && !Range.isRangeType(expression.type)) return operand.is(expression);

    return this;
  }
}

Expression.register(OverlapExpression);
