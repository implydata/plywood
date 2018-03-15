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

import { Dataset, PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
import { Aggregate } from './mixins/aggregate';

export class AverageExpression extends ChainableUnaryExpression implements Aggregate {
  static op = "Average";
  static fromJS(parameters: ExpressionJS): AverageExpression {
    return new AverageExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("average");
    this._checkOperandTypes('DATASET');
    this._checkExpressionTypes('NUMBER');
    this.type = 'NUMBER';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return operandValue ? (operandValue as Dataset).average(this.expression) : null;
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    return `AVG(${dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL)})`;
  }

  public decomposeAverage(countEx?: Expression): Expression {
    const { operand, expression } = this;
    return operand.sum(expression).divide(countEx ? operand.sum(countEx) : operand.count());
  }
}

Expression.applyMixins(AverageExpression, [Aggregate]);
Expression.register(AverageExpression);
