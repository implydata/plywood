/*
 * Copyright 2016-2019 Imply Data, Inc.
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

import { Dataset, Datum, PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
import { Aggregate } from './mixins/aggregate';

export class CountExpression extends ChainableExpression implements Aggregate {
  static op = "Count";
  static fromJS(parameters: ExpressionJS): CountExpression {
    return new CountExpression(ChainableExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("count");
    this._checkOperandTypes('DATASET');
    this.type = 'NUMBER';
  }

  public calc(datum: Datum): PlywoodValue {
    let inV = this.operand.calc(datum);
    return inV ? (inV as Dataset).count() : 0;
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return operandSQL.indexOf(' WHERE ') === -1 ? `COUNT(*)` : `SUM(${dialect.aggregateFilterIfNeeded(operandSQL, '1', '0')})`;
  }
}

Expression.applyMixins(CountExpression, [Aggregate]);
Expression.register(CountExpression);
