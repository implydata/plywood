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

import { SQLDialect } from '../dialect';

import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';

export class IpStringifyExpression extends ChainableExpression {
  static op = 'IpStringify';
  static fromJS(parameters: ExpressionJS): IpStringifyExpression {
    const value = ChainableExpression.jsToValue(parameters);
    return new IpStringifyExpression(value);
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('ipStringify');
    this._checkOperandTypes('STRING');
    this.type = 'STRING';
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    return js;
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return dialect.ipStringifyExpression(operandSQL);
  }
}

Expression.register(IpStringifyExpression);
