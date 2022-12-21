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

import { Ip } from '../datatypes/ip';
import { SQLDialect } from '../dialect';

import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';

export class IpSearchExpression extends ChainableExpression {
  static op = 'IpSearch';
  static fromJS(parameters: ExpressionJS): IpSearchExpression {
    const value = ChainableExpression.jsToValue(parameters);
    value.ipSearchString = parameters.ipSearchString;
    value.ipSearchType = parameters.ipSearchType;
    return new IpSearchExpression(value);
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('ipSearch');
    this._checkOperandTypes('IP');
    this.ipSearchString = parameters.ipSearchString;
    this.ipSearchType = parameters.ipSearchType;
    this.type = 'BOOLEAN';
  }

  public ipSearchString: Ip;
  public ipSearchType = 'ip';

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.ipSearchString = this.ipSearchString;
    value.ipSearchType = this.ipSearchType;
    return value;
  }

  public equals(other: IpSearchExpression | undefined): boolean {
    return (
      super.equals(other) &&
      this.ipSearchString === other.ipSearchString &&
      this.ipSearchType === other.ipSearchType
    );
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    js.ipSearchString = this.ipSearchString;
    js.ipSearchType = this.ipSearchType;
    return js;
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return dialect.ipSearchExpression(
      operandSQL,
      this.ipSearchString.toString(),
      this.ipSearchType,
    );
  }
}

Expression.register(IpSearchExpression);
