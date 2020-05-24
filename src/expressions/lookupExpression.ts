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

import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';

export class LookupExpression extends ChainableExpression {
  static op = 'Lookup';
  static fromJS(parameters: ExpressionJS): LookupExpression {
    let value = ChainableExpression.jsToValue(parameters);
    value.lookupFn = parameters.lookupFn || (parameters as any).lookup;
    return new LookupExpression(value);
  }

  public lookupFn: string;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('lookup');
    this._checkOperandTypes('STRING');
    this.lookupFn = parameters.lookupFn;
    this.type = this.operand.type;
  }

  public valueOf(): ExpressionValue {
    let value = super.valueOf();
    value.lookupFn = this.lookupFn;
    return value;
  }

  public toJS(): ExpressionJS {
    let js = super.toJS();
    js.lookupFn = this.lookupFn;
    return js;
  }

  public equals(other: LookupExpression | undefined): boolean {
    return super.equals(other) && this.lookupFn === other.lookupFn;
  }

  protected _toStringParameters(indent?: int): string[] {
    return [Expression.safeString(this.lookupFn)];
  }

  public fullyDefined(): boolean {
    return false;
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    throw new Error('can not express as JS');
  }

  protected _getJSChainableHelper(operandJS: string): string {
    throw new Error('can not express as JS');
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    throw new Error('can not express as SQL');
  }
}

Expression.register(LookupExpression);
