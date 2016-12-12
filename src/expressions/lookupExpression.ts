/*
 * Copyright 2016-2016 Imply Data, Inc.
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

import { r, ExpressionJS, ExpressionValue, Expression, ChainableExpression } from './baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { PlywoodValue } from '../datatypes/index';


export interface LookupExpressionJS extends ExpressionJS {
  lookupFn?: string;
}

export interface LookupExpressionValue extends ExpressionValue {
  lookupFn?: string;
}

export class LookupExpression extends ChainableExpression {
  static op = "Lookup";
  static fromJS(parameters: LookupExpressionJS): LookupExpression {
    let value = ChainableExpression.jsToValue(parameters) as LookupExpressionValue;
    value.lookupFn = parameters.lookupFn || (parameters as any).lookup;
    return new LookupExpression(value);
  }

  public lookupFn: string;

  constructor(parameters: LookupExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("lookup");
    this._checkOperandTypes('STRING', 'SET/STRING');
    this.lookupFn = parameters.lookupFn;
    this.type = this.operand.type;
  }

  public valueOf(): LookupExpressionValue {
    let value = super.valueOf() as LookupExpressionValue;
    value.lookupFn = this.lookupFn;
    return value;
  }

  public toJS(): LookupExpressionJS {
    let js = super.toJS() as LookupExpressionJS;
    js.lookupFn = this.lookupFn;
    return js;
  }

  public equals(other: LookupExpression): boolean {
    return super.equals(other) &&
      this.lookupFn === other.lookupFn;
  }

  protected _toStringParameters(indent?: int): string[] {
    return [String(this.lookupFn)];
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
