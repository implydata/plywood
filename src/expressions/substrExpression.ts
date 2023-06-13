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

import { PlywoodValue, Set } from '../datatypes';
import { SQLDialect } from '../dialect/baseDialect';

import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';

export class SubstrExpression extends ChainableExpression {
  static op = 'Substr';
  static fromJS(parameters: ExpressionJS): SubstrExpression {
    const value = ChainableExpression.jsToValue(parameters);
    value.position = parameters.position;
    value.len = parameters.len || (parameters as any).length;
    return new SubstrExpression(value);
  }

  public position: int;
  public len: int;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this.position = parameters.position;
    this.len = parameters.len;
    this._ensureOp('substr');
    this._checkOperandTypes('STRING');
    this.type = this.operand.type;
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.position = this.position;
    value.len = this.len;
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    js.position = this.position;
    js.len = this.len;
    return js;
  }

  public equals(other: SubstrExpression | undefined): boolean {
    return super.equals(other) && this.position === other.position && this.len === other.len;
  }

  protected _toStringParameters(_indent?: int): string[] {
    return [String(this.position), String(this.len)];
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    if (operandValue === null) return null;
    const { position, len } = this;
    return Set.crossUnary(operandValue, a => a.substr(position, len));
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return dialect.substrExpression(operandSQL, this.position, this.len);
  }

  public specialSimplify(): Expression {
    const { len } = this;

    // X.substr(_, 0)
    if (len === 0) return Expression.EMPTY_STRING;

    return this;
  }
}

Expression.register(SubstrExpression);
