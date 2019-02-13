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

import * as hasOwnProp from 'has-own-prop';
import { PlywoodValue } from '../datatypes/index';
import { NumberRange } from '../datatypes/numberRange';
import { SQLDialect } from '../dialect/baseDialect';
import { continuousFloorExpression } from '../helper/utils';
import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';

export class NumberBucketExpression extends ChainableExpression {
  static op = "NumberBucket";
  static fromJS(parameters: ExpressionJS): NumberBucketExpression {
    let value = ChainableExpression.jsToValue(parameters);
    value.size = parameters.size;
    value.offset = hasOwnProp(parameters, 'offset') ? parameters.offset : 0;
    return new NumberBucketExpression(value);
  }

  public size: number;
  public offset: number;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this.size = parameters.size;
    this.offset = parameters.offset;
    this._ensureOp("numberBucket");
    this._checkOperandTypes('NUMBER');
    this.type = 'NUMBER_RANGE';
  }

  public valueOf(): ExpressionValue {
    let value = super.valueOf();
    value.size = this.size;
    value.offset = this.offset;
    return value;
  }

  public toJS(): ExpressionJS {
    let js = super.toJS();
    js.size = this.size;
    if (this.offset) js.offset = this.offset;
    return js;
  }

  public equals(other: NumberBucketExpression): boolean {
    return super.equals(other) &&
      this.size === other.size &&
      this.offset === other.offset;
  }

  protected _toStringParameters(indent?: int): string[] {
    let params: string[] = [String(this.size)];
    if (this.offset) params.push(String(this.offset));
    return params;
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    return operandValue !== null ? NumberRange.numberBucket(operandValue, this.size, this.offset) : null;
  }

  protected _getJSChainableHelper(operandJS: string): string {
    return Expression.jsNullSafetyUnary(operandJS, (n) => continuousFloorExpression(n, "Math.floor", this.size, this.offset));
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return continuousFloorExpression(operandSQL, "FLOOR", this.size, this.offset);
  }
}

Expression.register(NumberBucketExpression);
