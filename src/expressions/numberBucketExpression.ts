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
import { hasOwnProperty, continuousFloorExpression } from '../helper/utils';
import { NumberRange } from '../datatypes/numberRange';

export interface NumberBucketExpressionJS extends ExpressionJS {
  size: number;
  offset?: number;
}

export interface NumberBucketExpressionValue extends ExpressionValue {
  size: number;
  offset?: number;
}

export class NumberBucketExpression extends ChainableExpression {
  static op = "NumberBucket";
  static fromJS(parameters: NumberBucketExpressionJS): NumberBucketExpression {
    let value = ChainableExpression.jsToValue(parameters) as NumberBucketExpressionValue;
    value.size = parameters.size;
    value.offset = hasOwnProperty(parameters, 'offset') ? parameters.offset : 0;
    return new NumberBucketExpression(value);
  }

  public size: number;
  public offset: number;

  constructor(parameters: NumberBucketExpressionValue) {
    super(parameters, dummyObject);
    this.size = parameters.size;
    this.offset = parameters.offset;
    this._ensureOp("numberBucket");
    this._checkOperandTypes('NUMBER');
    this.type = 'NUMBER_RANGE';
  }

  public valueOf(): NumberBucketExpressionValue {
    let value = super.valueOf() as NumberBucketExpressionValue;
    value.size = this.size;
    value.offset = this.offset;
    return value;
  }

  public toJS(): NumberBucketExpressionJS {
    let js = super.toJS() as NumberBucketExpressionJS;
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
    let size = this.size;
    let offset = this.offset;
      let num = operandValue;
      if (num === null) return null;
      return NumberRange.numberBucket(num, size, offset);
  }

  protected _getJSChainableHelper(operandJS: string): string {
    return Expression.jsNullSafetyUnary(operandJS, (n) => continuousFloorExpression(n, "Math.floor", this.size, this.offset));
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return continuousFloorExpression(operandSQL, "FLOOR", this.size, this.offset);
  }
}

Expression.register(NumberBucketExpression);
