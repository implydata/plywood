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
import { PlyTypeSingleValue } from '../types';

import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';

export class CustomTransformExpression extends ChainableExpression {
  static op = 'CustomTransform';
  static fromJS(parameters: ExpressionJS): CustomTransformExpression {
    const value = ChainableExpression.jsToValue(parameters);
    value.custom = parameters.custom;
    if (parameters.outputType) value.outputType = parameters.outputType;
    return new CustomTransformExpression(value);
  }

  public custom: string;
  public outputType: PlyTypeSingleValue;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('customTransform');
    this.custom = parameters.custom;
    if (parameters.outputType) this.outputType = parameters.outputType as PlyTypeSingleValue;
    this.type = this.outputType || this.operand.type;
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.custom = this.custom;
    if (this.outputType) value.outputType = this.outputType;
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    js.custom = this.custom;
    if (this.outputType) js.outputType = this.outputType;
    return js;
  }

  public equals(other: CustomTransformExpression | undefined): boolean {
    return (
      super.equals(other) && this.custom === other.custom && this.outputType === other.outputType
    );
  }

  protected _toStringParameters(_indent?: int): string[] {
    const param = [this.custom];
    if (this.outputType) param.push(this.outputType);
    return param;
  }

  protected _calcChainableHelper(_operandValue: any): PlywoodValue {
    throw new Error('can not calc on custom transform action');
  }

  protected _getSQLChainableHelper(_dialect: SQLDialect, _operandSQL: string): string {
    throw new Error('Custom transform not supported in SQL');
  }
}

Expression.register(CustomTransformExpression);
