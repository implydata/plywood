/*
 * Copyright 2016-2017 Imply Data, Inc.
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


import { PlywoodValue } from "../datatypes/index";
import { SQLDialect } from "../dialect/baseDialect";
import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from "./baseExpression";
import { Aggregate } from "./mixins/aggregate";

export class CustomAggregateExpression extends ChainableExpression {
  static op = "CustomAggregate";
  static fromJS(parameters: ExpressionJS): CustomAggregateExpression {
    let value = ChainableExpression.jsToValue(parameters);
    value.custom = parameters.custom;
    return new CustomAggregateExpression(value);
  }

  public custom: string;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this.custom = parameters.custom;
    this._ensureOp("customAggregate");
    this._checkOperandTypes('DATASET');
    this.type = 'NUMBER';
  }

  public valueOf(): ExpressionValue {
    let value = super.valueOf();
    value.custom = this.custom;
    return value;
  }

  public toJS(): ExpressionJS {
    let js = super.toJS();
    js.custom = this.custom;
    return js;
  }

  public equals(other: CustomAggregateExpression): boolean {
    return super.equals(other) &&
      this.custom === other.custom;
  }

  protected _toStringParameters(indent?: int): string[] {
    return [this.custom]; // ToDo: escape this
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    throw new Error('can not compute on custom action');
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    throw new Error('custom action not implemented');
  }

}

Expression.applyMixins(CustomAggregateExpression, [Aggregate]);
Expression.register(CustomAggregateExpression);
