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

import { r, ExpressionJS, ExpressionValue, Expression, ChainableUnaryExpression } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
import { PlywoodValue } from '../datatypes/index';

export class QuantileExpression extends ChainableUnaryExpression implements Aggregate {
  static op = "Quantile";
  static fromJS(parameters: ExpressionJS): QuantileExpression {
    let value = ChainableUnaryExpression.jsToValue(parameters);
    value.value = parameters.value || (parameters as any).quantile;
    return new QuantileExpression(value);
  }

  public value: number;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("quantile");
    this._checkOperandTypes('DATASET');
    this._checkExpressionTypes('NUMBER');
    this.value = parameters.value;
    this.type = 'NUMBER';
  }

  public valueOf(): ExpressionValue {
    let value = super.valueOf();
    value.value = this.value;
    return value;
  }

  public toJS(): ExpressionJS {
    let js = super.toJS();
    js.value = this.value;
    return js;
  }

  public equals(other: QuantileExpression): boolean {
    return super.equals(other) &&
      this.value === other.value;
  }

  protected _toStringParameters(indent?: int): string[] {
    return [this.expression.toString(indent), String(this.value)];
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return operandValue ? operandValue.quantile(this.expression.getFn(), this.value) : null;
  }

  // protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
  //   const { expression, quantile } = this;
  //   if (literalExpression.value === null) return Expression.NULL;
  //   let dataset = literalExpression.value;
  //
  //   dataset = dataset.quantile(expression.getFn(), quantile);
  //
  //   return r(dataset);
  // }
}

Expression.applyMixins(QuantileExpression, [Aggregate]);
Expression.register(QuantileExpression);
