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

import { Dataset, PlywoodValue, Set } from '../datatypes/index';
import {
  ChainableUnaryExpression,
  Expression,
  ExpressionJS,
  ExpressionValue,
} from './baseExpression';
import { Aggregate } from './mixins/aggregate';

export class CollectExpression extends ChainableUnaryExpression implements Aggregate {
  static op = 'Collect';
  static fromJS(parameters: ExpressionJS): CollectExpression {
    return new CollectExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('collect');
    this._checkOperandTypes('DATASET');
    this._checkExpressionTypes(
      'BOOLEAN',
      'NUMBER',
      'TIME',
      'STRING',
      'NUMBER_RANGE',
      'TIME_RANGE',
      'STRING_RANGE',
    );
    this.type = Set.wrapSetType(this.expression.type);
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return operandValue ? (operandValue as Dataset).collect(this.expression) : null;
  }
}

Expression.applyMixins(CollectExpression, [Aggregate]);
Expression.register(CollectExpression);
