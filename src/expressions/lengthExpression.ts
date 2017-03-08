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



import { r, ExpressionJS, ExpressionValue, Expression, ChainableExpression } from './baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { PlywoodValue, Set } from '../datatypes/index';

export class LengthExpression extends ChainableExpression {
  static op = "Length";
  static fromJS(parameters: ExpressionJS): LengthExpression {
    return new LengthExpression(ChainableExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("length");
    this._checkOperandTypes('STRING');
    this.type = 'NUMBER';
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    return operandValue ? (operandValue as string).length : null;
  }

  protected _getJSChainableHelper(operandJS: string): string {
    return Expression.jsNullSafetyUnary(operandJS, (input: string) => `${input}.length`);
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return dialect.lengthExpression(operandSQL);
  }
}

Expression.register(LengthExpression);
