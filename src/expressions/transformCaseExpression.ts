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

import { r, BaseExpressionJS, ExpressionValue, Expression, ChainableExpression, CaseType } from './baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { PlywoodValue } from '../datatypes/index';


export interface TransformCaseExpressionJS extends BaseExpressionJS {
  transformType: CaseType;
}

export interface TransformCaseExpressionValue extends ExpressionValue {
  transformType: CaseType;
}

export class TransformCaseExpression extends ChainableExpression {
  static UPPER_CASE = 'upperCase';
  static LOWER_CASE = 'lowerCase';

  static op = "TransformCase";
  static fromJS(parameters: TransformCaseExpressionJS): TransformCaseExpression {
    let value = ChainableExpression.jsToValue(parameters) as TransformCaseExpressionValue;
    value.transformType = parameters.transformType;
    return new TransformCaseExpression(value);
  }

  public transformType: CaseType;

  constructor(parameters: TransformCaseExpressionValue) {
    super(parameters, dummyObject);
    let transformType = parameters.transformType;
    if (transformType !== TransformCaseExpression.UPPER_CASE && transformType !== TransformCaseExpression.LOWER_CASE) {
      throw new Error(`Must supply transform type of '${TransformCaseExpression.UPPER_CASE}' or '${TransformCaseExpression.LOWER_CASE}'`);
    }
    this.transformType = transformType;
    this._ensureOp("transformCase");
    this._checkOperandTypes('STRING');
    this.type = 'STRING';
  }

  public valueOf(): TransformCaseExpressionValue {
    let value = super.valueOf() as TransformCaseExpressionValue;
    value.transformType = this.transformType;
    return value;
  }

  public toJS(): TransformCaseExpressionJS {
    let js = super.toJS() as TransformCaseExpressionJS;
    js.transformType = this.transformType;
    return js;
  }

  public equals(other: TransformCaseExpression): boolean {
    return super.equals(other) &&
      this.transformType === other.transformType;
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    const { transformType } = this;
    return transformType === TransformCaseExpression.UPPER_CASE ? String(operandValue).toLocaleUpperCase() : String(operandValue).toLocaleLowerCase();
  }

  protected _getJSChainableHelper(operandJS: string): string {
    const { transformType } = this;
    return Expression.jsNullSafetyUnary(operandJS, (input: string) => {
      return transformType === TransformCaseExpression.UPPER_CASE ? `String(${input}).toLocaleUpperCase()` : `String(${input}).toLocaleLowerCase()`;
    });
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    const { transformType } = this;
    return transformType === TransformCaseExpression.UPPER_CASE ? `UPPER(${operandSQL})` : `LOWER(${operandSQL})`;
  }

  public specialSimplify(): Expression {
    const { operand } = this;
    if (operand instanceof TransformCaseExpression) return this.changeOperand(operand.operand);
    return this;
  }
}

Expression.register(TransformCaseExpression);
