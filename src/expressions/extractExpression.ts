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



import { r, BaseExpressionJS, ExpressionValue, Expression, ChainableExpression } from './baseExpression';
import { Indexer, Alterations } from './baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { PlywoodValue } from '../datatypes/index';
import { MatchExpression } from './matchExpression';
import { RegexExpressionJS, RegexExpressionValue } from "./interfaces/interfaces";

export class ExtractExpression extends ChainableExpression {
  static op = "Extract";
  static fromJS(parameters: RegexExpressionJS): ExtractExpression {
    let value = ChainableExpression.jsToValue(parameters) as RegexExpressionValue;
    value.regexp = parameters.regexp;
    return new ExtractExpression(value);
  }

  public regexp: string;

  constructor(parameters: RegexExpressionValue) {
    super(parameters, dummyObject);
    this.regexp = parameters.regexp;
    this._ensureOp("extract");
    this._checkOperandTypes('STRING', 'SET/STRING');
    this.type = this.operand.type;
  }

  public valueOf(): RegexExpressionValue {
    let value = super.valueOf() as RegexExpressionValue;
    value.regexp = this.regexp;
    return value;
  }

  public toJS(): RegexExpressionJS {
    let js = super.toJS() as RegexExpressionJS;
    js.regexp = this.regexp;
    return js;
  }

  public equals(other: ExtractExpression): boolean {
    return super.equals(other) &&
      this.regexp === other.regexp;
  }

  protected _toStringParameters(indent?: int): string[] {
    return [this.regexp];
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    let re = new RegExp(this.regexp);
    return (String(operandValue).match(re) || [])[1] || null;
  }

  protected _getJSChainableHelper(operandJS: string): string {
    return `((''+${operandJS}).match(/${this.regexp}/) || [])[1] || null`;
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return dialect.extractExpression(operandSQL, this.regexp);
  }
}

Expression.register(ExtractExpression);
