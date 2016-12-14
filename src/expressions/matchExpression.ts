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


import { RegexExpressionJS } from "./interfaces/interfaces";
const REGEXP_SPECIAL = "\\^$.|?*+()[{";


import { r, BaseExpressionJS, ExpressionValue, Expression, ChainableExpression } from './baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { PlywoodValue } from '../datatypes/index';

export interface MatchExpressionValue extends ExpressionValue {
  regexp: string;
}

export interface MatchExpressionJS extends BaseExpressionJS {
  regexp: string;
}

export class MatchExpression extends ChainableExpression {

  static likeToRegExp(like: string, escapeChar = '\\'): string {
    let regExp: string[] = ['^'];
    for (let i = 0; i < like.length; i++) {
      let char = like[i];
      if (char === escapeChar) {
        let nextChar = like[i + 1];
        if (!nextChar) throw new Error(`invalid LIKE string '${like}'`);
        char = nextChar;
        i++;
      } else if (char === '%') {
        regExp.push('.*');
        continue;
      } else if (char === '_') {
        regExp.push('.');
        continue;
      }

      if (REGEXP_SPECIAL.indexOf(char) !== -1) {
        regExp.push('\\');
      }
      regExp.push(char);
    }
    regExp.push('$');
    return regExp.join('');
  }

  static op = "Match";
  static fromJS(parameters: RegexExpressionJS): MatchExpression {
    let value = ChainableExpression.jsToValue(parameters) as MatchExpressionValue;
    value.regexp = parameters.regexp;
    return new MatchExpression(value);
  }

  public regexp: string;

  constructor(parameters: MatchExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("match");
    this._checkOperandTypes('STRING', 'SET/STRING');
    this.regexp = parameters.regexp;
    this.type = 'BOOLEAN';
  }

  public valueOf(): MatchExpressionValue {
    let value = super.valueOf() as MatchExpressionValue;
    value.regexp = this.regexp;
    return value;
  }

  public toJS(): MatchExpressionJS {
    let js = super.toJS() as MatchExpressionJS;
    js.regexp = this.regexp;
    return js;
  }

  public equals(other: MatchExpression): boolean {
    return super.equals(other) &&
      this.regexp === other.regexp;
  }

  protected _toStringParameters(indent?: int): string[] {
    return [this.regexp];
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    let re = new RegExp(this.regexp);
    if (!operandValue) return null;
    if (operandValue === null) return null;
    return re.test(operandValue);
  }

  protected _getJSChainableHelper(operandJS: string): string {
    return `/${this.regexp}/.test(${operandJS})`;
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return dialect.regexpExpression(operandSQL, this.regexp);
  }
}

Expression.register(MatchExpression);
