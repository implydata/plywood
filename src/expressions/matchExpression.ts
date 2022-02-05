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

import { PlywoodValue, Set } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';

import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';

const REGEXP_SPECIAL = '\\^$.|?*+()[{';

export class MatchExpression extends ChainableExpression {
  static likeToRegExp(like: string, escapeChar = '\\'): string {
    const regExp: string[] = ['^'];
    for (let i = 0; i < like.length; i++) {
      let char = like[i];
      if (char === escapeChar) {
        const nextChar = like[i + 1];
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

  static op = 'Match';
  static fromJS(parameters: ExpressionJS): MatchExpression {
    const value = ChainableExpression.jsToValue(parameters);
    value.regexp = parameters.regexp;
    return new MatchExpression(value);
  }

  public regexp: string;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('match');
    this._checkOperandTypes('STRING');
    this.regexp = parameters.regexp;
    this.type = 'BOOLEAN';
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.regexp = this.regexp;
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    js.regexp = this.regexp;
    return js;
  }

  public equals(other: MatchExpression | undefined): boolean {
    return super.equals(other) && this.regexp === other.regexp;
  }

  protected _toStringParameters(_indent?: int): string[] {
    return [this.regexp];
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    const re = new RegExp(this.regexp);
    if (operandValue == null) return null;
    return Set.crossUnaryBoolean(operandValue, a => re.test(a));
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return dialect.regexpExpression(operandSQL, this.regexp);
  }
}

Expression.register(MatchExpression);
