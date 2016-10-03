/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
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


const REGEXP_SPECIAL = "\\^$.|?*+()[{";

import { Action, ActionJS, ActionValue } from './baseAction';
import { PlyType, DatasetFullType, PlyTypeSingleValue, FullType } from '../types';
import { SQLDialect } from '../dialect/baseDialect';
import { Datum, ComputeFn } from '../datatypes/dataset';

export class MatchAction extends Action {

  static likeToRegExp(like: string, escapeChar = '\\'): string {
    var regExp: string[] = ['^'];
    for (var i = 0; i < like.length; i++) {
      var char = like[i];
      if (char === escapeChar) {
        var nextChar = like[i + 1];
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

  static fromJS(parameters: ActionJS): MatchAction {
    var value = Action.jsToValue(parameters);
    value.regexp = parameters.regexp;
    return new MatchAction(value);
  }

  public regexp: string;

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this.regexp = parameters.regexp;
    this._ensureAction("match");
  }

  public valueOf(): ActionValue {
    var value = super.valueOf();
    value.regexp = this.regexp;
    return value;
  }

  public toJS(): ActionJS {
    var js = super.toJS();
    js.regexp = this.regexp;
    return js;
  }

  public equals(other: MatchAction): boolean {
    return super.equals(other) &&
      this.regexp === other.regexp;
  }

  protected _toStringParameters(expressionString: string): string[] {
    return [this.regexp];
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return this._stringTransformInputType;
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'BOOLEAN';
  }

  public _fillRefSubstitutions(): FullType {
    return {
      type: 'BOOLEAN'
    };
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
    var re = new RegExp(this.regexp);
    return (d: Datum, c: Datum) => {
      var inV = inputFn(d, c);
      if (!inV) return null;
      if (inV === null) return null;
      return re.test(inV);
    };
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string, expressionJS: string): string {
    return `/${this.regexp}/.test(${inputJS})`;
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return dialect.regexpExpression(inputSQL, this.regexp);
  }
}

Action.register(MatchAction);
