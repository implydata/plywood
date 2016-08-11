/*
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


import { Action, ActionJS, ActionValue } from "./baseAction";
import { Indexer, Alterations } from "../expressions/baseExpression";
import { SQLDialect } from "../dialect/baseDialect";
import { Datum, ComputeFn } from "../datatypes/dataset";
import { MatchAction } from "./matchAction";

export class ExtractAction extends Action {
  static fromJS(parameters: ActionJS): ExtractAction {
    var value = Action.jsToValue(parameters);
    value.regexp = parameters.regexp;
    return new ExtractAction(value);
  }

  public regexp: string;

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this.regexp = parameters.regexp;
    this._ensureAction("extract");
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
    return this._stringTransformOutputType(inputType);
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    return inputType;
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
    var re = new RegExp(this.regexp);
    return (d: Datum, c: Datum) => {
      return (String(inputFn(d, c)).match(re) || [])[1] || null;
    }
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string, expressionJS: string): string {
    return `((''+${inputJS}).match(/${this.regexp}/) || [])[1] || null`;
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return dialect.extractExpression(inputSQL, this.regexp);
  }
}

Action.register(ExtractAction);
