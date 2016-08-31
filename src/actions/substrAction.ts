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


import { Action, ActionJS, ActionValue } from "./baseAction";
import { PlyType, DatasetFullType, PlyTypeSingleValue, FullType } from "../types";
import { SQLDialect } from "../dialect/baseDialect";
import { Datum, ComputeFn } from "../datatypes/dataset";

export class SubstrAction extends Action {
  static fromJS(parameters: ActionJS): SubstrAction {
    var value = Action.jsToValue(parameters);
    value.position = parameters.position;
    value.length = parameters.length;
    return new SubstrAction(value);
  }

  public position: int;
  public length: int;

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this.position = parameters.position;
    this.length = parameters.length;
    this._ensureAction("substr");
  }

  public valueOf(): ActionValue {
    var value = super.valueOf();
    value.position = this.position;
    value.length = this.length;
    return value;
  }

  public toJS(): ActionJS {
    var js = super.toJS();
    js.position = this.position;
    js.length = this.length;
    return js;
  }

  public equals(other: SubstrAction): boolean {
    return super.equals(other) &&
      this.position === other.position &&
      this.length === other.length;
  }

  protected _toStringParameters(expressionString: string): string[] {
    return [String(this.position), String(this.length)];
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return this._stringTransformInputType;
  }

  public getOutputType(inputType: PlyType): PlyType {
    return this._stringTransformOutputType(inputType);
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType): FullType {
    return inputType;
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
    const { position, length } = this;
    return (d: Datum, c: Datum) => {
      var inV = inputFn(d, c);
      if (inV === null) return null;
      return inV.substr(position, length);
    };
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string): string {
    const { position, length } = this;
    return `(_=${inputJS},_==null?null:(''+_).substr(${position},${length}))`;
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return `SUBSTR(${inputSQL},${this.position + 1},${this.length})`;
  }
}

Action.register(SubstrAction);
