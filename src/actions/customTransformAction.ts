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
import { SQLDialect } from "../dialect/baseDialect";
import { ComputeFn } from "../datatypes/dataset";

export class CustomTransformAction extends Action {
  static fromJS(parameters: ActionJS): CustomTransformAction {
    var value = Action.jsToValue(parameters);
    value.custom = parameters.custom;
    if (parameters.outputType) value.outputType = parameters.outputType;
    return new CustomTransformAction(value);
  }

  public custom: string;
  public outputType: PlyTypeSingleValue;


  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this.custom = parameters.custom;
    if (parameters.outputType) this.outputType = parameters.outputType as PlyTypeSingleValue;
    this._ensureAction("customTransform");
  }

  public getNecessaryInputTypes(): PlyType[] {
    return ['NULL' as PlyTypeSimple, 'BOOLEAN' as PlyTypeSimple, 'NUMBER' as PlyTypeSimple, 'TIME' as PlyTypeSimple, 'STRING' as PlyTypeSimple]
  }

  public valueOf(): ActionValue {
    var value = super.valueOf();
    value.custom = this.custom;
    if (this.outputType) value.outputType = this.outputType;
    return value;
  }

  public toJS(): ActionJS {
    var js = super.toJS();
    js.custom = this.custom;
    if (this.outputType) js.outputType = this.outputType;
    return js;
  }

  public equals(other: CustomTransformAction): boolean {
    return super.equals(other) &&
      this.custom === other.custom &&
      this.outputType === other.outputType;
  }

  protected _toStringParameters(expressionString: string): string[] {
    var param = [`${this.custom} }`];
    if (this.outputType) param.push(this.outputType);
    return param;
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return this.outputType || inputType;
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType): FullType {
    return inputType;
  }


  public getFn(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
    throw new Error('can not getFn on custom transform action');
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    throw new Error("Custom transform not supported in SQL");
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string): string {
    throw new Error("Custom transform can't yet be expressed as JS");
  }
}

Action.register(CustomTransformAction);
