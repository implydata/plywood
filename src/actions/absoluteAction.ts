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
import { PlyType, DatasetFullType, PlyTypeSingleValue, FullType } from "../types";
import { SQLDialect } from "../dialect/baseDialect";
import { Datum, ComputeFn } from "../datatypes/dataset";

export class AbsoluteAction extends Action {
  static fromJS(parameters: ActionJS): AbsoluteAction {
    return new AbsoluteAction(Action.jsToValue(parameters));
  }

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this._ensureAction("absolute");
    this._checkNoExpression();
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return 'NUMBER' as PlyType;
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'NUMBER';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType): FullType {
    return inputType;
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
    return (d: Datum, c: Datum) => {
      var inV = inputFn(d, c);
      if (inV === null) return null;
      return Math.abs(inV);
    }
  }

  protected _foldWithPrevAction(prevAction: Action): Action {
    if (prevAction.equals(this)) {
      return this;
    }
    return null;
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string): string {
    return `Math.abs(${inputJS})`
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return `ABS(${inputSQL})`
  }
}

Action.register(AbsoluteAction);
