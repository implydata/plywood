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


import { dummyObject } from "../helper/dummy";
import { Action, ActionJS, ActionValue } from "./baseAction";
import { Expression } from "../expressions/baseExpression";
import { SQLDialect } from "../dialect/baseDialect";
import { Datum, ComputeFn } from "../datatypes/dataset";
import { AndAction } from "./andAction";

export class NotAction extends Action {
  static fromJS(parameters: ActionJS): NotAction {
    return new NotAction(Action.jsToValue(parameters));
  }

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this._ensureAction("not");
    this._checkNoExpression();
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return 'BOOLEAN';
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'BOOLEAN';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType): FullType {
    return inputType;
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
    return (d: Datum, c: Datum) => {
      return !inputFn(d, c);
    }
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string): string {
    return `!(${inputJS})`;
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return `NOT(${inputSQL})`;
  }

  protected _foldWithPrevAction(prevAction: Action): Action {
    if (prevAction instanceof NotAction) {
      return new AndAction({ expression: Expression.TRUE }); // Boolean noop
    }
    return null;
  }
}

Action.register(NotAction);
