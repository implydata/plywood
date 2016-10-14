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


import { Action, ActionJS, ActionValue } from './baseAction';
import { PlyType, DatasetFullType, PlyTypeSingleValue, FullType } from '../types';
import { Expression } from '../expressions/baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { Datum, ComputeFn } from '../datatypes/dataset';
import { getAllSetTypes } from '../datatypes/common';

export class CardinalityAction extends Action {
  static fromJS(parameters: ActionJS): CardinalityAction {
    return new CardinalityAction(Action.jsToValue(parameters));
  }

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this._ensureAction("cardinality");
    this._checkNoExpression();
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return getAllSetTypes();
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'NUMBER';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType): FullType {
    return inputType;
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
    return (d: Datum) => {
      let inV = inputFn(d);
      if (inV === null) return null;
      if (Array.isArray(inV)) return inV.length; // this is to allow passing an array into .compute()
      return inV.cardinality();
    };
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string): string {
    return Expression.jsNullSafetyUnary(inputJS, (input: string) => `${input}.length`);
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return `cardinality(${inputSQL})`;
  }
}

Action.register(CardinalityAction);
