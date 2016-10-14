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

import { Action, ActionJS, ActionValue, CaseType } from './baseAction';
import { PlyType, DatasetFullType, PlyTypeSingleValue, FullType } from '../types';
import { Indexer, Alterations } from '../expressions/baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { Datum, ComputeFn } from '../datatypes/dataset';

export class TransformCaseAction extends Action {
  static UPPER_CASE = 'upperCase';
  static LOWER_CASE = 'lowerCase';

  static fromJS(parameters: ActionJS): TransformCaseAction {
    let value = Action.jsToValue(parameters);
    value.transformType = parameters.transformType;
    return new TransformCaseAction(value);
  }

  public transformType: CaseType;

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    let transformType = parameters.transformType;
    if (transformType !== TransformCaseAction.UPPER_CASE && transformType !== TransformCaseAction.LOWER_CASE) {
      throw new Error(`Must supply transform type of '${TransformCaseAction.UPPER_CASE}' or '${TransformCaseAction.LOWER_CASE}'`);
    }
    this.transformType = transformType;
    this._ensureAction("transformCase");
  }

  public valueOf(): ActionValue {
    let value = super.valueOf();
    value.transformType = this.transformType;
    return value;
  }

  public toJS(): ActionJS {
    let js = super.toJS();
    js.transformType = this.transformType;
    return js;
  }

  public equals(other: TransformCaseAction): boolean {
    return super.equals(other) &&
      this.transformType === other.transformType;
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return 'STRING';
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'STRING';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    return inputType;
  }

  protected _foldWithPrevAction(prevAction: Action): Action {
    if (prevAction instanceof TransformCaseAction) {
      return this;
    }
    return null;
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
    const { transformType } = this;
    return (d: Datum) => {
      return transformType === TransformCaseAction.UPPER_CASE ? inputFn(d).toLocaleUpperCase() : inputFn(d).toLocaleLowerCase();
    };
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string, expressionJS: string): string {
    const { transformType } = this;
    return transformType === TransformCaseAction.UPPER_CASE ? `${inputJS}.toLocaleUpperCase()` : `${inputJS}.toLocaleLowerCase()`;
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string): string {
    const { transformType } = this;
    return transformType === TransformCaseAction.UPPER_CASE ? `UPPER(${inputSQL})` : `LOWER(${inputSQL})`;
  }
}

Action.register(TransformCaseAction);
