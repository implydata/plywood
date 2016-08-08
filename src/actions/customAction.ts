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

module Plywood {
  export class CustomAction extends Action {
    static fromJS(parameters: ActionJS): CustomAction {
      var value = Action.jsToValue(parameters);
      value.custom = parameters.custom;
      return new CustomAction(value);
    }

    public custom: string;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.custom = parameters.custom;
      this._ensureAction("custom");
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.custom = this.custom;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.custom = this.custom;
      return js;
    }

    public equals(other: CustomAction): boolean {
      return super.equals(other) &&
        this.custom === other.custom;
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [this.custom]; // ToDo: escape this
    }

    public getNecessaryInputTypes(): PlyType | PlyType[] {
      return 'DATASET';
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType);
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      return {
        type: 'NUMBER',
      };
    }

    public getFn(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
      throw new Error('can not getFn on custom action');
    }

    protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      throw new Error('custom action not implemented');
    }

    public isAggregate(): boolean {
      return true;
    }
  }

  Action.register(CustomAction);
}
