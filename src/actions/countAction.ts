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

module Plywood {
  export class CountAction extends Action {
    static fromJS(parameters: ActionJS): CountAction {
      return new CountAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("count");
      this._checkNoExpression();
    }

    public getNecessaryInputTypes(): PlyType | PlyType[] {
      return 'DATASET';
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType);
      return 'NUMBER';
    }

    public _fillRefSubstitutions(): FullType {
      return {
        type: 'NUMBER',
      };
    }

    public getFn(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        return inV ? inV.count() : 0;
      }
    }

    protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return inputSQL.indexOf(' WHERE ') === -1 ? `COUNT(*)` : `SUM(${dialect.aggregateFilterIfNeeded(inputSQL, '1')})`;
    }

    public isAggregate(): boolean {
      return true;
    }
  }

  Action.register(CountAction);
}
