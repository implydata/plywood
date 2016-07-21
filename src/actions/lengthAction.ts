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
  export class LengthAction extends Action {
    static fromJS(parameters: ActionJS): LengthAction {
      return new LengthAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("length");
      this._checkNoExpression();
    }

    public getNecessaryInputTypes(): PlyType | PlyType[] {
      return 'STRING';
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
        return inV.length;
      }
    }
    
    protected _getJSHelper(inputType: PlyType, inputJS: string): string {
      return Expression.jsNullSafetyUnary(inputJS, (input: string) => `${input}.length`);
    }

    protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return dialect.lengthExpression(inputSQL);
    }
  }

  Action.register(LengthAction);
}
