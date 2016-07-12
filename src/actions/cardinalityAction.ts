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
  export class CardinalityAction extends Action {
    static fromJS(parameters: ActionJS): CardinalityAction {
      return new CardinalityAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("cardinality");
      this._checkNoExpression();
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'SET/STRING', 'SET/STRING_RANGE', 'SET/NUMBER', 'SET/NUMBER_RANGE', 'SET/TIME', 'SET/TIME_RANGE');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType): FullType {
      return inputType;
    }

    protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return null;
        if (Array.isArray(inV)) return inV.length; // this is to allow passing an array into .compute()
        return inV.cardinality();
      }
    }

    protected _getJSHelper(inputType: PlyType, inputJS: string): string {
      return Expression.jsNullSafetyUnary(inputJS, (input: string) => `${input}.length`);
    }

    protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `cardinality(${inputSQL})`
    }
  }

  Action.register(CardinalityAction);
}
