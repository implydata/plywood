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
import { Indexer, Alterations } from "../expressions/baseExpression";
import { Datum, ComputeFn, foldContext } from "../datatypes/dataset";

export class QuantileAction extends Action {
  static fromJS(parameters: ActionJS): QuantileAction {
    var value = Action.jsToValue(parameters);
    value.quantile = parameters.quantile;
    return new QuantileAction(value);
  }

  public quantile: number;

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this.quantile = parameters.quantile;
    this._ensureAction("quantile");
    this._checkExpressionTypes('NUMBER');
  }

  public valueOf(): ActionValue {
    var value = super.valueOf();
    value.quantile = this.quantile;
    return value;
  }

  public toJS(): ActionJS {
    var js = super.toJS();
    js.quantile = this.quantile;
    return js;
  }

  public equals(other: QuantileAction): boolean {
    return super.equals(other) &&
      this.quantile === other.quantile;
  }

  protected _toStringParameters(expressionString: string): string[] {
    return [expressionString, String(this.quantile)];
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return 'DATASET';
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'NUMBER';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
    return {
      type: 'NUMBER'
    };
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
    var quantile = this.quantile;
    return (d: Datum, c: Datum) => {
      var inV = inputFn(d, c);
      return inV ? inV.quantile(expressionFn, quantile, foldContext(d, c)) : null;
    }
  }

  public isAggregate(): boolean {
    return true;
  }

  public isNester(): boolean {
    return true;
  }
}

Action.register(QuantileAction);
