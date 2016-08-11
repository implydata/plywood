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


import { Action, ActionJS, ActionValue } from "./baseAction";
import { Expression } from "../expressions/baseExpression";
import { SQLDialect } from "../dialect/baseDialect";
import { Datum, ComputeFn } from "../datatypes/dataset";
import { hasOwnProperty, continuousFloorExpression } from "../helper/utils";
import { NumberRange } from "../datatypes/numberRange";

export class NumberBucketAction extends Action {
  static fromJS(parameters: ActionJS): NumberBucketAction {
    var value = Action.jsToValue(parameters);
    value.size = parameters.size;
    value.offset = hasOwnProperty(parameters, 'offset') ? parameters.offset : 0;
    return new NumberBucketAction(value);
  }

  public size: number;
  public offset: number;

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this.size = parameters.size;
    this.offset = parameters.offset;
    this._ensureAction("numberBucket");
  }

  public valueOf(): ActionValue {
    var value = super.valueOf();
    value.size = this.size;
    value.offset = this.offset;
    return value;
  }

  public toJS(): ActionJS {
    var js = super.toJS();
    js.size = this.size;
    if (this.offset) js.offset = this.offset;
    return js;
  }

  public equals(other: NumberBucketAction): boolean {
    return super.equals(other) &&
      this.size === other.size &&
      this.offset === other.offset;
  }

  protected _toStringParameters(expressionString: string): string[] {
    var params: string[] = [String(this.size)];
    if (this.offset) params.push(String(this.offset));
    return params;
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return ['NUMBER' as PlyType, 'NUMBER_RANGE' as PlyType];
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'NUMBER_RANGE';
  }

  public _fillRefSubstitutions(): FullType {
    return {
      type: 'NUMBER_RANGE',
    };
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
    var size = this.size;
    var offset = this.offset;
    return (d: Datum, c: Datum) => {
      var num = inputFn(d, c);
      if (num === null) return null;
      return NumberRange.numberBucket(num, size, offset);
    }
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string): string {
    return Expression.jsNullSafetyUnary(inputJS, (n) => continuousFloorExpression(n, "Math.floor", this.size, this.offset));
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return continuousFloorExpression(inputSQL, "FLOOR", this.size, this.offset);
  }
}

Action.register(NumberBucketAction);
