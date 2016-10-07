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


import { Action, ActionJS, ActionValue, AggregateAction } from './baseAction';
import { PlyType } from '../types';
import { r, Expression, LiteralExpression } from '../expressions/index';
import { Datum, ComputeFn, foldContext } from '../datatypes/dataset';

export class QuantileAction extends AggregateAction {
  static fromJS(parameters: ActionJS): QuantileAction {
    let value = Action.jsToValue(parameters);
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
    let value = super.valueOf();
    value.quantile = this.quantile;
    return value;
  }

  public toJS(): ActionJS {
    let js = super.toJS();
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

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
    let quantile = this.quantile;
    return (d: Datum, c: Datum) => {
      let inV = inputFn(d, c);
      return inV ? inV.quantile(expressionFn, quantile, foldContext(d, c)) : null;
    };
  }

  protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
    const { expression, quantile } = this;
    if (literalExpression.value === null) return Expression.NULL;
    let dataset = literalExpression.value;

    dataset = dataset.quantile(expression.getFn(), quantile);

    return r(dataset);
  }
}

Action.register(QuantileAction);
