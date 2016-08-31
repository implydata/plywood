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
import { PlyType, DatasetFullType, PlyTypeSingleValue, FullType } from "../types";
import { Indexer, Alterations } from "../expressions/baseExpression";
import { SQLDialect } from "../dialect/baseDialect";
import { Datum, ComputeFn, foldContext } from "../datatypes/dataset";
import { RefExpression } from "../expressions/refExpression";
import { ChainExpression } from "../expressions/chainExpression";

export class ApplyAction extends Action {
  static fromJS(parameters: ActionJS): ApplyAction {
    var value = Action.jsToValue(parameters);
    value.name = parameters.name;
    return new ApplyAction(value);
  }

  public name: string;

  constructor(parameters: ActionValue = {}) {
    super(parameters, dummyObject);
    this.name = parameters.name;
    this._ensureAction("apply");
  }

  public valueOf(): ActionValue {
    var value = super.valueOf();
    value.name = this.name;
    return value;
  }

  public toJS(): ActionJS {
    var js = super.toJS();
    js.name = this.name;
    return js;
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return 'DATASET';
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'DATASET';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    typeContext.datasetType[this.name] = this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
    return typeContext;
  }

  protected _toStringParameters(expressionString: string): string[] {
    var name = this.name;
    if (!RefExpression.SIMPLE_NAME_REGEXP.test(name)) name = JSON.stringify(name);
    return [name, expressionString];
  }

  public equals(other: ApplyAction): boolean {
    return super.equals(other) &&
      this.name === other.name;
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
    var name = this.name;
    var type = this.expression.type;
    return (d: Datum, c: Datum) => {
      var inV = inputFn(d, c);
      return inV ? inV.apply(name, expressionFn, type, foldContext(d, c)) : null;
    };
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return `${expressionSQL} AS ${dialect.escapeName(this.name)}`;
  }

  public isSimpleAggregate(): boolean {
    const { expression } = this;
    if (expression instanceof ChainExpression) {
      var actions = expression.actions;
      return actions.length === 1 && actions[0].isAggregate();
    }
    return false;
  }

  public isNester(): boolean {
    return true;
  }

  protected _removeAction(): boolean {
    const { name, expression } = this;
    if (expression instanceof RefExpression) {
      return expression.name === name && expression.nest === 0;
    }
    return false;
  }

  protected _putBeforeLastAction(lastAction: Action): Action {
    if (
      this.isSimpleAggregate() &&
      lastAction instanceof ApplyAction &&
      !lastAction.isSimpleAggregate() &&
      this.expression.getFreeReferences().indexOf(lastAction.name) === -1
    ) {
      return this;
    }
    return null;
  }

}

Action.register(ApplyAction);
