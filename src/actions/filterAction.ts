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
import { Expression, Indexer, Alterations } from "../expressions/baseExpression";
import { SQLDialect } from "../dialect/baseDialect";
import { RefExpression } from "../expressions/refExpression";
import { SplitAction } from "./splitAction";
import { SortAction } from "./sortAction";
import { ApplyAction } from "./applyAction";

export class FilterAction extends Action {
  static fromJS(parameters: ActionJS): FilterAction {
    return new FilterAction({
      action: parameters.action,
      name: parameters.name,
      expression: Expression.fromJS(parameters.expression)
    });
  }

  constructor(parameters: ActionValue = {}) {
    super(parameters, dummyObject);
    this._ensureAction("filter");
    this._checkExpressionTypes('BOOLEAN');
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return 'DATASET';
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'DATASET';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
    return inputType;
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return `${inputSQL} WHERE ${expressionSQL}`;
  }

  public isNester(): boolean {
    return true;
  }

  protected _foldWithPrevAction(prevAction: Action): Action {
    if (prevAction instanceof FilterAction) {
      return new FilterAction({
        expression: prevAction.expression.and(this.expression)
      });
    }
    return null;
  }

  protected _putBeforeLastAction(lastAction: Action): Action {
    if (lastAction instanceof ApplyAction) {
      var freeReferences = this.getFreeReferences();
      return freeReferences.indexOf(lastAction.name) === -1 ? this : null;
    }

    if (lastAction instanceof SplitAction) {
      var splits = lastAction.splits;
      return new FilterAction({
        expression: this.expression.substitute((ex) => {
          if (ex instanceof RefExpression && splits[ex.name]) return splits[ex.name];
          return null;
        })
      });
    }

    if (lastAction instanceof SortAction) {
      return this;
    }

    return null;
  }
}

Action.register(FilterAction);
