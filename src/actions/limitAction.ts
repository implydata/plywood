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
import { SQLDialect } from "../dialect/baseDialect";
import { Datum, ComputeFn } from "../datatypes/dataset";
import { ApplyAction } from "./applyAction";

export class LimitAction extends Action {
  static fromJS(parameters: ActionJS): LimitAction {
    return new LimitAction({
      action: parameters.action,
      limit: parameters.limit
    });
  }

  public limit: int;

  constructor(parameters: ActionValue = {}) {
    super(parameters, dummyObject);
    this.limit = parameters.limit;
    this._ensureAction("limit");
  }

  public valueOf(): ActionValue {
    var value = super.valueOf();
    value.limit = this.limit;
    return value;
  }

  public toJS(): ActionJS {
    var js = super.toJS();
    js.limit = this.limit;
    return js;
  }

  public equals(other: LimitAction): boolean {
    return super.equals(other) &&
      this.limit === other.limit;
  }

  protected _toStringParameters(expressionString: string): string[] {
    return [String(this.limit)];
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return 'DATASET';
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'DATASET';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    return inputType;
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
    var limit = this.limit;
    return (d: Datum, c: Datum) => {
      var inV = inputFn(d, c);
      return inV ? inV.limit(limit) : null;
    }
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return `LIMIT ${this.limit}`;
  }

  protected _foldWithPrevAction(prevAction: Action): Action {
    if (prevAction instanceof LimitAction) {
      return new LimitAction({
        limit: Math.min(prevAction.limit, this.limit)
      });
    }
    return null;
  }

  protected _putBeforeLastAction(lastAction: Action): Action {
    if (lastAction instanceof ApplyAction) {
      return this;
    }
    return null;
  }
}

Action.register(LimitAction);
