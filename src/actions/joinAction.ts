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
import { Datum, ComputeFn } from "../datatypes/dataset";
import { hasOwnProperty } from "../helper/utils";

export class JoinAction extends Action {
  static fromJS(parameters: ActionJS): JoinAction {
    return new JoinAction(Action.jsToValue(parameters));
  }

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this._ensureAction("join");
    if(!this.expression.canHaveType('DATASET')) throw new TypeError('expression must be a DATASET');
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return 'DATASET';
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'DATASET';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    var typeContextParent = typeContext.parent;
    var expressionFullType = <DatasetFullType>this.expression._fillRefSubstitutions(typeContextParent, indexer, alterations);

    var inputDatasetType = typeContext.datasetType;
    var expressionDatasetType = expressionFullType.datasetType;
    var newDatasetType: Lookup<FullType> = Object.create(null);

    for (var k in inputDatasetType) {
      newDatasetType[k] = inputDatasetType[k];
    }
    for (var k in expressionDatasetType) {
      var ft = expressionDatasetType[k];
      if (hasOwnProperty(newDatasetType, k)) {
        if (newDatasetType[k].type !== ft.type) {
          throw new Error(`incompatible types of joins on ${k} between ${newDatasetType[k].type} and ${ft.type}`);
        }
      } else {
        newDatasetType[k] = ft;
      }
    }

    return {
      parent: typeContextParent,
      type: 'DATASET',
      datasetType: newDatasetType,
      remote: typeContext.remote || expressionFullType.remote
    };
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
    return (d: Datum, c: Datum) => {
      var inV = inputFn(d, c);
      return inV ? inV.join(expressionFn(d, c)) : inV;
    }
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    throw new Error('not possible');
  }

}

Action.register(JoinAction);
