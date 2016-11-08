/*
 * Copyright 2016-2016 Imply Data, Inc.
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

import { r, ExpressionJS, ExpressionValue, Expression, ChainableUnaryExpression } from './baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { PlywoodValue } from '../datatypes/index';
import { hasOwnProperty } from '../helper/utils';

export class JoinExpression extends ChainableUnaryExpression {
  static op = "Join";
  static fromJS(parameters: ExpressionJS): JoinExpression {
    return new JoinExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("join");
    this._checkOperandTypes('DATASET');
    this._checkExpressionTypes('DATASET');
    this.type = 'DATASET';
  }

  /*
  public _fillRefSubstitutions__(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    let typeContextParent = typeContext.parent;
    let expressionFullType = <DatasetFullType>this.expression._fillRefSubstitutions__(typeContextParent, indexer, alterations);

    let inputDatasetType = typeContext.datasetType;
    let expressionDatasetType = expressionFullType.datasetType;
    let newDatasetType: Lookup<FullType> = Object.create(null);

    for (let k in inputDatasetType) {
      newDatasetType[k] = inputDatasetType[k];
    }
    for (let k in expressionDatasetType) {
      let ft = expressionDatasetType[k];
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
      datasetType: newDatasetType
    };
  }
  */

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return operandValue ? operandValue.join(expressionValue) : null;
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    throw new Error('not possible');
  }

}

Expression.register(JoinExpression);
