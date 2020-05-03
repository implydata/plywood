/*
 * Copyright 2016-2020 Imply Data, Inc.
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

import * as hasOwnProp from 'has-own-prop';
import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { DatasetFullType } from '../types';
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
import { ExternalExpression } from './externalExpression';

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

  public updateTypeContext(typeContext: DatasetFullType, expressionTypeContext: DatasetFullType): DatasetFullType {
    const myDatasetType = typeContext.datasetType;
    const expressionDatasetType = expressionTypeContext.datasetType;
    for (let k in expressionDatasetType) {
      typeContext.datasetType[k] = expressionDatasetType[k];

      let ft = expressionDatasetType[k];
      if (hasOwnProp(myDatasetType, k)) {
        if (myDatasetType[k].type !== ft.type) {
          throw new Error(`incompatible types of joins on ${k} between ${myDatasetType[k].type} and ${ft.type}`);
        }
      } else {
        myDatasetType[k] = ft;
      }
    }
    return typeContext;
  }

  public pushIntoExternal(): ExternalExpression | null {
    return null;
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return operandValue ? operandValue.join(expressionValue) : null;
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    throw new Error('not possible');
  }

}

Expression.register(JoinExpression);
