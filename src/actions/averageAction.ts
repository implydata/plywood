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
import { SQLDialect } from '../dialect/baseDialect';

export class AverageAction extends AggregateAction {
  static fromJS(parameters: ActionJS): AverageAction {
    return new AverageAction(Action.jsToValue(parameters));
  }

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this._ensureAction("average");
    this._checkExpressionTypes('NUMBER');
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return `AVG(${dialect.aggregateFilterIfNeeded(inputSQL, expressionSQL)})`;
  }
}

Action.register(AverageAction);
