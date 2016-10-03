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
import { Expression } from '../expressions/index';
import { SQLDialect } from '../dialect/baseDialect';
import { LiteralExpression } from '../expressions/literalExpression';

export class SumAction extends AggregateAction {
  static fromJS(parameters: ActionJS): SumAction {
    return new SumAction(Action.jsToValue(parameters));
  }

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this._ensureAction("sum");
    this._checkExpressionTypes('NUMBER');
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return `SUM(${dialect.aggregateFilterIfNeeded(inputSQL, expressionSQL)})`;
  }

  public canDistribute(): boolean {
    var expression = this.expression;
    return expression instanceof LiteralExpression ||
      Boolean(expression.getExpressionPattern('add') || expression.getExpressionPattern('subtract'));
  }

  public distribute(preEx: Expression): Expression {
    var expression = this.expression;
    if (expression instanceof LiteralExpression) {
      var value = expression.value;
      if (value === 0) return Expression.ZERO;
      return expression.multiply(preEx.count()).simplify();
    }

    var pattern: Expression[];
    if (pattern = expression.getExpressionPattern('add')) {
      return Expression.add(pattern.map(ex => preEx.sum(ex).distribute()));
    }
    if (pattern = expression.getExpressionPattern('subtract')) {
      return Expression.subtract(pattern.map(ex => preEx.sum(ex).distribute()));
    }
    return null;
  }
}

Action.register(SumAction);
