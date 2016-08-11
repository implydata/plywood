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
import { Datum, ComputeFn } from "../datatypes/dataset";
import { InAction } from "./inAction";
import { TimeBucketAction } from "./timeBucketAction";
import { TimeRange } from "../datatypes/timeRange";
import { NumberBucketAction } from "./numberBucketAction";
import { NumberRange } from "../datatypes/numberRange";
import { ChainExpression } from "../expressions/chainExpression";
import { IndexOfAction } from "./indexOfAction";
import { FallbackAction } from "./fallbackAction";
import { LiteralExpression } from "../expressions/literalExpression";
import { RefExpression } from "../expressions/refExpression";

export class IsAction extends Action {
  static fromJS(parameters: ActionJS): IsAction {
    return new IsAction(Action.jsToValue(parameters));
  }

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this._ensureAction("is");
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return this.expression.type;
  }

  public getOutputType(inputType: PlyType): PlyType {
    var expressionType = this.expression.type;
    if (expressionType && expressionType !== 'NULL') this._checkInputTypes(inputType);
    return 'BOOLEAN';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
    return {
      type: 'BOOLEAN'
    };
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
    return (d: Datum, c: Datum) => {
      return inputFn(d, c) === expressionFn(d, c);
    }
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string, expressionJS: string): string {
    return `(${inputJS}===${expressionJS})`;
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return dialect.isNotDistinctFromExpression(inputSQL, expressionSQL);
  }

  protected _nukeExpression(precedingExpression: Expression): Expression {
    var prevAction = precedingExpression.lastAction();
    var literalValue = this.getLiteralValue();

    if (prevAction instanceof TimeBucketAction && literalValue instanceof TimeRange && prevAction.timezone) {
      if (literalValue.start !== null && TimeRange.timeBucket(literalValue.start, prevAction.duration, prevAction.timezone).equals(literalValue)) return null;
      return Expression.FALSE;
    }

    if (prevAction instanceof NumberBucketAction && literalValue instanceof NumberRange) {
      if (literalValue.start !== null && NumberRange.numberBucket(literalValue.start, prevAction.size, prevAction.offset).equals(literalValue)) return null;
      return Expression.FALSE;
    }

    return null;
  }

  protected _foldWithPrevAction(prevAction: Action): Action {
    var literalValue = this.getLiteralValue();

    if (prevAction instanceof TimeBucketAction && literalValue instanceof TimeRange && prevAction.timezone) {
      if (!(literalValue.start !== null && TimeRange.timeBucket(literalValue.start, prevAction.duration, prevAction.timezone).equals(literalValue))) return null;
      return new InAction({ expression: this.expression });
    }

    if (prevAction instanceof NumberBucketAction && literalValue instanceof NumberRange) {
      if (!(literalValue.start !== null && NumberRange.numberBucket(literalValue.start, prevAction.size, prevAction.offset).equals(literalValue))) return null;
      return new InAction({ expression: this.expression })
    }

    if (prevAction instanceof FallbackAction && prevAction.expression.isOp('literal') && this.expression.isOp('literal') && !prevAction.expression.equals(this.expression)) {
      return this;
    }

    return null;
  }

  protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
    var expression = this.expression;
    if (!expression.isOp('literal')) {
      return new IsAction({ expression: literalExpression }).performOnSimple(expression);
    }
    return null;
  }

  protected _performOnRef(refExpression: RefExpression): Expression {
    if (this.expression.equals(refExpression)) {
      return Expression.TRUE;
    }
    return null;
  }

  protected _performOnSimpleChain(chainExpression: ChainExpression): Expression {
    if (this.expression.equals(chainExpression)) {
      return Expression.TRUE;
    }

    var prevAction = chainExpression.lastAction();
    var literalValue = this.getLiteralValue();

    if (prevAction instanceof IndexOfAction && literalValue === -1) {
      var precedingExpression = (chainExpression as ChainExpression).expression;
      var actionExpression = prevAction.expression;
      return precedingExpression.contains(actionExpression).not().simplify();
    }

    return null;
  }
}

Action.register(IsAction);
