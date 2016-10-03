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


import { Action, ActionJS, ActionValue } from './baseAction';
import { PlyType, DatasetFullType, PlyTypeSingleValue, FullType } from '../types';
import { r, Indexer, Alterations, Expression, LiteralExpression } from '../expressions/index';
import { SQLDialect } from '../dialect/baseDialect';
import { Datum, ComputeFn } from '../datatypes/dataset';
import { RefExpression } from '../expressions/refExpression';

export type Direction = 'ascending' | 'descending';

export class SortAction extends Action {
  static DESCENDING: Direction = 'descending';
  static ASCENDING: Direction = 'ascending';

  static fromJS(parameters: ActionJS): SortAction {
    var value = Action.jsToValue(parameters);
    value.direction = parameters.direction;
    return new SortAction(value);
  }

  public direction: Direction;

  constructor(parameters: ActionValue = {}) {
    super(parameters, dummyObject);
    var direction = parameters.direction || 'ascending';
    if (direction !== SortAction.DESCENDING && direction !== SortAction.ASCENDING) {
      throw new Error(`direction must be '${SortAction.DESCENDING}' or '${SortAction.ASCENDING}'`);
    }
    this.direction = direction;
    if (!this.expression.isOp('ref')) {
      throw new Error(`must be a reference expression: ${this.expression}`);
    }
    this._ensureAction("sort");
  }

  public valueOf(): ActionValue {
    var value = super.valueOf();
    value.direction = this.direction;
    return value;
  }

  public toJS(): ActionJS {
    var js = super.toJS();
    js.direction = this.direction;
    return js;
  }

  public equals(other: SortAction): boolean {
    return super.equals(other) &&
      this.direction === other.direction;
  }

  protected _toStringParameters(expressionString: string): string[] {
    return [expressionString, this.direction];
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
    return typeContext;
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
    var direction = this.direction;
    return (d: Datum, c: Datum) => {
      var inV = inputFn(d, c);
      return inV ? inV.sort(expressionFn, direction) : null;
    };
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    var dir = this.direction === SortAction.DESCENDING ? 'DESC' : 'ASC';
    return `ORDER BY ${expressionSQL} ${dir}`;
  }

  public refName(): string {
    var expression = this.expression;
    return (expression instanceof RefExpression) ? expression.name : null;
  }

  public isNester(): boolean {
    return true;
  }

  protected _foldWithPrevAction(prevAction: Action): Action {
    if (prevAction instanceof SortAction && this.expression.equals(prevAction.expression)) {
      return this;
    }
    return null;
  }

  public toggleDirection(): SortAction {
    return new SortAction({
      expression: this.expression,
      direction: this.direction === SortAction.ASCENDING ? SortAction.DESCENDING : SortAction.ASCENDING
    });
  }

  protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
    const { expression, direction } = this;
    if (!expression.resolved()) return null;
    if (literalExpression.value === null) return Expression.NULL;
    var dataset = literalExpression.value;

    dataset = dataset.sort(expression.getFn(), direction);

    return r(dataset);
  }
}

Action.register(SortAction);
