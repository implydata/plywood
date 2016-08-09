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


import { dummyObject } from '../helper/dummy';
import { Action, ActionJS, ActionValue } from './baseAction';
import { Expression, Indexer, Alterations } from '../expressions/baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { Datum, ComputeFn } from '../datatypes/dataset';

export class MultiplyAction extends Action {
  static fromJS(parameters: ActionJS): MultiplyAction {
    return new MultiplyAction(Action.jsToValue(parameters));
  }

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this._ensureAction("multiply");
    this._checkExpressionTypes('NUMBER');
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return 'NUMBER';
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'NUMBER';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
    return inputType;
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
    return (d: Datum, c: Datum) => {
      return (inputFn(d, c) || 0) * (expressionFn(d, c) || 0);
    }
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string, expressionJS: string): string {
    return `(${inputJS}*${expressionJS})`;
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return `(${inputSQL}*${expressionSQL})`;
  }

  protected _removeAction(): boolean {
    return this.expression.equals(Expression.ONE);
  }

  protected _nukeExpression(): Expression {
    if (this.expression.equals(Expression.ZERO)) return Expression.ZERO;
    return null;
  }

  protected _distributeAction(): Action[] {
    return this.expression.actionize(this.action);
  }

  protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
    if (literalExpression.equals(Expression.ONE)) {
      return this.expression;
    } else if (literalExpression.equals(Expression.ZERO)) {
      return Expression.ZERO;
    }
    return null;
  }
}

Action.register(MultiplyAction);
