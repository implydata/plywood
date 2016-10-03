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
import { r, Expression, Indexer, Alterations } from '../expressions/baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { Datum, ComputeFn } from '../datatypes/dataset';
import { LiteralExpression } from '../expressions/literalExpression';

export class ConcatAction extends Action {
  static fromJS(parameters: ActionJS): ConcatAction {
    return new ConcatAction(Action.jsToValue(parameters));
  }

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this._ensureAction("concat");
    this._checkExpressionTypes('STRING');
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return this._stringTransformInputType;
  }

  public getOutputType(inputType: PlyType): PlyType {
    return this._stringTransformOutputType(inputType);
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
    return inputType;
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
    return (d: Datum, c: Datum) => {
      var inV = inputFn(d, c);
      if (inV === null) return null;
      var exV = expressionFn(d, c);
      if (exV === null) return null;
      return '' + inV + exV;
    };
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string, expressionJS: string): string {
    return Expression.jsNullSafetyBinary(inputJS, expressionJS, ((a, b) => `${a}+${b}`), inputJS[0] === '"', expressionJS[0] === '"');
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return dialect.concatExpression(inputSQL, expressionSQL);
  }

  protected _removeAction(): boolean {
    return this.expression.equals(Expression.EMPTY_STRING);
  }

  protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
    if (literalExpression.equals(Expression.EMPTY_STRING)) {
      return this.expression;
    }
    return null;
  }

  protected _foldWithPrevAction(prevAction: Action): Action {
    if (prevAction instanceof ConcatAction) {
      var prevValue = prevAction.expression.getLiteralValue();
      var myValue = this.expression.getLiteralValue();
      if (typeof prevValue === 'string' && typeof myValue === 'string') {
        return new ConcatAction({
          expression: r(prevValue + myValue)
        });
      }
    }
    return null;
  }
}

Action.register(ConcatAction);
