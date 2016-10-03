/*
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
import { Expression, Indexer, Alterations } from '../expressions/baseExpression';
import { Datum, ComputeFn } from '../datatypes/dataset';
import { InAction } from './inAction';
import { LiteralExpression } from '../expressions/literalExpression';
import { RefExpression } from '../expressions/refExpression';
import { ChainExpression } from '../expressions/chainExpression';
import { unwrapSetType, wrapSetType } from '../datatypes/common';
import { Set } from '../datatypes/set';

export class OverlapAction extends Action {
  static fromJS(parameters: ActionJS): OverlapAction {
    return new OverlapAction(Action.jsToValue(parameters));
  }

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this._ensureAction("overlap");
    if (!this.expression.canHaveType('SET')) {
      throw new Error(`${this.action} must have an expression of type SET (is: ${this.expression.type})`);
    }
  }

  public getNecessaryInputTypes(): PlyType[] {
    var expressionType = this.expression.type;
    if (expressionType && expressionType !== 'NULL' && expressionType !== 'SET/NULL') {
      var setExpressionType = wrapSetType(expressionType);
      var unwrapped = unwrapSetType(setExpressionType);
      return [setExpressionType, unwrapped] as PlyType[];
    } else {
      // if it's null, accept anything
      return [
        'NULL', 'BOOLEAN', 'NUMBER', 'TIME', 'STRING', 'NUMBER_RANGE', 'TIME_RANGE', 'STRING_RANGE',
        'SET', 'SET/NULL', 'SET/BOOLEAN', 'SET/NUMBER', 'SET/TIME', 'SET/STRING',
        'SET/NUMBER_RANGE', 'SET/TIME_RANGE', 'DATASET'
      ] as PlyType[];
    }

  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
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
      var inV = inputFn(d, c);
      var exV = expressionFn(d, c);
      if (exV == null) return null;
      return Set.isSet(inV) ? inV.overlap(exV) : exV.contains(inV);
    };
  }

  //protected _getJSHelper(inputJS: string, expressionJS: string): string {
  //  return `(${inputJS}===${expressionJS})`;
  //}
  //
  //protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
  //  return `(${inputSQL}=${expressionSQL})`;
  //}

  protected _nukeExpression(): Expression {
    if (this.expression.equals(Expression.EMPTY_SET)) return Expression.FALSE;
    return null;
  }

  private _performOnSimpleWhatever(ex: Expression): Expression {
    var expression = this.expression;
    if ('SET/' + ex.type === expression.type) {
      return new InAction({ expression }).performOnSimple(ex);
    }
    return null;
  }

  protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
    var { expression } = this;
    if (!expression.isOp('literal')) return new OverlapAction({ expression: literalExpression }).performOnSimple(expression);

    return this._performOnSimpleWhatever(literalExpression);
  }

  protected _performOnRef(refExpression: RefExpression): Expression {
    return this._performOnSimpleWhatever(refExpression);
  }

  protected _performOnSimpleChain(chainExpression: ChainExpression): Expression {
    return this._performOnSimpleWhatever(chainExpression);
  }
}

Action.register(OverlapAction);
