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



import { Set } from "../datatypes/set";
import { Action, ActionJS, ActionValue } from "./baseAction";
import { Expression, Indexer, Alterations } from "../expressions/baseExpression";
import { SQLDialect } from "../dialect/baseDialect";
import { Datum, ComputeFn } from "../datatypes/dataset";
import { arraysEqual } from "../helper/utils";
import { ChainExpression } from "../expressions/chainExpression";
import { LiteralExpression } from "../expressions/literalExpression";

function mergeOr(ex1: Expression, ex2: Expression): Expression {
  if (
    !ex1.isOp('chain') ||
    !ex2.isOp('chain') ||
    !(<ChainExpression>ex1).expression.isOp('ref') ||
    !(<ChainExpression>ex2).expression.isOp('ref') ||
    !arraysEqual(ex1.getFreeReferences(), ex2.getFreeReferences())
  ) return null;

  var ex1Actions = (<ChainExpression>ex1).actions;
  var ex2Actions = (<ChainExpression>ex2).actions;
  if (ex1Actions.length !== 1 || ex2Actions.length !== 1) return null;

  var firstActionExpression1 = ex1Actions[0].expression;
  var firstActionExpression2 = ex2Actions[0].expression;
  if (!firstActionExpression1.isOp('literal') || !firstActionExpression2.isOp('literal')) return null;

  var intersect = Set.generalUnion(firstActionExpression1.getLiteralValue(), firstActionExpression2.getLiteralValue());
  if (intersect === null) return null;

  return Expression.inOrIs((<ChainExpression>ex1).expression, intersect);
}

export class OrAction extends Action {
  static fromJS(parameters: ActionJS): OrAction {
    return new OrAction(Action.jsToValue(parameters));
  }

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this._ensureAction("or");
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return 'BOOLEAN';
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'BOOLEAN';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
    return inputType;
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
    return (d: Datum, c: Datum) => {
      return inputFn(d, c) || expressionFn(d, c);
    }
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string, expressionJS: string): string {
    return `(${inputJS}||${expressionJS})`;
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return `(${inputSQL} OR ${expressionSQL})`;
  }

  protected _removeAction(): boolean {
    return this.expression.equals(Expression.FALSE);
  }

  protected _nukeExpression(): Expression {
    if (this.expression.equals(Expression.TRUE)) return Expression.TRUE;
    return null;
  }

  protected _distributeAction(): Action[] {
    return this.expression.actionize(this.action);
  }

  protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
    if (literalExpression.equals(Expression.FALSE)) {
      return this.expression;
    }
    if (literalExpression.equals(Expression.TRUE)) {
      return Expression.TRUE;
    }
    return null;
  }

  protected _performOnSimpleChain(chainExpression: ChainExpression): Expression {
    var { expression } = this;

    var orExpressions = chainExpression.getExpressionPattern('or');
    if (orExpressions) {
      for (var i = 0; i < orExpressions.length; i++) {
        var orExpression = orExpressions[i];
        var mergedExpression = mergeOr(orExpression, expression);
        if (mergedExpression) {
          orExpressions[i] = mergedExpression;
          return Expression.or(orExpressions).simplify();
        }
      }
    }

    return mergeOr(chainExpression, expression);
  }
}

Action.register(OrAction);
