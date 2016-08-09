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

export class GreaterThanOrEqualAction extends Action {
  static fromJS(parameters: ActionJS): GreaterThanOrEqualAction {
    return new GreaterThanOrEqualAction(Action.jsToValue(parameters));
  }

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this._ensureAction("greaterThanOrEqual");
    this._checkExpressionTypes('NUMBER', 'TIME', 'STRING');
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return this.expression.type;
  }

  public getOutputType(inputType: PlyType): PlyType {
    var expressionType = this.expression.type;
    if (expressionType) this._checkInputTypes(inputType);
    return 'BOOLEAN';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
    return {
      type: 'BOOLEAN'
    };
  }

  public getUpgradedType(type: PlyType): Action {
    return this.changeExpression(this.expression.upgradeToType(type))
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
    return (d: Datum, c: Datum) => {
      return inputFn(d, c) >= expressionFn(d, c);
    }
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string, expressionJS: string): string {
    return `(${inputJS}>=${expressionJS})`;
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return `(${inputSQL}>=${expressionSQL})`;
  }

  protected _specialSimplify(simpleExpression: Expression): Action {
    if (simpleExpression instanceof LiteralExpression) { // x >= 5
      return new InAction({
        expression: new LiteralExpression({
          value: Range.fromJS({ start: simpleExpression.value, end: null, bounds: '[)' })
        })
      });
    }
    return null;
  }

  protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
    // 5 >= x
    return (new InAction({
      expression: new LiteralExpression({
        value: Range.fromJS({ start: null, end: literalExpression.value, bounds: '(]' })
      })
    })).performOnSimple(this.expression);
  }
}

Action.register(GreaterThanOrEqualAction);
