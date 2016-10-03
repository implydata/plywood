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
import { Expression, Indexer, Alterations } from '../expressions/baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { Datum, ComputeFn } from '../datatypes/dataset';
import { ChainExpression } from '../expressions/chainExpression';
import { TransformCaseAction } from './transformCaseAction';

export class ContainsAction extends Action {
  static NORMAL = 'normal';
  static IGNORE_CASE = 'ignoreCase';

  static fromJS(parameters: ActionJS): ContainsAction {
    var value = Action.jsToValue(parameters);
    value.compare = parameters.compare;
    return new ContainsAction(value);
  }

  public compare: string;

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    var { compare } = parameters;
    if (!compare) {
      compare = ContainsAction.NORMAL;
    } else if (compare !== ContainsAction.NORMAL && compare !== ContainsAction.IGNORE_CASE) {
      throw new Error(`compare must be '${ContainsAction.NORMAL}' or '${ContainsAction.IGNORE_CASE}'`);
    }
    this.compare = compare;
    this._ensureAction("contains");
    this._checkExpressionTypes('STRING');
  }

  public valueOf(): ActionValue {
    var value = super.valueOf();
    value.compare = this.compare;
    return value;
  }

  public toJS(): ActionJS {
    var js = super.toJS();
    js.compare = this.compare;
    return js;
  }

  public equals(other: ContainsAction): boolean {
    return super.equals(other) &&
      this.compare === other.compare;
  }

  protected _toStringParameters(expressionString: string): string[] {
    return [expressionString, this.compare];
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return ['STRING' as PlyType, 'SET/STRING' as PlyType];
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
    if (this.compare === ContainsAction.NORMAL) {
      return (d: Datum, c: Datum) => {
        return String(inputFn(d, c)).indexOf(expressionFn(d, c)) > -1;
      };
    } else {
      return (d: Datum, c: Datum) => {
        return String(inputFn(d, c)).toLowerCase().indexOf(String(expressionFn(d, c)).toLowerCase()) > -1;
      };
    }
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string, expressionJS: string): string {
    var combine: (lhs: string, rhs: string) => string;
    if (this.compare === ContainsAction.NORMAL) {
      combine = (lhs, rhs) => `(''+${lhs}).indexOf(${rhs})>-1`;
    } else {
      combine = (lhs, rhs) => `(''+${lhs}).toLowerCase().indexOf((''+${rhs}).toLowerCase())>-1`;
    }
    return Expression.jsNullSafetyBinary(inputJS, expressionJS, combine, inputJS[0] === '"', expressionJS[0] === '"');
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    if (this.compare === ContainsAction.IGNORE_CASE) {
      expressionSQL = `LOWER(${expressionSQL})`;
      inputSQL = `LOWER(${inputSQL})`;
    }
    return dialect.containsExpression(expressionSQL, inputSQL);
  }

  protected _performOnSimpleChain(chainExpression: ChainExpression): Expression {
    var { expression } = this;
    if (expression instanceof ChainExpression) {
      var precedingAction = chainExpression.lastAction();
      var succeedingAction = expression.lastAction();
      if (precedingAction instanceof TransformCaseAction && succeedingAction instanceof TransformCaseAction) {
        if (precedingAction.transformType === succeedingAction.transformType) {
          var precedingExpression = chainExpression.expression;
          return precedingExpression.contains(expression.expression, ContainsAction.IGNORE_CASE).simplify();
        }
      }
    }
    return null;
  }
}

Action.register(ContainsAction);
