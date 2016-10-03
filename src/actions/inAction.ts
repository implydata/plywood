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
import { Expression, Indexer, Alterations, r } from '../expressions/baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { Datum, ComputeFn } from '../datatypes/dataset';
import { LiteralExpression } from '../expressions/literalExpression';
import { RefExpression } from '../expressions/refExpression';
import { ChainExpression } from '../expressions/chainExpression';
import { ContainsAction } from './containsAction';
import { NumberRange } from '../datatypes/numberRange';
import { IsAction } from './isAction';
import { Set } from '../datatypes/set';
import { isSetType } from '../datatypes/common';
import { TimeRange } from '../datatypes/timeRange';
import { PlywoodRange } from '../datatypes/range';
import { StringRange } from '../datatypes/stringRange';

export class InAction extends Action {
  static fromJS(parameters: ActionJS): InAction {
    return new InAction(Action.jsToValue(parameters));
  }

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this._ensureAction("in");
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return this.expression.type;
  }

  public getOutputType(inputType: PlyType): PlyType {
    var expression = this.expression;
    if (inputType) {
      if (!(
        (!isSetType(inputType) && expression.canHaveType('SET')) ||
        (inputType === 'NUMBER' && expression.canHaveType('NUMBER_RANGE')) ||
        (inputType === 'STRING' && expression.canHaveType('STRING_RANGE')) ||
        (inputType === 'TIME' && expression.canHaveType('TIME_RANGE'))
      )) {
        throw new TypeError(`in action has a bad type combination ${inputType} IN ${expression.type || '*'}`);
      }
    } else {
      if (!(expression.canHaveType('NUMBER_RANGE') || expression.canHaveType('STRING_RANGE') || expression.canHaveType('TIME_RANGE') || expression.canHaveType('SET'))) {
        throw new TypeError(`in action has invalid expression type ${expression.type}`);
      }
    }
    return 'BOOLEAN';
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
    this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
    return {
      type: 'BOOLEAN'
    };
  }

  public getUpgradedType(type: PlyType): Action {
    return this.changeExpression(this.expression.upgradeToType(type));
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
    return (d: Datum, c: Datum) => {
      var inV = inputFn(d, c);
      var exV = expressionFn(d, c);
      if (!exV) return null;
      return (<any>exV).contains(inV);
    };
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string, expressionJS: string): string {
    const { expression } = this;
    if (expression instanceof LiteralExpression) {
      switch (expression.type) {
        case 'NUMBER_RANGE':
        case 'STRING_RANGE':
        case 'TIME_RANGE':
          var range: PlywoodRange = expression.value;
          var r0 = range.start;
          var r1 = range.end;
          var bounds = range.bounds;

          var cmpStrings: string[] = [];
          if (r0 != null) {
            cmpStrings.push(`${JSON.stringify(r0)} ${bounds[0] === '(' ? '<' : '<='} _`);
          }
          if (r1 != null) {
            cmpStrings.push(`_ ${bounds[1] === ')' ? '<' : '<='} ${JSON.stringify(r1)}`);
          }

          return `(_=${inputJS}, ${cmpStrings.join(' && ')})`;

        case 'SET/STRING':
          var valueSet: Set = expression.value;
          return `${JSON.stringify(valueSet.elements)}.indexOf(${inputJS})>-1`;

        default:
          throw new Error(`can not convert ${this} to JS function, unsupported type ${expression.type}`);
      }
    }

    throw new Error(`can not convert ${this} to JS function`);
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    var expression = this.expression;
    var expressionType = expression.type;
    switch (expressionType) {
      case 'NUMBER_RANGE':
      case 'TIME_RANGE':
        if (expression instanceof LiteralExpression) {
          var range: (NumberRange | TimeRange) = expression.value;
          return dialect.inExpression(inputSQL, dialect.numberOrTimeToSQL(range.start), dialect.numberOrTimeToSQL(range.end), range.bounds);
        }
        throw new Error(`can not convert action to SQL ${this}`);

      case 'STRING_RANGE':
        if (expression instanceof LiteralExpression) {
          var stringRange: StringRange = expression.value;
          return dialect.inExpression(inputSQL, dialect.escapeLiteral(stringRange.start), dialect.escapeLiteral(stringRange.end), stringRange.bounds);
        }
        throw new Error(`can not convert action to SQL ${this}`);

      case 'SET/STRING':
      case 'SET/NUMBER':
        return `${inputSQL} IN ${expressionSQL}`;

      case 'SET/NUMBER_RANGE':
      case 'SET/TIME_RANGE':
        if (expression instanceof LiteralExpression) {
          var setOfRange: Set = expression.value;
          return setOfRange.elements.map((range: (NumberRange | TimeRange)) => {
            return dialect.inExpression(inputSQL, dialect.numberOrTimeToSQL(range.start), dialect.numberOrTimeToSQL(range.end), range.bounds);
          }).join(' OR ');
        }
        throw new Error(`can not convert action to SQL ${this}`);

      default:
        throw new Error(`can not convert action to SQL ${this}`);
    }
  }

  protected _nukeExpression(): Expression {
    var expression = this.expression;
    if (
      expression instanceof LiteralExpression &&
      isSetType(expression.type) &&
      expression.value.empty()
    ) return Expression.FALSE;
    return null;
  }

  private _performOnSimpleWhatever(ex: Expression): Expression {
    var expression = this.expression;
    var setValue: Set = expression.getLiteralValue();
    if (setValue && 'SET/' + ex.type === expression.type && setValue.size() === 1) {
      return new IsAction({ expression: r(setValue.elements[0]) }).performOnSimple(ex);
    }

    if (ex instanceof ChainExpression) {
      var indexOfAction = (ex as ChainExpression).getSingleAction('indexOf');
      var range: NumberRange = expression.getLiteralValue() as NumberRange;

      // contains could be either start less than 0 or start === 0 with inclusive bounds
      if (indexOfAction && ((range.start < 0 && range.end === null) || (range.start === 0 && range.end === null && range.bounds[0] === '['))) {
        return new ContainsAction({ expression: indexOfAction.expression }).performOnSimple((ex as ChainExpression).expression);
      }
    }

    return null;
  }

  protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
    return this._performOnSimpleWhatever(literalExpression);
  }

  protected _performOnRef(refExpression: RefExpression): Expression {
    return this._performOnSimpleWhatever(refExpression);
  }

  protected _performOnSimpleChain(chainExpression: ChainExpression): Expression {
    return this._performOnSimpleWhatever(chainExpression);
  }
}

Action.register(InAction);
