/*
 * Copyright 2016-2017 Imply Data, Inc.
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

import { r, ExpressionJS, ExpressionValue, Expression, ChainableUnaryExpression } from './baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { PlyType } from '../types';
import { PlywoodValue, Set, Range, PlywoodRange, NumberRange, TimeRange, StringRange } from '../datatypes/index';
import { LiteralExpression } from './literalExpression';
import { OverlapExpression } from './overlapExpression';

export class InExpression extends ChainableUnaryExpression {
  static op = "In";
  static fromJS(parameters: ExpressionJS): InExpression {
    const value = ChainableUnaryExpression.jsToValue(parameters);

    // Back compat.
    if (Range.isRangeType(value.expression.type)) {
      console.warn('In -> Overlap');
      value.op = 'overlap';
      return (new OverlapExpression(value) as any);
    }

    return new InExpression(value);
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("in");

    let operandType = this.operand.type;
    let expression = this.expression;
    if (operandType) {
      if (!(
          operandType === 'NULL' ||
          expression.type === 'NULL' ||
          (!Set.isSetType(operandType) && expression.canHaveType('SET')) // ||
          // (operandType === 'NUMBER' && expression.canHaveType('NUMBER_RANGE')) ||
          // (operandType === 'STRING' && expression.canHaveType('STRING_RANGE')) ||
          // (operandType === 'TIME' && expression.canHaveType('TIME_RANGE'))
        )) {
        throw new TypeError(`in expression ${this} has a bad type combination ${operandType} IN ${expression.type || '*'}`);
      }
    } else {
      if (!(expression.canHaveType('NUMBER_RANGE') || expression.canHaveType('STRING_RANGE') || expression.canHaveType('TIME_RANGE') || expression.canHaveType('SET'))) {
        throw new TypeError(`in expression has invalid expression type ${expression.type}`);
      }
    }
    this.type = 'BOOLEAN';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    if (!expressionValue) return null;
    return (<any>expressionValue).contains(operandValue);
  }

  protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string {
    const { expression } = this;
    if (expression instanceof LiteralExpression) {
      switch (expression.type) {
        case 'NUMBER_RANGE':
        case 'STRING_RANGE':
        case 'TIME_RANGE':
          let range: PlywoodRange = expression.value;
          let r0 = range.start;
          let r1 = range.end;
          let bounds = range.bounds;

          let cmpStrings: string[] = [];
          if (r0 != null) {
            cmpStrings.push(`${JSON.stringify(r0)} ${bounds[0] === '(' ? '<' : '<='} _`);
          }
          if (r1 != null) {
            cmpStrings.push(`_ ${bounds[1] === ')' ? '<' : '<='} ${JSON.stringify(r1)}`);
          }

          return `((_=${operandJS}),${cmpStrings.join('&&')})`;

        default:
          throw new Error(`can not convert ${this} to JS function, unsupported type ${expression.type}`);
      }
    }

    throw new Error(`can not convert ${this} to JS function`);
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    let expression = this.expression;
    let expressionType = expression.type;
    switch (expressionType) {
      case 'NUMBER_RANGE':
      case 'TIME_RANGE':
        if (expression instanceof LiteralExpression) {
          let range: (NumberRange | TimeRange) = expression.value;
          return dialect.inExpression(operandSQL, dialect.numberOrTimeToSQL(range.start), dialect.numberOrTimeToSQL(range.end), range.bounds);
        }
        throw new Error(`can not convert action to SQL ${this}`);

      case 'STRING_RANGE':
        if (expression instanceof LiteralExpression) {
          let stringRange: StringRange = expression.value;
          return dialect.inExpression(operandSQL, dialect.escapeLiteral(stringRange.start), dialect.escapeLiteral(stringRange.end), stringRange.bounds);
        }
        throw new Error(`can not convert action to SQL ${this}`);

      case 'SET/NUMBER_RANGE':
      case 'SET/TIME_RANGE':
        if (expression instanceof LiteralExpression) {
          let setOfRange: Set = expression.value;
          return setOfRange.elements.map((range: (NumberRange | TimeRange)) => {
            return dialect.inExpression(operandSQL, dialect.numberOrTimeToSQL(range.start), dialect.numberOrTimeToSQL(range.end), range.bounds);
          }).join(' OR ');
        }
        throw new Error(`can not convert action to SQL ${this}`);

      default:
        throw new Error(`can not convert action to SQL ${this}`);
    }
  }

  public specialSimplify(): Expression {
    const { operand, expression } = this;

    // NotSet.in(Y) => NotSet.is(Y)
    if (operand.type && !Set.isSetType(operand.type)) return operand.is(expression);

    return this;
  }
}

Expression.register(InExpression);
