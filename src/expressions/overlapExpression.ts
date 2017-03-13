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
import { PlywoodValue, Set, Range, PlywoodRange, NumberRange, TimeRange, StringRange } from '../datatypes/index';
import { LiteralExpression } from './literalExpression';
import { IndexOfExpression } from './indexOfExpression';
import { SQLDialect } from '../dialect/baseDialect';

export class OverlapExpression extends ChainableUnaryExpression {
  static op = "Overlap";
  static fromJS(parameters: ExpressionJS): OverlapExpression {
    return new OverlapExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("overlap");
    let operandType = Range.unwrapRangeType(Set.unwrapSetType(this.operand.type));
    let expressionType = Range.unwrapRangeType(Set.unwrapSetType(this.expression.type));
    if (!(!operandType || operandType === 'NULL' || !expressionType || expressionType === 'NULL' || operandType === expressionType)) {
      throw new Error(`${this.op} must have matching types (are ${this.operand.type}, ${this.expression.type})`);
    }
    this.type = 'BOOLEAN';
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    return Set.crossBinaryBoolean(operandValue, expressionValue, (a, b) => {
      if (a instanceof Range) {
        return b instanceof Range ? a.intersects(b) : a.contains(b);
      } else {
        return b instanceof Range ? b.contains(a) : a === b;
      }
    });
  }

  protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string {
    const { expression } = this;
    if (expression instanceof LiteralExpression) {
      if (Range.isRangeType(expression.type)) {
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
      } else {
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

  public isCommutative(): boolean {
    return true;
  }

  protected specialSimplify(): Expression {
    const { operand, expression } = this;

    const literalValue = expression.getLiteralValue();
    if (literalValue instanceof Set) {
      // X.overlap({})
      if (literalValue.empty()) return Expression.FALSE;

      const simpleSet = literalValue.simplifyCover();
      if (simpleSet !== literalValue) {
        return operand.overlap(r(simpleSet));
      }
    }

    // NonRange.overlap(NonRange)
    if (!Range.isRangeType(operand.type) && !Range.isRangeType(expression.type)) return operand.is(expression);

    // X.indexOf(Y).overlap([start, end])
    if (operand instanceof IndexOfExpression && literalValue instanceof NumberRange) {
      const { operand: x, expression: y } = operand;
      const { start, end, bounds } = literalValue;

      // contains could be either start less than 0 or start === 0 with inclusive bounds
      if ((start < 0 && end === null) || (start === 0 && end === null && bounds[0] === '[')) {
        return x.contains(y);
      }
    }

    return this;
  }
}

Expression.register(OverlapExpression);
