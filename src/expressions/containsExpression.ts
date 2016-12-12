/*
 * Copyright 2016-2016 Imply Data, Inc.
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
import { PlywoodValue } from '../datatypes/index';
import { TransformCaseExpression } from './transformCaseExpression';

export interface ContainsExpressionJS extends ExpressionJS {
  compare?: string;
}

export interface ContainsExpressionValue extends ExpressionValue {
  compare?: string;
}

export class ContainsExpression extends ChainableUnaryExpression {
  static NORMAL = 'normal';
  static IGNORE_CASE = 'ignoreCase';

  static op = "Contains";
  static fromJS(parameters: ContainsExpressionJS): ContainsExpression {
    let value = ChainableUnaryExpression.jsToValue(parameters) as ContainsExpressionValue;
    value.compare = parameters.compare;
    return new ContainsExpression(value);
  }

  public compare: string;

  constructor(parameters: ContainsExpressionValue) {
    super(parameters, dummyObject);
    this._checkOperandTypes('STRING', 'SET/STRING');
    this._checkExpressionTypes('STRING');

    let { compare } = parameters;
    if (!compare) {
      compare = ContainsExpression.NORMAL;
    } else if (compare !== ContainsExpression.NORMAL && compare !== ContainsExpression.IGNORE_CASE) {
      throw new Error(`compare must be '${ContainsExpression.NORMAL}' or '${ContainsExpression.IGNORE_CASE}'`);
    }
    this.compare = compare;
    this._ensureOp("contains");

    this.type = 'BOOLEAN';
  }

  public valueOf(): ContainsExpressionValue {
    let value = super.valueOf() as ContainsExpressionValue;
    value.compare = this.compare;
    return value;
  }

  public toJS(): ContainsExpressionJS {
    let js = super.toJS() as ContainsExpressionJS;
    js.compare = this.compare;
    return js;
  }

  public equals(other: ContainsExpression): boolean {
    return super.equals(other) &&
      this.compare === other.compare;
  }

  protected _toStringParameters(indent?: int): string[] {
    return [this.expression.toString(indent), this.compare];
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    if (this.compare === ContainsExpression.NORMAL) {
      return String(operandValue).indexOf(expressionValue) > -1;
    } else {
      return String(operandValue).toLowerCase().indexOf(String(expressionValue).toLowerCase()) > -1;
    }
  }

  protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string {
    let combine: (lhs: string, rhs: string) => string;
    if (this.compare === ContainsExpression.NORMAL) {
      combine = (lhs, rhs) => `(''+${lhs}).indexOf(${rhs})>-1`;
    } else {
      combine = (lhs, rhs) => `(''+${lhs}).toLowerCase().indexOf((''+${rhs}).toLowerCase())>-1`;
    }
    return Expression.jsNullSafetyBinary(operandJS, expressionJS, combine, operandJS[0] === '"', expressionJS[0] === '"');
  }

  protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string {
    if (this.compare === ContainsExpression.IGNORE_CASE) {
      expressionSQL = `LOWER(${expressionSQL})`;
      operandSQL = `LOWER(${operandSQL})`;
    }
    return dialect.containsExpression(expressionSQL, operandSQL);
  }

  public specialSimplify(): Expression {
    const { operand, expression } = this;

    // X.transformCase(tt1).contains(Y.transformCase(tt2))
    if (operand instanceof TransformCaseExpression && expression instanceof TransformCaseExpression) {
      const { operand: x, transformType: tt1 } = operand;
      const { operand: y, transformType: tt2 } = expression;
      if (tt1 === tt2) {
        return x.contains(y, ContainsExpression.IGNORE_CASE);
      }
    }

    return this;
  }
}

Expression.register(ContainsExpression);
