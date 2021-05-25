/*
 * Copyright 2016-2020 Imply Data, Inc.
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

import { PlywoodValue, Set } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import {
  ChainableUnaryExpression,
  Expression,
  ExpressionJS,
  ExpressionValue,
} from './baseExpression';
import { TransformCaseExpression } from './transformCaseExpression';

export class ContainsExpression extends ChainableUnaryExpression {
  static NORMAL = 'normal';
  static IGNORE_CASE = 'ignoreCase';

  static caseIndependent(str: string): boolean {
    return str.toUpperCase() === str.toLowerCase();
  }

  static op = 'Contains';
  static fromJS(parameters: ExpressionJS): ContainsExpression {
    let value = ChainableUnaryExpression.jsToValue(parameters);
    value.compare = parameters.compare;
    return new ContainsExpression(value);
  }

  public compare: string;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._checkOperandTypes('STRING');
    this._checkExpressionTypes('STRING');

    let { compare } = parameters;
    if (!compare) {
      compare = ContainsExpression.NORMAL;
    } else if (
      compare !== ContainsExpression.NORMAL &&
      compare !== ContainsExpression.IGNORE_CASE
    ) {
      throw new Error(
        `compare must be '${ContainsExpression.NORMAL}' or '${ContainsExpression.IGNORE_CASE}'`,
      );
    }
    this.compare = compare;
    this._ensureOp('contains');

    this.type = 'BOOLEAN';
  }

  public valueOf(): ExpressionValue {
    let value = super.valueOf();
    value.compare = this.compare;
    return value;
  }

  public toJS(): ExpressionJS {
    let js = super.toJS();
    js.compare = this.compare;
    return js;
  }

  public equals(other: ContainsExpression | undefined): boolean {
    return super.equals(other) && this.compare === other.compare;
  }

  protected _toStringParameters(indent?: int): string[] {
    return [this.expression.toString(indent), this.compare];
  }

  protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue {
    let fn: (a: any, b: any) => boolean;
    if (this.compare === ContainsExpression.NORMAL) {
      fn = (a: any, b: any) => String(a).indexOf(b) > -1;
    } else {
      fn = (a: any, b: any) =>
        String(a)
          .toLowerCase()
          .indexOf(String(b).toLowerCase()) > -1;
    }
    return Set.crossBinaryBoolean(operandValue, expressionValue, fn);
  }

  protected _getSQLChainableUnaryHelper(
    dialect: SQLDialect,
    operandSQL: string,
    expressionSQL: string,
  ): string {
    return dialect.containsExpression(operandSQL, expressionSQL,this.compare === ContainsExpression.IGNORE_CASE);
  }

  public changeCompare(compare: string): ContainsExpression {
    let value = this.valueOf();
    value.compare = compare;
    return new ContainsExpression(value);
  }

  public specialSimplify(): Expression {
    const { operand, expression, compare } = this;

    // X.transformCase(tt1).contains(Y.transformCase(tt2))
    if (
      operand instanceof TransformCaseExpression &&
      expression instanceof TransformCaseExpression
    ) {
      const { operand: x, transformType: tt1 } = operand;
      const { operand: y, transformType: tt2 } = expression;
      if (tt1 === tt2) {
        return x.contains(y, ContainsExpression.IGNORE_CASE);
      }
    }

    if (compare === 'ignoreCase') {
      // X.contains(CaseIndependentLiteral, ignoreCase)
      const expressionLiteral = expression.getLiteralValue();
      if (
        expressionLiteral != null &&
        ((typeof expressionLiteral === 'string' &&
          ContainsExpression.caseIndependent(expressionLiteral)) ||
          (expressionLiteral instanceof Set &&
            expressionLiteral.elements.every(ContainsExpression.caseIndependent)))
      ) {
        return this.changeCompare('normal');
      }
    }

    return this;
  }
}

Expression.register(ContainsExpression);
