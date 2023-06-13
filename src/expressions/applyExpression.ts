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

import { Dataset, Datum, PlywoodValue } from '../datatypes';
import { SQLDialect } from '../dialect/baseDialect';
import { indentBy } from '../helper/utils';
import { DatasetFullType } from '../types';

import {
  ChainableUnaryExpression,
  Expression,
  ExpressionJS,
  ExpressionValue,
  r,
} from './baseExpression';
import { ExternalExpression } from './externalExpression';
import { LiteralExpression } from './literalExpression';
import { RefExpression } from './refExpression';

export class ApplyExpression extends ChainableUnaryExpression {
  static op = 'Apply';
  static fromJS(parameters: ExpressionJS): ApplyExpression {
    const value = ChainableUnaryExpression.jsToValue(parameters);
    value.name = parameters.name;
    return new ApplyExpression(value);
  }

  public name: string;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this.name = parameters.name;
    this._ensureOp('apply');
    this._checkOperandTypes('DATASET');
    this.type = 'DATASET';
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.name = this.name;
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    js.name = this.name;
    return js;
  }

  public updateTypeContext(
    typeContext: DatasetFullType,
    expressionTypeContext: DatasetFullType,
  ): DatasetFullType {
    const exprType: any = this.expression.type;
    typeContext.datasetType[this.name] =
      exprType === 'DATASET' ? expressionTypeContext : { type: exprType };
    return typeContext;
  }

  protected _toStringParameters(indent?: int): string[] {
    let name = this.name;
    if (!RefExpression.SIMPLE_NAME_REGEXP.test(name)) name = JSON.stringify(name);
    return [name, this.expression.toString(indent)];
  }

  public toString(indent?: int): string {
    if (indent == null) return super.toString();
    let param: string;
    if (this.expression.type === 'DATASET') {
      param = '\n    ' + this._toStringParameters(indent + 2).join(',\n    ') + '\n  ';
    } else {
      param = this._toStringParameters(indent).join(',');
    }
    const actionStr = indentBy(`  .apply(${param})`, indent);
    return `${this.operand.toString(indent)}\n${actionStr}`;
  }

  public equals(other: ApplyExpression | undefined): boolean {
    return super.equals(other) && this.name === other.name;
  }

  public changeName(name: string): ApplyExpression {
    const value = this.valueOf();
    value.name = name;
    return new ApplyExpression(value);
  }

  protected _calcChainableUnaryHelper(operandValue: any, _expressionValue: any): PlywoodValue {
    if (!operandValue) return null;
    const { name, expression } = this;
    return (operandValue as Dataset).apply(name, expression);
  }

  protected _getSQLChainableUnaryHelper(
    dialect: SQLDialect,
    operandSQL: string,
    expressionSQL: string,
  ): string {
    return `${expressionSQL} AS ${dialect.escapeName(this.name)}`;
  }

  public isNester(): boolean {
    return true;
  }

  public fullyDefined(): boolean {
    return false;
  }

  protected specialSimplify(): Expression {
    const { name, operand, expression } = this;

    // X.apply('hello', '$hello') => X
    if (expression instanceof RefExpression && expression.name === name && expression.nest === 0) {
      return operand;
    }

    // X.apply(...).apply(...)
    if (
      expression.isAggregate() &&
      operand instanceof ApplyExpression &&
      !operand.expression.isAggregate() &&
      expression.getFreeReferences().indexOf(operand.name) === -1
    ) {
      return this.swapWithOperand();
    }

    // r(Dataset).apply(...)
    let dataset = operand.getLiteralValue();
    if (dataset instanceof Dataset && expression.resolved()) {
      // Omg mega hack:
      // Ensure that non of the free references in this expression are to be resolved with chain expressions
      const freeReferences = expression.getFreeReferences();
      const datum = dataset.data[0];
      if (
        datum &&
        freeReferences.some(freeReference => datum[freeReference] instanceof Expression)
      ) {
        return this;
      }

      dataset = dataset.applyFn(
        name,
        (d: Datum): any => {
          const simp = expression.resolve(d, 'null').simplify();
          if (simp instanceof ExternalExpression) return simp.external;
          if (simp instanceof LiteralExpression) return simp.value;
          return simp;
        },
        expression.type,
      );

      return r(dataset);
    }

    return this;
  }
}

Expression.register(ApplyExpression);
