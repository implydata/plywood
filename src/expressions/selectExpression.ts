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

import { Dataset, PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { DatasetFullType } from '../types';

import { ApplyExpression } from './applyExpression';
import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';

export class SelectExpression extends ChainableExpression {
  static op = 'Select';
  static fromJS(parameters: ExpressionJS): SelectExpression {
    const value = ChainableExpression.jsToValue(parameters);
    value.attributes = parameters.attributes;
    return new SelectExpression(value);
  }

  public attributes: string[];

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('select');
    this._checkOperandTypes('DATASET');
    this.attributes = parameters.attributes;
    this.type = 'DATASET';
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.attributes = this.attributes;
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    js.attributes = this.attributes;
    return js;
  }

  public equals(other: SelectExpression | undefined): boolean {
    return super.equals(other) && String(this.attributes) === String(other.attributes);
  }

  protected _toStringParameters(_indent?: int): string[] {
    return this.attributes;
  }

  public updateTypeContext(typeContext: DatasetFullType): DatasetFullType {
    const { attributes } = this;
    const { datasetType, parent } = typeContext;
    const newDatasetType = Object.create(null);
    for (const attr of attributes) {
      const attrType = datasetType[attr];
      if (!attrType) throw new Error(`unknown attribute '${attr}' in select`);
      newDatasetType[attr] = attrType;
    }
    return {
      type: 'DATASET',
      datasetType: newDatasetType,
      parent,
    };
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    return operandValue ? (operandValue as Dataset).select(this.attributes) : null;
  }

  protected _getSQLChainableHelper(_dialect: SQLDialect, _operandSQL: string): string {
    throw new Error('can not be expressed as SQL directly');
  }

  public specialSimplify(): Expression {
    const { operand, attributes } = this;

    // X.select(attr).select()
    if (operand instanceof SelectExpression) {
      const { operand: x, attributes: attr } = operand;
      return x.select(attr.filter(a => attributes.indexOf(a) !== -1));

      // X.apply('foo', _).select(<not foo>)
    } else if (operand instanceof ApplyExpression) {
      const { operand: x, name } = operand;
      if (attributes.indexOf(name) === -1) {
        return this.changeOperand(x);
      }
    }

    return this;
  }
}

Expression.register(SelectExpression);
