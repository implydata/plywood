/*
 * Copyright 2015-2020 Imply Data, Inc.
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

import { ComputeFn, Dataset, Datum, PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { External } from '../external/baseExternal';
import { DatasetFullType } from '../types';

import {
  ChainableUnaryExpression,
  Expression,
  ExpressionJS,
  ExpressionValue,
  r,
} from './baseExpression';

export class ExternalExpression extends Expression {
  static op = 'external';
  static fromJS(parameters: ExpressionJS): ExternalExpression {
    const value: ExpressionValue = {
      op: parameters.op,
    };
    value.external = External.fromJS(parameters.external);
    return new ExternalExpression(value);
  }

  public external: External;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    const external = parameters.external;
    if (!external) throw new Error('must have an external');
    this.external = external;
    this._ensureOp('external');
    this.type = external.mode === 'value' ? external.getValueType() : 'DATASET'; // ToDo: not always number
    this.simple = true;
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.external = this.external;
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    js.external = this.external.toJS();
    return js;
  }

  public toString(): string {
    return `E:${this.external}`;
  }

  public getFn(): ComputeFn {
    throw new Error('should not call getFn on External');
  }

  public calc(_datum: Datum): PlywoodValue {
    throw new Error('should not call calc on External');
  }

  public getSQL(_dialect: SQLDialect): string {
    throw new Error('should not call getSQL on External');
  }

  public equals(other: ExternalExpression | undefined): boolean {
    return super.equals(other) && this.external.equals(other.external);
  }

  public updateTypeContext(typeContext: DatasetFullType): DatasetFullType {
    const { external } = this;
    if (external.mode !== 'value') {
      const newTypeContext = this.external.getFullType();
      newTypeContext.parent = typeContext;
      return newTypeContext;
    }
    return typeContext;
  }

  public unsuppress(): ExternalExpression {
    const value = this.valueOf();
    value.external = this.external.show();
    return new ExternalExpression(value);
  }

  public addExpression(expression: Expression): Expression {
    const newExternal = this.external.addExpression(expression);
    if (!newExternal) return null;

    // If the filter is false, just evaluate to an empty dataset
    if (newExternal.filter.equals(Expression.FALSE)) {
      return r(
        new Dataset({
          attributes: newExternal.attributes,
          keys: newExternal.split ? newExternal.split.keys : undefined,
          data: [],
        }),
      );
    }

    return new ExternalExpression({ external: newExternal });
  }

  public prePush(expression: ChainableUnaryExpression): ExternalExpression {
    const newExternal = this.external.prePush(expression);
    if (!newExternal) return null;
    return new ExternalExpression({ external: newExternal });
  }

  public maxPossibleSplitValues(): number {
    return Infinity;
  }
}

Expression.register(ExternalExpression);
