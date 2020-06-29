/*
 * Copyright 2012-2015 Metamarkets Group Inc.
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

import { parseISODate } from 'chronoshift';
import * as hasOwnProp from 'has-own-prop';
import { isImmutableClass } from 'immutable-class';
import { getValueType, valueFromJS } from '../datatypes/common';
import { ComputeFn, Dataset, Datum, PlywoodValue, Set, TimeRange } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { DatasetFullType, PlyType } from '../types';
import { Expression, ExpressionJS, ExpressionValue, r } from './baseExpression';

export class LiteralExpression extends Expression {
  static op = 'Literal';
  static fromJS(parameters: ExpressionJS): LiteralExpression {
    let value: ExpressionValue = {
      op: parameters.op,
      type: parameters.type,
    };
    if (!hasOwnProp(parameters, 'value')) throw new Error('literal expression must have value');
    let v: any = parameters.value;
    if (isImmutableClass(v)) {
      value.value = v;
    } else {
      value.value = valueFromJS(v, parameters.type);
    }
    return new LiteralExpression(value);
  }

  public value: any;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    let value = parameters.value;
    this.value = value;
    this._ensureOp('literal');
    if (typeof this.value === 'undefined') {
      throw new TypeError('must have a `value`');
    }
    this.type = getValueType(value);
    this.simple = true;
  }

  public valueOf(): ExpressionValue {
    let value = super.valueOf();
    value.value = this.value;
    if (this.type) value.type = this.type;
    return value;
  }

  public toJS(): ExpressionJS {
    let js = super.toJS();
    if (this.value && this.value.toJS) {
      js.value = this.value.toJS();
      js.type = Set.isSetType(this.type) ? 'SET' : this.type;
    } else {
      js.value = this.value;
      if (this.type === 'TIME') js.type = 'TIME';
    }
    return js;
  }

  public toString(): string {
    let value = this.value;
    if (value instanceof Dataset && value.basis()) {
      return 'ply()';
    } else if (this.type === 'STRING') {
      return JSON.stringify(value);
    } else {
      return String(value);
    }
  }

  public getFn(): ComputeFn {
    let value = this.value;
    return () => value;
  }

  public calc(datum: Datum): PlywoodValue {
    return this.value;
  }

  public getJS(datumVar: string): string {
    return JSON.stringify(this.value); // ToDo: what to do with higher objects?
  }

  public getSQL(dialect: SQLDialect): string {
    let value = this.value;
    if (value === null) return dialect.nullConstant();

    switch (this.type) {
      case 'STRING':
        return dialect.escapeLiteral(value);

      case 'BOOLEAN':
        return dialect.booleanToSQL(value);

      case 'NUMBER':
        return dialect.numberToSQL(value);

      case 'NUMBER_RANGE':
        return `${dialect.numberToSQL(value.start)}`;

      case 'TIME':
        return dialect.timeToSQL(<Date>value);

      case 'TIME_RANGE':
        return `${dialect.timeToSQL(value.start)}`;

      case 'STRING_RANGE':
        return dialect.escapeLiteral(value.start);

      case 'SET/STRING':
      case 'SET/NUMBER':
      case 'SET/NUMBER_RANGE':
      case 'SET/TIME_RANGE':
        return '<DUMMY>';

      default:
        throw new Error('currently unsupported type: ' + this.type);
    }
  }

  public equals(other: LiteralExpression | undefined): boolean {
    if (!super.equals(other) || this.type !== other.type) return false;
    if (this.value && this.type !== 'DATASET') {
      // ToDo: make dataset equals work
      if (this.value.equals) {
        return this.value.equals(other.value);
      } else if (this.value.toISOString && other.value.toISOString) {
        return this.value.valueOf() === other.value.valueOf();
      } else {
        return this.value === other.value;
      }
    } else {
      return this.value === other.value;
    }
  }

  public updateTypeContext(typeContext: DatasetFullType): DatasetFullType {
    const { value } = this;
    if (value instanceof Dataset) {
      let newTypeContext = value.getFullType();
      newTypeContext.parent = typeContext;
      return newTypeContext;
    }
    return typeContext;
  }

  public getLiteralValue(): any {
    return this.value;
  }

  public maxPossibleSplitValues(): number {
    const { value } = this;
    return value instanceof Set ? value.size() : 1;
  }

  public upgradeToType(targetType: PlyType): Expression {
    const { type, value } = this;
    if (type === targetType) return this;

    if (type === 'STRING' && targetType === 'TIME') {
      let parse = parseISODate(value, Expression.defaultParserTimezone);
      if (!parse) throw new Error(`can not upgrade ${value} to TIME`);
      return r(parse);
    } else if (type === 'STRING_RANGE' && targetType === 'TIME_RANGE') {
      let parseStart = parseISODate(value.start, Expression.defaultParserTimezone);
      if (!parseStart) throw new Error(`can not upgrade ${value.start} to TIME`);

      let parseEnd = parseISODate(value.end, Expression.defaultParserTimezone);
      if (!parseEnd) throw new Error(`can not upgrade ${value.end} to TIME`);

      return r(
        TimeRange.fromJS({
          start: parseStart,
          end: parseEnd,
          bounds: '[]',
        }),
      );
    }

    throw new Error(`can not upgrade ${type} to ${targetType}`);
  }
}

Expression.NULL = new LiteralExpression({ value: null });
Expression.ZERO = new LiteralExpression({ value: 0 });
Expression.ONE = new LiteralExpression({ value: 1 });
Expression.FALSE = new LiteralExpression({ value: false });
Expression.TRUE = new LiteralExpression({ value: true });
Expression.EMPTY_STRING = new LiteralExpression({ value: '' });
Expression.EMPTY_SET = new LiteralExpression({ value: Set.fromJS([]) });

Expression.register(LiteralExpression);
