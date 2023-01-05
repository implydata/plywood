/*
 * Copyright 2020-2020 Imply Data, Inc.
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

import { SqlExpression } from 'druid-query-toolkit';

import { ComputeFn, Datum, PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/index';

import { Expression, ExpressionJS, ExpressionValue } from './baseExpression';

export class SqlRefExpression extends Expression {
  static op = 'SqlRef';
  static fromJS(parameters: ExpressionJS): SqlRefExpression {
    const value: ExpressionValue = Expression.jsToValue(parameters);
    value.sql = parameters.sql;
    return new SqlRefExpression(value);
  }

  public sql: string;
  public parsedSql: SqlExpression;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('sqlRef');

    const sql = parameters.sql;
    if (typeof sql !== 'string' || sql.length === 0) {
      throw new TypeError('must have a nonempty `sql`');
    }
    this.sql = sql;
    this.type = parameters.type;

    this.simple = true;
    this.parsedSql = SqlExpression.parse(this.sql);
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.sql = this.sql;
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    js.sql = this.sql;
    return js;
  }

  public toString(): string {
    return `s$\{${this.sql}}`;
  }

  public changeSql(sql: string): SqlRefExpression {
    const value = this.valueOf();
    value.sql = sql;
    return new SqlRefExpression(value);
  }

  public getFn(): ComputeFn {
    throw new Error('can not getFn on SQL');
  }

  public calc(_datum: Datum): PlywoodValue {
    throw new Error('can not calc on SQL');
  }

  public getSQL(dialect: SQLDialect, _minimal = false): string {
    return `(${this.sql})`;
  }

  public equals(other: SqlRefExpression | undefined): boolean {
    return super.equals(other) && this.sql === other.sql;
  }
}

Expression.register(SqlRefExpression);
