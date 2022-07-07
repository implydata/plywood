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

import type { Duration, Timezone } from 'chronoshift';

import { Set } from '../datatypes';
import { PlyType, PlyTypeSimple } from '../types';

export abstract class SQLDialect {
  private escapedTableName: string | null = null;

  constructor() {}

  public setTable(name: string | null): void {
    if (name) {
      // If it is one char no escape is needed (kind of a hack)
      this.escapedTableName = name.length === 1 ? name : this.escapeName(name);
    } else {
      this.escapedTableName = null;
    }
  }

  public nullConstant(): string {
    return 'NULL';
  }

  public emptyGroupBy(): string {
    return "GROUP BY ''";
  }

  public escapeName(name: string): string {
    name = name.replace(/"/g, '""');
    return '"' + name + '"';
  }

  public maybeNamespacedName(name: string): string {
    const escapedName = this.escapeName(name);
    if (this.escapedTableName) {
      return this.escapedTableName + '.' + escapedName;
    } else {
      return escapedName;
    }
  }

  public escapeLiteral(name: string): string {
    if (name === null) return this.nullConstant();
    name = name.replace(/'/g, "''");
    return "'" + name + "'";
  }

  public booleanToSQL(bool: boolean): string {
    return ('' + bool).toUpperCase();
  }

  public floatDivision(numerator: string, denominator: string): string {
    return `(${numerator}/${denominator})`;
  }

  public numberOrTimeToSQL(x: number | Date): string {
    if (x === null) return this.nullConstant();
    if ((x as Date).toISOString) {
      return this.timeToSQL(x as Date);
    } else {
      return this.numberToSQL(x as number);
    }
  }

  public numberToSQL(num: number): string {
    if (num === null) return this.nullConstant();
    return '' + num;
  }

  public dateToSQLDateString(date: Date): string {
    return date
      .toISOString()
      .replace('T', ' ')
      .replace('Z', '')
      .replace(/\.000$/, '')
      .replace(/ 00:00:00$/, '');
  }

  public abstract timeToSQL(date: Date): string;

  public stringSetToSQL(_value: Set): string {
    return '<DUMMY>';
  }

  public aggregateFilterIfNeeded(
    inputSQL: string,
    expressionSQL: string,
    elseSQL: string | null = null,
  ): string {
    const whereIndex = inputSQL.indexOf(' WHERE ');
    if (whereIndex === -1) return expressionSQL;
    const filterSQL = inputSQL.substr(whereIndex + 7);
    return this.ifThenElseExpression(filterSQL, expressionSQL, elseSQL);
  }

  public concatExpression(_a: string, _b: string): string {
    throw new Error('must implement');
  }

  public containsExpression(_a: string, _b: string, _insensitive: boolean): string {
    throw new Error('must implement');
  }

  public mvContainsExpression(_a: string, _b: string): string {
    throw new Error('must implement');
  }

  public mvFilterOnlyExpression(_a: string, _b: string[]): string {
    throw new Error('must implement');
  }

  public mvOverlapExpression(_a: string, _b: string): string {
    throw new Error('must implement');
  }

  public substrExpression(a: string, position: number, length: number): string {
    return `SUBSTR(${a},${position + 1},${length})`;
  }

  public coalesceExpression(a: string, b: string): string {
    return `COALESCE(${a}, ${b})`;
  }

  public countDistinctExpression(a: string, _parameterAttributeName: string | undefined): string {
    return `COUNT(DISTINCT ${a})`;
  }

  public ifThenElseExpression(a: string, b: string, c?: string): string {
    const elsePart = typeof c === 'string' ? ` ELSE ${c}` : '';
    return `CASE WHEN ${a} THEN ${b}${elsePart} END`;
  }

  public filterAggregatorExpression(aggregate: string, whereFilter: string): string {
    const whereIndex = whereFilter.indexOf('WHERE');
    return `${aggregate}${whereIndex !== -1 ? `FILTER (${whereFilter.substr(whereIndex)})` : ''}`;
  }

  public isNotDistinctFromExpression(a: string, b: string): string {
    const nullConst = this.nullConstant();
    if (a === nullConst) return `${b} IS ${nullConst}`;
    if (b === nullConst) return `${a} IS ${nullConst}`;
    return `(${a} IS NOT DISTINCT FROM ${b})`;
  }

  public regexpExpression(expression: string, regexp: string): string {
    return `(${expression} REGEXP ${this.escapeLiteral(regexp)})`;
  }

  public inExpression(operand: string, start: string, end: string, bounds: string) {
    if (start === end && bounds === '[]') return `${operand}=${start}`;
    let startSQL: string = null;
    if (start !== this.nullConstant()) {
      startSQL = start + (bounds[0] === '[' ? '<=' : '<') + operand;
    }
    let endSQL: string = null;
    if (end !== this.nullConstant()) {
      endSQL = operand + (bounds[1] === ']' ? '<=' : '<') + end;
    }
    if (startSQL) {
      return endSQL ? `(${startSQL} AND ${endSQL})` : startSQL;
    } else {
      return endSQL ? endSQL : 'TRUE';
    }
  }

  public abstract castExpression(inputType: PlyType, operand: string, cast: PlyTypeSimple): string;

  public lengthExpression(a: string): string {
    return `CHAR_LENGTH(${a})`;
  }

  public abstract timeFloorExpression(
    operand: string,
    duration: Duration,
    timezone: Timezone,
  ): string;

  public abstract timeBucketExpression(
    operand: string,
    duration: Duration,
    timezone: Timezone,
  ): string;

  public abstract timePartExpression(operand: string, part: string, timezone: Timezone): string;

  public abstract timeShiftExpression(
    operand: string,
    duration: Duration,
    step: int,
    timezone: Timezone,
  ): string;

  public abstract extractExpression(operand: string, regexp: string): string;

  public abstract indexOfExpression(str: string, substr: string): string;

  public quantileExpression(
    _str: string,
    _quantile: number,
    _parameterAttributeName: string | undefined,
  ): string {
    throw new Error('dialect does not implement quantile');
  }

  public logExpression(base: string, operand: string): string {
    if (base === String(Math.E)) return `LN(${operand})`;
    return `LOG(${base},${operand})`;
  }

  public lookupExpression(_base: string, _lookup: string): string {
    throw new Error('can not express a lookup as a function');
  }
}
