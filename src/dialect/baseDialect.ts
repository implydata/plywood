/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2017 Imply Data, Inc.
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

import { Duration, Timezone } from "chronoshift";
import { PlyType, PlyTypeSimple } from "../types";


export abstract class SQLDialect {
  private escapedTableName: string | null = null;

  constructor() {}

  public setTable(name: string | null): void {
    if (name) {
      this.escapedTableName = this.escapeName(name);
    } else {
      this.escapedTableName = null;
    }
  }


  public nullConstant(): string {
    return 'NULL';
  }

  public constantGroupBy(): string {
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
    return date.toISOString()
      .replace('T', ' ')
      .replace('Z', '')
      .replace(/\.000$/, '')
      .replace(/ 00:00:00$/, '');
  }

  public abstract timeToSQL(date: Date): string;

  public aggregateFilterIfNeeded(inputSQL: string, expressionSQL: string, elseSQL: string | null = null): string {
    let whereIndex = inputSQL.indexOf(' WHERE ');
    if (whereIndex === -1) return expressionSQL;
    let filterSQL = inputSQL.substr(whereIndex + 7);
    return this.ifThenElseExpression(filterSQL, expressionSQL, elseSQL);
  }

  public concatExpression(a: string, b: string): string {
    throw new Error('must implement');
  }

  public containsExpression(a: string, b: string): string {
    throw new Error('must implement');
  }

  public substrExpression(a: string, position: number, length: number): string {
    return `SUBSTR(${a},${position + 1},${length})`;
  }

  public coalesceExpression(a: string, b: string): string {
    return `COALESCE(${a}, ${b})`;
  }

  public ifThenElseExpression(a: string, b: string, c: string | null = null): string {
    const elsePart = c != null ? ` ELSE ${c}` : '';
    return `CASE WHEN ${a} THEN ${b}${elsePart} END`;
  }

  public isNotDistinctFromExpression(a: string, b: string): string {
    const nullConst = this.nullConstant();
    if (a === nullConst) return `${b} IS ${nullConst}`;
    if (b === nullConst) return `${a} IS ${nullConst}`;
    return `(${a} IS NOT DISTINCT FROM ${b})`;
  }

  public regexpExpression(expression: string, regexp: string): string {
    return `(${expression} REGEXP '${regexp}')`; // ToDo: escape this.regexp
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

  public abstract castExpression(inputType: PlyType, operand: string, cast: PlyTypeSimple): string

  public lengthExpression(a: string): string {
    return `CHAR_LENGTH(${a})`;
  }

  public abstract timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string

  public abstract timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string

  public abstract timePartExpression(operand: string, part: string, timezone: Timezone): string

  public abstract timeShiftExpression(operand: string, duration: Duration, timezone: Timezone): string

  public abstract extractExpression(operand: string, regexp: string): string

  public abstract indexOfExpression(str: string, substr: string): string
}

