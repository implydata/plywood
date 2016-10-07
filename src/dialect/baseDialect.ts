/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
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

import { Timezone, Duration } from 'chronoshift';
import { PlyType, PlyTypeSimple } from '../types';


export abstract class SQLDialect {
  constructor() {}

  public constantGroupBy(): string {
    return "GROUP BY ''";
  }

  public escapeName(name: string): string {
    name = name.replace(/"/g, '""');
    return '"' + name + '"';
  }

  public escapeLiteral(name: string): string {
    if (name === null) return 'NULL';
    name = name.replace(/'/g, "''");
    return "'" + name + "'";
  }

  public booleanToSQL(bool: boolean): string {
    return ('' + bool).toUpperCase();
  }

  public numberOrTimeToSQL(x: number | Date): string {
    if (x === null) return 'NULL';
    if ((x as Date).toISOString) {
      return this.timeToSQL(x as Date);
    } else {
      return this.numberToSQL(x as number);
    }
  }

  public numberToSQL(num: number): string {
    if (num === null) return 'NULL';
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

  public aggregateFilterIfNeeded(inputSQL: string, expressionSQL: string, zeroSQL = '0'): string {
    let whereIndex = inputSQL.indexOf(' WHERE ');
    if (whereIndex === -1) return expressionSQL;
    let filterSQL = inputSQL.substr(whereIndex + 7);
    return this.conditionalExpression(filterSQL, expressionSQL, zeroSQL);
  }

  public conditionalExpression(condition: string, thenPart: string, elsePart: string): string {
    return `IF(${condition},${thenPart},${elsePart})`;
  }

  public concatExpression(a: string, b: string): string {
    throw new Error('must implement');
  }

  public containsExpression(a: string, b: string): string {
    throw new Error('must implement');
  }

  public isNotDistinctFromExpression(a: string, b: string): string {
    if (a === 'NULL') return `${b} IS NULL`;
    if (b === 'NULL') return `${a} IS NULL`;
    return `(${a} IS NOT DISTINCT FROM ${b})`;
  }

  public abstract regexpExpression(expression: string, regexp: string): string

  public inExpression(operand: string, start: string, end: string, bounds: string) {
    if (start === end && bounds === '[]') return `${operand}=${start}`;
    let startSQL: string = null;
    if (start !== 'NULL') {
      startSQL = start + (bounds[0] === '[' ? '<=' : '<') + operand;
    }
    let endSQL: string = null;
    if (end !== 'NULL') {
      endSQL = operand + (bounds[1] === ']' ? '<=' : '<') + end;
    }
    if (startSQL) {
      return endSQL ? `(${startSQL} AND ${endSQL})` : startSQL;
    } else {
      return endSQL ? endSQL : 'TRUE';
    }
  }

  public abstract castExpression(inputType: PlyType, operand: string, cast: PlyTypeSimple): string

  public abstract lengthExpression(a: string): string;

  public abstract timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string

  public abstract timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string

  public abstract timePartExpression(operand: string, part: string, timezone: Timezone): string

  public abstract timeShiftExpression(operand: string, duration: Duration, timezone: Timezone): string

  public abstract extractExpression(operand: string, regexp: string): string

  public abstract indexOfExpression(str: string, substr: string): string

}

