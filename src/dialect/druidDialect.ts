/*
 * Copyright 2015-2019 Imply Data, Inc.
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

import { Duration, Timezone } from 'chronoshift';
import { PlyType } from '../types';
import { SQLDialect } from './baseDialect';

export class DruidDialect extends SQLDialect {
  static TIME_BUCKETING: Record<string, string> = {
    "PT1S": "second",
    "PT1M": "minute",
    "PT1H": "hour",
    "P1D":  "day",
    "P1W":  "week",
    "P1M":  "month",
    "P3M":  "quarter",
    "P1Y":  "year"
  };

  static TIME_PART_TO_FUNCTION: Record<string, string> = {
    SECOND_OF_MINUTE: "EXTRACT(SECOND FROM $$)",
    SECOND_OF_HOUR: "(EXTRACT(MINUTE FROM $$)*60+EXTRACT(SECOND FROM $$))",
    SECOND_OF_DAY: "((EXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$))*60+EXTRACT(SECOND FROM $$))",
    SECOND_OF_WEEK: "(((MOD(CAST((TIME_EXTRACT($$,'DOW')+6) AS int),7)*24)+EXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$))*60+EXTRACT(SECOND FROM $$))",
    SECOND_OF_MONTH: "((((EXTRACT(DAY FROM $$)-1)*24)+EXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$))*60+EXTRACT(SECOND FROM $$))",
    SECOND_OF_YEAR: "((((TIME_EXTRACT($$,'DOY')-1)*24)+EXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$))*60+EXTRACT(SECOND FROM $$))",

    MINUTE_OF_HOUR: "EXTRACT(MINUTE FROM $$)",
    MINUTE_OF_DAY: "EXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$)",
    MINUTE_OF_WEEK: "(MOD(CAST((TIME_EXTRACT($$,'DOW')+6) AS int),7)*24)+EXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$)",
    MINUTE_OF_MONTH: "((EXTRACT(DAY FROM $$)-1)*24)+EplyXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$)",
    MINUTE_OF_YEAR: "((TIME_EXTRACT($$,'DOY')-1)*24)+EXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$)",

    HOUR_OF_DAY: "EXTRACT(HOUR FROM $$)",
    HOUR_OF_WEEK: "(MOD(CAST((TIME_EXTRACT($$,'DOW')+6) AS int),7)*24+EXTRACT(HOUR FROM $$))",
    HOUR_OF_MONTH: "((EXTRACT(DAY FROM $$)-1)*24+EXTRACT(HOUR FROM $$))",
    HOUR_OF_YEAR: "((TIME_EXTRACT($$,'DOY')-1)*24+EXTRACT(HOUR FROM $$))",

    DAY_OF_WEEK: "MOD(CAST((TIME_EXTRACT($$,'DOW')+6) AS int),7)+1",
    DAY_OF_MONTH: "EXTRACT(DAY FROM $$)",
    DAY_OF_YEAR: "TIME_EXTRACT($$,'DOY')",

    //WEEK_OF_MONTH: ???,
    WEEK_OF_YEAR: "TIME_EXTRACT($$,'WEEK')",

    MONTH_OF_YEAR: "TIME_EXTRACT($$,'MONTH')",
    YEAR: "EXTRACT(YEAR FROM $$)"
  };

  static CAST_TO_FUNCTION: Record<string, Record<string, string>> = {
    TIME: {
      NUMBER: 'TO_TIMESTAMP($$::double precision / 1000)'
    },
    NUMBER: {
      TIME: "CAST($$ AS BIGINT)",
      STRING: "CAST($$ AS FLOAT)"
    },
    STRING: {
      NUMBER: "CAST($$ AS VARCHAR)"
    }
  };

  constructor() {
    super();
  }

  public nullConstant(): string {
    return "''";
  }

  public dateToSQLDateString(date: Date): string {
    return date.toISOString()
      .replace('T', ' ')
      .replace('Z', '')
      .replace(/\.000$/, '');
  }

  public constantGroupBy(): string {
    return "GROUP BY ''";
  }

  public timeToSQL(date: Date): string {
    if (!date) return this.nullConstant();
    return `TIMESTAMP '${this.dateToSQLDateString(date)}'`;
  }

  public concatExpression(a: string, b: string): string {
    return `(${a}||${b})`;
  }

  public containsExpression(a: string, b: string): string {
    return `POSITION(${a} IN ${b})>0`;
  }

  public coalesceExpression(a: string, b: string): string {
    return `CASE WHEN ${a}='' THEN ${b} ELSE ${a} END`;
  }

  public substrExpression(a: string, position: number, length: number): string {
    return `SUBSTRING(${a},${position + 1},${length})`;
  }

  public isNotDistinctFromExpression(a: string, b: string): string {
    return `(${a}=${b})`;
  }

  public castExpression(inputType: PlyType, operand: string, cast: string): string {
    let castFunction = DruidDialect.CAST_TO_FUNCTION[cast][inputType];
    if (!castFunction) throw new Error(`unsupported cast from ${inputType} to ${cast} in Druid dialect`);
    return castFunction.replace(/\$\$/g, operand);
  }

  public timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string {
    let bucketFormat = DruidDialect.TIME_BUCKETING[duration.toString()];
    if (!bucketFormat) throw new Error(`unsupported duration '${duration}'`);
    return `FLOOR(${operand} TO ${bucketFormat})`;
  }

  public timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string {
    return this.timeFloorExpression(operand, duration, timezone);
  }

  public timePartExpression(operand: string, part: string, timezone: Timezone): string {
    let timePartFunction = DruidDialect.TIME_PART_TO_FUNCTION[part];
    if (!timePartFunction) throw new Error(`unsupported part ${part} in Druid dialect`);
    return timePartFunction.replace(/\$\$/g, operand);
  }

  public timeShiftExpression(operand: string, duration: Duration, timezone: Timezone): string {
    // https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_date-add
    let sqlFn = "DATE_ADD("; //warpDirection > 0 ? "DATE_ADD(" : "DATE_SUB(";
    let spans = duration.valueOf();
    if (spans.week) {
      return sqlFn + operand + ", INTERVAL " + String(spans.week) + ' WEEK)';
    }
    if (spans.year || spans.month) {
      let expr = String(spans.year || 0) + "-" + String(spans.month || 0);
      operand = sqlFn + operand + ", INTERVAL '" + expr + "' YEAR_MONTH)";
    }
    if (spans.day || spans.hour || spans.minute || spans.second) {
      let expr = String(spans.day || 0) + " " + [spans.hour || 0, spans.minute || 0, spans.second || 0].join(':');
      operand = sqlFn + operand + ", INTERVAL '" + expr + "' DAY_SECOND)";
    }
    return operand;
  }

  public extractExpression(operand: string, regexp: string): string {
    return `(SELECT (REGEXP_MATCHES(${operand}, '${regexp}'))[1])`;
  }

  public indexOfExpression(str: string, substr: string): string {
    return `POSITION(${substr} IN ${str}) - 1`;
  }

}

