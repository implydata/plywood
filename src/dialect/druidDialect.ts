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

import type { Duration, Timezone } from 'chronoshift';
import { NamedArray } from 'immutable-class';

import { Attributes } from '../datatypes';
import { PlyType } from '../types';

import { SQLDialect } from './baseDialect';

export interface DruidDialectOptions {
  attributes?: Attributes;
}

export class DruidDialect extends SQLDialect {
  static TIME_PART_TO_FUNCTION: Record<string, string> = {
    SECOND_OF_MINUTE: "TIME_EXTRACT($$,'SECOND',##)",
    SECOND_OF_HOUR: "(TIME_EXTRACT($$,'MINUTE',##)*60+TIME_EXTRACT($$,'SECOND',##))",
    SECOND_OF_DAY:
      "((TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##))*60+TIME_EXTRACT($$,'SECOND',##))",
    SECOND_OF_WEEK:
      "(((MOD(CAST((TIME_EXTRACT($$,'DOW',##)+6) AS int),7)*24)+TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##))*60+TIME_EXTRACT($$,'SECOND',##))",
    SECOND_OF_MONTH:
      "((((TIME_EXTRACT($$,'DAY',##)-1)*24)+TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##))*60+TIME_EXTRACT($$,'SECOND',##))",
    SECOND_OF_YEAR:
      "((((TIME_EXTRACT($$,'DOY',##)-1)*24)+TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##))*60+TIME_EXTRACT($$,'SECOND',##))",

    MINUTE_OF_HOUR: "TIME_EXTRACT($$,'MINUTE',##)",
    MINUTE_OF_DAY: "TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##)",
    MINUTE_OF_WEEK:
      "(MOD(CAST((TIME_EXTRACT($$,'DOW',##)+6) AS int),7)*24)+TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##)",
    MINUTE_OF_MONTH:
      "((TIME_EXTRACT($$,'DAY',##)-1)*24)+TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##)",
    MINUTE_OF_YEAR:
      "((TIME_EXTRACT($$,'DOY',##)-1)*24)+TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##)",

    HOUR_OF_DAY: "TIME_EXTRACT($$,'HOUR',##)",
    HOUR_OF_WEEK:
      "(MOD(CAST((TIME_EXTRACT($$,'DOW',##)+6) AS int),7)*24+TIME_EXTRACT($$,'HOUR',##))",
    HOUR_OF_MONTH: "((TIME_EXTRACT($$,'DAY',##)-1)*24+TIME_EXTRACT($$,'HOUR',##))",
    HOUR_OF_YEAR: "((TIME_EXTRACT($$,'DOY',##)-1)*24+TIME_EXTRACT($$,'HOUR',##))",

    DAY_OF_WEEK: "MOD(CAST((TIME_EXTRACT($$,'DOW',##)+6) AS int),7)+1",
    DAY_OF_MONTH: "TIME_EXTRACT($$,'DAY',##)",
    DAY_OF_YEAR: "TIME_EXTRACT($$,'DOY',##)",

    // WEEK_OF_MONTH: ???,
    WEEK_OF_YEAR: "TIME_EXTRACT($$,'WEEK',##)",

    MONTH_OF_YEAR: "TIME_EXTRACT($$,'MONTH',##)",
    YEAR: "TIME_EXTRACT($$,'YEAR',##)",
  };

  static CAST_TO_FUNCTION: Record<string, Record<string, string>> = {
    TIME: {
      NUMBER: 'MILLIS_TO_TIMESTAMP(CAST($$ AS BIGINT))',
      _: 'CAST($$ AS DATE)',
    },
    NUMBER: {
      TIME: 'CAST($$ AS BIGINT)',
      STRING: 'CAST($$ AS DOUBLE)',
      _: 'CAST($$ AS DOUBLE)',
    },
    STRING: {
      NUMBER: 'CAST($$ AS VARCHAR)',
      _: 'CAST($$ AS VARCHAR)',
    },
  };

  private readonly attributes?: Attributes;

  constructor(options: DruidDialectOptions = {}) {
    super();
    this.attributes = options.attributes;
  }

  public dateToSQLDateString(date: Date): string {
    return date
      .toISOString()
      .replace('T', ' ')
      .replace('Z', '')
      .replace(/\.000$/, '');
  }

  public floatDivision(numerator: string, denominator: string): string {
    return `(${numerator}*1.0/${denominator})`;
  }

  public emptyGroupBy(): string {
    return 'GROUP BY ()';
  }

  public timeToSQL(date: Date): string {
    if (!date) return this.nullConstant();
    return `TIMESTAMP '${this.dateToSQLDateString(date)}'`;
  }

  public stringArrayToSQL(value: string[]): string {
    const arr = value.map((v: string) => this.escapeLiteral(v));
    return `ARRAY[${arr.join(',')}]`;
  }

  public ipParse(value: string): string {
    return `IP_PARSE(${value})`;
  }

  public ipPrefixParse(value: string): string {
    return `IP_PREFIX_PARSE(${value})`;
  }

  public concatExpression(a: string, b: string): string {
    return `(${a}||${b})`;
  }

  public containsExpression(a: string, b: string, insensitive: boolean): string {
    return `${insensitive ? 'ICONTAINS_STRING' : 'CONTAINS_STRING'}(CAST(${a} AS VARCHAR),${b})`;
  }

  public mvContainsExpression(a: string, b: string[]): string {
    return `MV_CONTAINS(${a}, ${this.stringArrayToSQL(b)})`;
  }

  public mvFilterOnlyExpression(a: string, b: string[]): string {
    return `MV_FILTER_ONLY(${a}, ${this.stringArrayToSQL(b)})`;
  }

  public mvOverlapExpression(a: string, b: string[]): string {
    return `MV_OVERLAP(${a}, ${this.stringArrayToSQL(b)})`;
  }

  public substrExpression(a: string, position: number, length: number): string {
    return `SUBSTRING(${a},${position + 1},${length})`;
  }

  public countDistinctExpression(a: string, parameterAttributeName: string | undefined): string {
    const attribute = NamedArray.findByName(this.attributes || [], parameterAttributeName);
    const nativeType = attribute ? attribute.nativeType : undefined;
    switch (nativeType) {
      case 'HLLSketch':
        return `APPROX_COUNT_DISTINCT_DS_HLL(${a})`;

      case 'thetaSketch':
        return `APPROX_COUNT_DISTINCT_DS_THETA(${a})`;

      case 'hyperUnique':
        return `APPROX_COUNT_DISTINCT(${a})`;

      default:
        return `COUNT(DISTINCT ${a})`;
    }
  }

  public isNotDistinctFromExpression(a: string, b: string): string {
    const nullConst = this.nullConstant();
    if (a === nullConst) return `${b} IS ${nullConst}`;
    if (b === nullConst) return `${a} IS ${nullConst}`;
    return `(${a}=${b})`;
  }

  public castExpression(inputType: PlyType, operand: string, cast: string): string {
    if (inputType === cast) return operand;
    const castForInput = DruidDialect.CAST_TO_FUNCTION[cast];
    const castFunction = castForInput[inputType || '_'] || castForInput['_'];
    if (!castFunction) {
      throw new Error(`unsupported cast from ${inputType} to ${cast} in Druid dialect`);
    }
    return castFunction.replace(/\$\$/g, operand);
  }

  private operandAsTimestamp(operand: string): string {
    return operand.includes('__time') ? operand : `CAST(${operand} AS TIMESTAMP)`;
  }

  public timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string {
    return `TIME_FLOOR(${this.operandAsTimestamp(operand)}, ${this.escapeLiteral(
      duration.toString(),
    )}, NULL, ${this.escapeLiteral(timezone.toString())})`;
  }

  public timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string {
    return this.timeFloorExpression(operand, duration, timezone);
  }

  public timePartExpression(operand: string, part: string, timezone: Timezone): string {
    const timePartFunction = DruidDialect.TIME_PART_TO_FUNCTION[part];
    if (!timePartFunction) throw new Error(`unsupported part ${part} in Druid dialect`);
    return timePartFunction
      .replace(/\$\$/g, this.operandAsTimestamp(operand))
      .replace(/##/g, this.escapeLiteral(timezone.toString()));
  }

  public timeShiftExpression(
    operand: string,
    duration: Duration,
    step: int,
    timezone: Timezone,
  ): string {
    return `TIME_SHIFT(${this.operandAsTimestamp(operand)}, ${this.escapeLiteral(
      duration.toString(),
    )}, ${step}, ${this.escapeLiteral(timezone.toString())})`;
  }

  public extractExpression(operand: string, regexp: string): string {
    return `REGEXP_EXTRACT(CAST(${operand} AS VARCHAR), ${this.escapeLiteral(regexp)}, 1)`;
  }

  public regexpExpression(expression: string, regexp: string): string {
    return `REGEXP_LIKE(CAST(${expression} AS VARCHAR), ${this.escapeLiteral(regexp)})`;
  }

  public indexOfExpression(str: string, substr: string): string {
    return `POSITION(${substr} IN ${str}) - 1`;
  }

  public quantileExpression(
    str: string,
    quantile: number,
    parameterAttributeName: string | undefined,
  ): string {
    const attribute = NamedArray.findByName(this.attributes || [], parameterAttributeName);
    const nativeType = attribute ? attribute.nativeType : undefined;
    switch (nativeType) {
      case 'approximateHistogram':
        return `APPROX_QUANTILE(${str}, ${quantile})`;

      default:
        return `APPROX_QUANTILE_DS(${str}, ${quantile})`;
    }
  }

  public logExpression(base: string, operand: string): string {
    if (base === String(Math.E)) return `LN(${operand})`;
    if (base === '10') return `LOG10(${operand})`;
    return `LN(${operand})/LN(${base})`;
  }

  public lookupExpression(base: string, lookup: string): string {
    return `LOOKUP(${base}, ${this.escapeLiteral(lookup)})`;
  }

  public ipMatchExpression(columnName: string, searchString: string, ipSearchType: string): string {
    // TODO: remove toString hack
    return ipSearchType === 'ipPrefix'
      ? `IP_MATCH(${this.escapeLiteral(searchString.toString())}, ${columnName})`
      : `IP_MATCH(${columnName}, ${this.escapeLiteral(searchString.toString())})`;
  }

  public ipSearchExpression(
    columnName: string,
    searchString: string,
    ipSearchType: string,
  ): string {
    // TODO: remove toString hack
    return ipSearchType === 'ipPrefix'
      ? `IP_SEARCH(${this.escapeLiteral(searchString.toString())}, ${columnName})`
      : `IP_SEARCH(${this.ipParse(columnName)}, ${this.escapeLiteral(searchString.toString())})`;
  }

  public ipStringifyExpression(operand: string): string {
    return `IP_STRINGIFY(${operand})`;
  }
}
