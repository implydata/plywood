/*
 * Copyright 2018 Imply Data, Inc.
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

import { TimeRange } from '../../datatypes/index';
import {
  $,
  CastExpression,
  ChainableExpression,
  ChainableUnaryExpression,
  ConcatExpression,
  CustomTransformExpression,
  Expression,
  IsExpression,
  ExtractExpression,
  FallbackExpression,
  LengthExpression,
  LiteralExpression,
  LookupExpression,
  NumberBucketExpression,
  OverlapExpression,
  r,
  RefExpression,
  SubstrExpression,
  TimeBucketExpression,
  TimeFloorExpression,
  TimePartExpression,
  TransformCaseExpression,
  MultiplyExpression,
  AddExpression,
  SubtractExpression,
  DivideExpression,
  TimeShiftExpression,
  PowerExpression, AbsoluteExpression
} from '../../expressions/index';

export interface DruidExpressionBuilderOptions {
}

export class DruidExpressionBuilder {
  static TIME_PART_TO_FORMAT: Record<string, string> = {
    SECOND_OF_MINUTE: "SECOND",
    MINUTE_OF_HOUR: "MINUTE",
    HOUR_OF_DAY: "HOUR",
    DAY_OF_WEEK: "DOW",
    DAY_OF_MONTH: "DAY",
    DAY_OF_YEAR: "DOY",
    WEEK_OF_YEAR: "WEEK",
    MONTH_OF_YEAR: "MONTH",
    YEAR: "YEAR"
  };

  static UNSAFE_CHAR = /[^a-z0-9 ,._\-;:(){}\[\]<>!@#$%^&*`~?]/ig;

  static escape(str: string): string {
    return str.replace(DruidExpressionBuilder.UNSAFE_CHAR, (s) => {
      return '\\u' + ('000' + s.charCodeAt(0).toString(16)).substr(-4);
    });
  }

  constructor(options: DruidExpressionBuilderOptions) {
  }

  public expressionToDruidExpression(expression: Expression): string | null {
    if (expression instanceof LiteralExpression) {
      const literalValue = expression.getLiteralValue();
      if (literalValue === null) {
        return `null`;
      } else if (typeof literalValue === 'number') {
        return String(literalValue);
      } else {
        return `'${DruidExpressionBuilder.escape(String(literalValue))}'`;
      }

    } else if (expression instanceof RefExpression) {
      return `"${DruidExpressionBuilder.escape(expression.name)}"`;

    } else if (expression instanceof ChainableExpression) {
      const ex1 = this.expressionToDruidExpression(expression.operand);

      if (expression instanceof CastExpression) {
        if (expression.outputType === 'TIME') {
          return `timestamp(${ex1})`;
        } else {
          throw new Error(`cast to ${expression.outputType} not implemented yet`);
        }

      } else if (expression instanceof SubstrExpression) {
        return `substring(${ex1},${expression.position},${expression.len})`;

      } else if (expression instanceof ExtractExpression) {
        return `regexp_extract(${ex1},'${DruidExpressionBuilder.escape(expression.regexp)}',1)`;

      } else if (expression instanceof LengthExpression) {
        return `strlen(${ex1})`;

      } else if (expression instanceof AbsoluteExpression) {
        return `abs(${ex1})`;

      } else if (expression instanceof TimePartExpression) {
        const format = DruidExpressionBuilder.TIME_PART_TO_FORMAT[expression.part];
        if (!format) throw new Error(`can not convert ${expression.part} to Druid expression format`);
        return `timestamp_extract(${ex1},'${format}','${DruidExpressionBuilder.escape(expression.timezone.toString())}')`;

      } else if (expression instanceof TimeFloorExpression || expression instanceof TimeBucketExpression) {
        return `timestamp_floor(${ex1},'${expression.duration}','','${DruidExpressionBuilder.escape(expression.timezone.toString())}')`;

      } else if (expression instanceof TimeShiftExpression) {
        return `timestamp_shift(${ex1},'${expression.duration}',${expression.step},'${DruidExpressionBuilder.escape(expression.timezone.toString())}')`;

      } else if (expression instanceof LookupExpression) {
        return `lookup(${ex1},'${DruidExpressionBuilder.escape(expression.lookupFn)}')`;

      } else if (expression instanceof TransformCaseExpression) {
        if (expression.transformType === TransformCaseExpression.UPPER_CASE) {
          return `upper(${ex1})`;
        } else {
          return `lower(${ex1})`;
        }

      } else if (expression instanceof ChainableUnaryExpression) {
        const ex2 = this.expressionToDruidExpression(expression.expression);

        if (expression instanceof AddExpression) {
          return `(${ex1}+${ex2})`;

        } else if (expression instanceof SubtractExpression) {
          return `(${ex1}-${ex2})`;

        } else if (expression instanceof MultiplyExpression) {
          return `(${ex1}*${ex2})`;

        } else if (expression instanceof DivideExpression) {
          return `(${ex1}/${ex2})`;

        } else if (expression instanceof PowerExpression) {
          return `pow(${ex1},${ex2})`;

        } else if (expression instanceof ConcatExpression) {
          return `concat(${ex1},${ex2})`;

        } else if (expression instanceof OverlapExpression || expression instanceof IsExpression) {
          return `(${ex1}==${ex2})`;

        }
      }
    }

    throw new Error(`not implemented Druid expression for ${expression}`);
  }

}
