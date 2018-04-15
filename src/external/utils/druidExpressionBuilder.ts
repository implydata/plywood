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

import { NumberRange, Set, StringRange } from '../../datatypes';
import { TimeRange } from '../../datatypes/index';
import {
  $,
  CastExpression,
  ChainableExpression,
  ChainableUnaryExpression,
  ConcatExpression,
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
  PowerExpression,
  AbsoluteExpression,
  AndExpression,
  OrExpression,
  NotExpression,
  ThenExpression
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

  static escapeVariable(name: string): string {
    return `"${DruidExpressionBuilder.escape(name)}"`;
  }

  static escapeStringLiteral(s: string): string {
    return `'${DruidExpressionBuilder.escape(String(s))}'`;
  }

  static numberOrTime(x: number | Date): string {
    if ((x as Date).toISOString) {
      return String(x.valueOf());
    } else {
      return String(x);
    }
  }

  constructor(options: DruidExpressionBuilderOptions) {
  }

  public expressionToDruidExpression(expression: Expression): string | null {
    if (expression instanceof LiteralExpression) {
      const literalValue = expression.getLiteralValue();
      if (literalValue === null) {
        return `null`;
      } else {
        switch (typeof literalValue) {
          case 'string':
            return DruidExpressionBuilder.escapeStringLiteral(literalValue);

          case 'number':
            return String(literalValue);

          default:
            return `no_such_type`;
        }
      }

    } else if (expression instanceof RefExpression) {
      return DruidExpressionBuilder.escapeVariable(expression.name);

    } else if (expression instanceof ChainableExpression) {
      const ex1 = this.expressionToDruidExpression(expression.operand);

      if (expression instanceof CastExpression) {
        switch (expression.outputType) {
          case 'TIME': return `timestamp(${ex1})`;
          case 'STRING': return `cast(${ex1},'STRING')`;
          case 'NUMBER': return `cast(${ex1},'DOUBLE')`;
          default: throw new Error(`cast to ${expression.outputType} not implemented yet`);
        }

      } else if (expression instanceof SubstrExpression) {
        return `substring(${ex1},${expression.position},${expression.len})`;

      } else if (expression instanceof ExtractExpression) {
        return `regexp_extract(${ex1},${DruidExpressionBuilder.escapeStringLiteral(expression.regexp)},1)`;

      } else if (expression instanceof LengthExpression) {
        return `strlen(${ex1})`;

      } else if (expression instanceof NotExpression) {
        return `!${ex1}`;

      } else if (expression instanceof AbsoluteExpression) {
        return `abs(${ex1})`;

      } else if (expression instanceof TimePartExpression) {
        const format = DruidExpressionBuilder.TIME_PART_TO_FORMAT[expression.part];
        if (!format) throw new Error(`can not convert ${expression.part} to Druid expression format`);
        return `timestamp_extract(${ex1},'${format}',${DruidExpressionBuilder.escapeStringLiteral(expression.timezone.toString())})`;

      } else if (expression instanceof TimeFloorExpression || expression instanceof TimeBucketExpression) {
        return `timestamp_floor(${ex1},'${expression.duration}','',${DruidExpressionBuilder.escapeStringLiteral(expression.timezone.toString())})`;

      } else if (expression instanceof TimeShiftExpression) {
        return `timestamp_shift(${ex1},'${expression.duration}',${expression.step},${DruidExpressionBuilder.escapeStringLiteral(expression.timezone.toString())})`;

      } else if (expression instanceof LookupExpression) {
        return `lookup(${ex1},${DruidExpressionBuilder.escapeStringLiteral(expression.lookupFn)})`;

      } else if (expression instanceof TransformCaseExpression) {
        if (expression.transformType === TransformCaseExpression.UPPER_CASE) {
          return `upper(${ex1})`;
        } else {
          return `lower(${ex1})`;
        }

      } else if (expression instanceof ChainableUnaryExpression) {
        const myExpression = expression.expression;
        const ex2 = this.expressionToDruidExpression(myExpression);

        if (expression instanceof AddExpression) {
          return `(${ex1}+${ex2})`;

        } else if (expression instanceof SubtractExpression) {
          return `(${ex1}-${ex2})`;

        } else if (expression instanceof MultiplyExpression) {
          return `(${ex1}*${ex2})`;

        } else if (expression instanceof DivideExpression) {
          // Need to cast to double otherwise it might default to integer division and no one wants that
          return `(cast(${ex1},'DOUBLE')/${ex2})`;

        } else if (expression instanceof PowerExpression) {
          return `pow(${ex1},${ex2})`;

        } else if (expression instanceof ConcatExpression) {
          return `concat(${ex1},${ex2})`;

        } else if (expression instanceof ThenExpression) {
          return `if(${ex1},${ex2},'')`;

        } else if (expression instanceof FallbackExpression) {
          return `nvl(${ex1},${ex2})`;

        } else if (expression instanceof AndExpression) {
          return `(${ex1}&&${ex2})`;

        } else if (expression instanceof OrExpression) {
          return `(${ex1}||${ex2})`;

        } else if (expression instanceof IsExpression) {
          return `(${ex1}==${ex2})`;

        } else if (expression instanceof OverlapExpression) {
          let myExpressionType = myExpression.type;
          switch (myExpressionType) {
            case 'NUMBER_RANGE':
            case 'TIME_RANGE':
              if (myExpression instanceof LiteralExpression) {
                let range: (NumberRange | TimeRange) = myExpression.value;
                return this.overlapExpression(ex1, DruidExpressionBuilder.numberOrTime(range.start), DruidExpressionBuilder.numberOrTime(range.end), range.bounds);
              }
              throw new Error(`can not convert ${expression} to Druid expression`);

            case 'STRING_RANGE':
              if (myExpression instanceof LiteralExpression) {
                let stringRange: StringRange = myExpression.value;
                return this.overlapExpression(ex1, DruidExpressionBuilder.escapeStringLiteral(stringRange.start), DruidExpressionBuilder.escapeStringLiteral(stringRange.end), stringRange.bounds);
              }
              throw new Error(`can not convert ${expression} to Druid expression`);

            case 'SET/NUMBER_RANGE':
            case 'SET/TIME_RANGE':
              if (myExpression instanceof LiteralExpression) {
                let setOfRange: Set = myExpression.value;
                return setOfRange.elements.map((range: (NumberRange | TimeRange)) => {
                  return this.overlapExpression(ex1, DruidExpressionBuilder.numberOrTime(range.start), DruidExpressionBuilder.numberOrTime(range.end), range.bounds);
                }).join(' || ');
              }
              throw new Error(`can not convert ${expression} to Druid expression`);

            default:
              throw new Error(`can not convert ${expression} to Druid expression`);
          }
        }
      }
    }

    throw new Error(`not implemented Druid exprenot implemented yetssion for ${expression}`);
  }

  private overlapExpression(operand: string, start: string, end: string, bounds: string) {
    if (start === end && bounds === '[]') return `(${operand}==${start})`;
    let startExpression: string = null;
    if (start !== 'null') {
      startExpression = start + (bounds[0] === '[' ? '<=' : '<') + operand;
    }
    let endExpression: string = null;
    if (end !== 'null') {
      endExpression = operand + (bounds[1] === ']' ? '<=' : '<') + end;
    }
    if (startExpression) {
      return endExpression ? `(${startExpression} && ${endExpression})` : startExpression;
    } else {
      return endExpression ? endExpression : 'true';
    }
  }

}
