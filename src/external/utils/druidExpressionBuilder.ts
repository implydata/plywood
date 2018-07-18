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
  MatchExpression,
  AddExpression,
  SubtractExpression,
  DivideExpression,
  TimeShiftExpression,
  PowerExpression,
  LogExpression,
  AbsoluteExpression,
  AndExpression,
  OrExpression,
  NotExpression,
  ThenExpression
} from '../../expressions';
import { continuousFloorExpression } from '../../helper';
import { PlyType } from '../../types';
import { External } from '../baseExternal';

export interface DruidExpressionBuilderOptions {
  version: string;
  timeAttribute: string;
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

  static escapeLiteral(x: number | string | Date): string {
    if (x == null) return 'null';
    if ((x as Date).toISOString) {
      return String(x.valueOf());
    } else if (typeof x === 'number') {
      return String(x);
    } else {
      return `'${DruidExpressionBuilder.escape(String(x))}'`;
    }
  }

  static expressionTypeToOutputType(type: PlyType): Druid.OutputType {
    switch (type) {
      case 'TIME':
      case 'TIME_RANGE':
        return 'LONG';

      case 'NUMBER':
      case 'NUMBER_RANGE':
        return 'FLOAT'; // 'DOUBLE'?

      default:
        return 'STRING';
    }
  }

  public version: string;
  public timeAttribute: string;

  constructor(options: DruidExpressionBuilderOptions) {
    this.version = options.version;
    this.timeAttribute = options.timeAttribute;
  }

  public expressionToDruidExpression(expression: Expression): string | null {
    if (expression instanceof LiteralExpression) {
      const literalValue = expression.getLiteralValue();
      if (literalValue === null) {
        return `null`;
      } else {
        switch (typeof literalValue) {
          case 'string':
            return DruidExpressionBuilder.escapeLiteral(literalValue);

          case 'number':
            return String(literalValue);

          default:
            return `no_such_type`;
        }
      }

    } else if (expression instanceof RefExpression) {
      if (expression.name === this.timeAttribute) {
        return '__time';
      } else {
        return DruidExpressionBuilder.escapeVariable(expression.name);
      }

    } else if (expression instanceof ChainableExpression) {
      const myOperand = expression.operand;
      const ex1 = this.expressionToDruidExpression(myOperand);

      if (expression instanceof CastExpression) {
        switch (expression.outputType) {
          case 'TIME': return `timestamp(${ex1})`;
          case 'STRING': return `cast(${ex1},'STRING')`;
          case 'NUMBER': return `cast(${ex1},'DOUBLE')`;
          default: throw new Error(`cast to ${expression.outputType} not implemented yet`);
        }

      } else if (expression instanceof SubstrExpression) {
        this.checkDruid11('substring');
        return `substring(${ex1},${expression.position},${expression.len})`;

      } else if (expression instanceof ExtractExpression) {
        this.checkDruid11('regexp_extract');
        return `regexp_extract(${ex1},${DruidExpressionBuilder.escapeLiteral(expression.regexp)},1)`;

      } else if (expression instanceof MatchExpression) {
        this.checkDruid11('regexp_extract');
        return `(regexp_extract(${ex1},${DruidExpressionBuilder.escapeLiteral(expression.regexp)})!='')`;

      } else if (expression instanceof LengthExpression) {
        this.checkDruid11('strlen');
        return `strlen(${ex1})`;

      } else if (expression instanceof NotExpression) {
        return `!${ex1}`;

      } else if (expression instanceof AbsoluteExpression) {
        return `abs(${ex1})`;

      } else if (expression instanceof NumberBucketExpression) {
        return continuousFloorExpression(ex1, 'floor', expression.size, expression.offset);

      } else if (expression instanceof TimePartExpression) {
        this.checkDruid11('timestamp_extract');
        const format = DruidExpressionBuilder.TIME_PART_TO_FORMAT[expression.part];
        if (!format) throw new Error(`can not convert ${expression.part} to Druid expression format`);
        return `timestamp_extract(${ex1},'${format}',${DruidExpressionBuilder.escapeLiteral(expression.timezone.toString())})`;

      } else if (expression instanceof TimeFloorExpression || expression instanceof TimeBucketExpression) {
        this.checkDruid11('timestamp_floor');
        return `timestamp_floor(${ex1},'${expression.duration}','',${DruidExpressionBuilder.escapeLiteral(expression.timezone.toString())})`;

      } else if (expression instanceof TimeShiftExpression) {
        this.checkDruid11('timestamp_shift');
        return `timestamp_shift(${ex1},'${expression.duration}',${expression.step},${DruidExpressionBuilder.escapeLiteral(expression.timezone.toString())})`;

      } else if (expression instanceof LookupExpression) {
        this.checkDruid11('timestamp_lookup');
        return `lookup(${ex1},${DruidExpressionBuilder.escapeLiteral(expression.lookupFn)})`;

      } else if (expression instanceof TransformCaseExpression) {
        if (expression.transformType === TransformCaseExpression.UPPER_CASE) {
          this.checkDruid11('upper');
          return `upper(${ex1})`;
        } else {
          this.checkDruid11('lower');
          return `lower(${ex1})`;
        }

      } else if (expression instanceof ChainableUnaryExpression) {
        const myExpression = expression.expression;

        if (expression instanceof ConcatExpression) {
          this.checkDruid11('concat');
          return 'concat(' + expression.getExpressionList().map(ex => this.expressionToDruidExpression(ex)).join(',') + ')';
        }

        const ex2 = this.expressionToDruidExpression(myExpression);

        if (expression instanceof AddExpression) {
          return `(${ex1}+${ex2})`;

        } else if (expression instanceof SubtractExpression) {
          return `(${ex1}-${ex2})`;

        } else if (expression instanceof MultiplyExpression) {
          return `(${ex1}*${ex2})`;

        } else if (expression instanceof DivideExpression) {
          // Need to cast to double otherwise it might default to integer division and no one wants that
          if (myExpression instanceof LiteralExpression) {
            return `(cast(${ex1},'DOUBLE')/${ex2})`;
          } else {
            return `if(${ex2}!=0,(cast(${ex1},'DOUBLE')/${ex2}),0)`;
          }

        } else if (expression instanceof PowerExpression) {
          return `pow(${ex1},${ex2})`;

        } else if (expression instanceof LogExpression) {
          const myLiteral = myExpression.getLiteralValue();
          if (myLiteral === Math.E) return `log(${ex1})`;
          if (myLiteral === 10) return `log10(${ex1})`;
          return `log(${ex1})/log(${ex2})`;

        } else if (expression instanceof ThenExpression) {
          return `if(${ex1},${ex2},'')`;

        } else if (expression instanceof FallbackExpression) {
          return `nvl(${ex1},${ex2})`;

        } else if (expression instanceof AndExpression) {
          return `(${ex1}&&${ex2})`;

        } else if (expression instanceof OrExpression) {
          return `(${ex1}||${ex2})`;

        } else if (expression instanceof IsExpression) {
          const myLiteral = myExpression.getLiteralValue();
          if (myLiteral instanceof Set) {
            return '(' + myLiteral.elements.map(e => {
              return `${ex1}==${DruidExpressionBuilder.escapeLiteral(e)}`;
            }).join('||') + ')';
          } else {
            return `(${ex1}==${ex2})`;
          }

        } else if (expression instanceof OverlapExpression) {
          let myExpressionType = myExpression.type;
          switch (myExpressionType) {
            case 'NUMBER_RANGE':
            case 'TIME_RANGE':
              if (myExpression instanceof LiteralExpression) {
                let range: (NumberRange | TimeRange) = myExpression.value;
                return this.overlapExpression(ex1, DruidExpressionBuilder.escapeLiteral(range.start), DruidExpressionBuilder.escapeLiteral(range.end), range.bounds);
              }
              throw new Error(`can not convert ${expression} to Druid expression`);

            case 'STRING_RANGE':
              if (myExpression instanceof LiteralExpression) {
                let stringRange: StringRange = myExpression.value;
                return this.overlapExpression(ex1, DruidExpressionBuilder.escapeLiteral(stringRange.start), DruidExpressionBuilder.escapeLiteral(stringRange.end), stringRange.bounds);
              }
              throw new Error(`can not convert ${expression} to Druid expression`);

            case 'SET/NUMBER_RANGE':
            case 'SET/TIME_RANGE':
              if (myExpression instanceof LiteralExpression) {
                let setOfRange: Set = myExpression.value;
                return setOfRange.elements.map((range: (NumberRange | TimeRange)) => {
                  return this.overlapExpression(ex1, DruidExpressionBuilder.escapeLiteral(range.start), DruidExpressionBuilder.escapeLiteral(range.end), range.bounds);
                }).join('||');
              }
              throw new Error(`can not convert ${expression} to Druid expression`);

            default:
              throw new Error(`can not convert ${expression} to Druid expression`);
          }
        }
      }
    }

    throw new Error(`can not convert ${expression} to Druid expression`);
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

  private checkDruid11(expr: string): void {
    if (this.versionBefore('0.11.0')) {
      throw new Error(`expression '${expr}' requires Druid 0.11.0 or newer`);
    }
  }

  private versionBefore(neededVersion: string): boolean {
    const { version } = this;
    return version && External.versionLessThan(version, neededVersion);
  }
}
