/*
 * Copyright 2020 Imply Data, Inc.
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

import { NamedArray } from 'immutable-class';

import { AttributeInfo, NumberRange, Set, StringRange } from '../../datatypes';
import { TimeRange } from '../../datatypes/index';
import {
  AbsoluteExpression,
  AddExpression,
  AndExpression,
  CastExpression,
  ChainableExpression,
  ChainableUnaryExpression,
  ConcatExpression,
  ContainsExpression,
  DivideExpression,
  Expression,
  ExtractExpression,
  FallbackExpression,
  IndexOfExpression,
  IpMatchExpression,
  IpSearchExpression,
  IpStringifyExpression,
  IsExpression,
  LengthExpression,
  LiteralExpression,
  LogExpression,
  LookupExpression,
  MatchExpression,
  MultiplyExpression,
  MvContainsExpression,
  MvOverlapExpression,
  NotExpression,
  NumberBucketExpression,
  OrExpression,
  OverlapExpression,
  PowerExpression,
  RefExpression,
  SubstrExpression,
  SubtractExpression,
  ThenExpression,
  TimeBucketExpression,
  TimeFloorExpression,
  TimePartExpression,
  TimeShiftExpression,
  TransformCaseExpression,
} from '../../expressions';
import { continuousFloorExpression } from '../../helper';
import { PlyType } from '../../types';

export interface DruidExpressionBuilderOptions {
  rawAttributes: AttributeInfo[];
  timeAttribute: string;
}

export class DruidExpressionBuilder {
  static TIME_PART_TO_FORMAT: Record<string, string> = {
    SECOND_OF_MINUTE: 'SECOND',
    MINUTE_OF_HOUR: 'MINUTE',
    HOUR_OF_DAY: 'HOUR',
    DAY_OF_WEEK: 'DOW',
    DAY_OF_MONTH: 'DAY',
    DAY_OF_YEAR: 'DOY',
    WEEK_OF_YEAR: 'WEEK',
    MONTH_OF_YEAR: 'MONTH',
    QUARTER: 'QUARTER',
    YEAR: 'YEAR',
  };

  static UNSAFE_CHAR = /[^a-z0-9 ,._\-;:(){}\[\]<>!@#$%^&*`~?]/gi;

  static escape(str: string): string {
    return str.replace(DruidExpressionBuilder.UNSAFE_CHAR, s => {
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

  static escapeLike(str: string): string {
    return str.replace(/([%_~])/g, '~$1');
  }

  static expressionTypeToOutputType(type: PlyType): Druid.OutputType {
    switch (type) {
      case 'TIME':
      case 'TIME_RANGE':
        return 'LONG';

      case 'NUMBER':
      case 'NUMBER_RANGE':
        return 'DOUBLE';

      default:
        return 'STRING';
    }
  }

  public rawAttributes: AttributeInfo[];
  public timeAttribute: string;

  constructor(options: DruidExpressionBuilderOptions) {
    this.rawAttributes = options.rawAttributes;
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

          case 'boolean':
            return String(Number(literalValue));

          case 'object':
            if (literalValue instanceof Date) {
              return DruidExpressionBuilder.escapeLiteral(literalValue);
            } else return `no_such_type`;

          default:
            return `no_such_type`;
        }
      }
    } else if (expression instanceof RefExpression) {
      if (expression.name === this.timeAttribute) {
        return '__time';
      } else {
        let exStr = DruidExpressionBuilder.escapeVariable(expression.name);

        const info = this.getAttributesInfo(expression.name);
        if (info) {
          if (info.nativeType === 'STRING') {
            if (info.type === 'TIME') {
              exStr = this.castToType(exStr, info.nativeType, info.type);
            }
          }
        }

        return exStr;
      }
    } else if (expression instanceof ChainableExpression) {
      const myOperand = expression.operand;
      const ex1 = this.expressionToDruidExpression(myOperand);

      if (expression instanceof CastExpression) {
        return this.castToType(ex1, expression.operand.type, expression.outputType);
      } else if (expression instanceof SubstrExpression) {
        return `substring(${ex1},${expression.position},${expression.len})`;
      } else if (expression instanceof ExtractExpression) {
        return `regexp_extract(${ex1},${DruidExpressionBuilder.escapeLiteral(
          expression.regexp,
        )},1)`;
      } else if (expression instanceof MatchExpression) {
        return `(regexp_extract(${ex1},${DruidExpressionBuilder.escapeLiteral(
          expression.regexp,
        )})!='')`;
      } else if (expression instanceof ContainsExpression) {
        const needle = expression.expression;
        if (needle instanceof LiteralExpression) {
          const needleValue = DruidExpressionBuilder.escape(
            DruidExpressionBuilder.escapeLike(needle.value),
          );
          if (expression.compare === ContainsExpression.IGNORE_CASE) {
            return `like(lower(${ex1}),'%${needleValue.toLowerCase()}%','~')`;
          } else {
            return `like(${ex1},'%${needleValue}%','~')`;
          }
        } else {
          throw new Error(`can not plan ${expression} into Druid`);
        }
      } else if (expression instanceof LengthExpression) {
        return `strlen(${ex1})`;
      } else if (expression instanceof NotExpression) {
        return `!${ex1}`;
      } else if (expression instanceof AbsoluteExpression) {
        return `abs(${ex1})`;
      } else if (expression instanceof NumberBucketExpression) {
        return continuousFloorExpression(ex1, 'floor', expression.size, expression.offset);
      } else if (expression instanceof TimePartExpression) {
        const format = DruidExpressionBuilder.TIME_PART_TO_FORMAT[expression.part];
        if (!format)
          throw new Error(`can not convert ${expression.part} to Druid expression format`);
        return `timestamp_extract(${ex1},'${format}',${DruidExpressionBuilder.escapeLiteral(
          expression.timezone.toString(),
        )})`;
      } else if (
        expression instanceof TimeFloorExpression ||
        expression instanceof TimeBucketExpression
      ) {
        return `timestamp_floor(${ex1},'${
          expression.duration
        }',null,${DruidExpressionBuilder.escapeLiteral(expression.timezone.toString())})`;
      } else if (expression instanceof TimeShiftExpression) {
        return `timestamp_shift(${ex1},'${expression.duration}',${
          expression.step
        },${DruidExpressionBuilder.escapeLiteral(expression.timezone.toString())})`;
      } else if (expression instanceof LookupExpression) {
        return `lookup(${ex1},${DruidExpressionBuilder.escapeLiteral(expression.lookupFn)})`;
      } else if (expression instanceof TransformCaseExpression) {
        if (expression.transformType === TransformCaseExpression.UPPER_CASE) {
          return `upper(${ex1})`;
        } else {
          return `lower(${ex1})`;
        }
      } else if (expression instanceof MvContainsExpression) {
        return `array_contains(${ex1}, [${expression.mvArray
          .map(DruidExpressionBuilder.escapeLiteral)
          .join(',')}])`;
      } else if (expression instanceof MvOverlapExpression) {
        return `array_overlap(${ex1}, [${expression.mvArray
          .map(DruidExpressionBuilder.escapeLiteral)
          .join(',')}])`;
      } else if (expression instanceof IpMatchExpression) {
        return expression.ipSearchType === 'ipPrefix'
          ? `ip_match(${expression.ipSearchString.toString()}, ${ex1})`
          : `ip_match(${ex1}, ${expression.ipSearchString.toString()})`;
      } else if (expression instanceof IpSearchExpression) {
        return expression.ipSearchType === 'ipPrefix'
          ? `ip_search(${expression.ipSearchString.toString()}, ${ex1})`
          : `ip_search(${ex1}, ${expression.ipSearchString.toString()})`;
      } else if (expression instanceof IpStringifyExpression) {
        return `ip_stringify(${ex1})`;
      } else if (expression instanceof ChainableUnaryExpression) {
        const myExpression = expression.expression;

        if (expression instanceof ConcatExpression) {
          return (
            'concat(' +
            expression
              .getExpressionList()
              .map(ex => this.expressionToDruidExpression(ex))
              .join(',') +
            ')'
          );
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
            return `if(${ex2}!=0,(cast(${ex1},'DOUBLE')/${ex2}),null)`;
          }
        } else if (expression instanceof PowerExpression) {
          return `pow(${ex1},${ex2})`;
        } else if (expression instanceof LogExpression) {
          const myLiteral = myExpression.getLiteralValue();
          if (myLiteral === Math.E) return `log(${ex1})`;
          if (myLiteral === 10) return `log10(${ex1})`;
          return `log(${ex1})/log(${ex2})`;
        } else if (expression instanceof ThenExpression) {
          return `if(${ex1},${ex2},null)`;
        } else if (expression instanceof FallbackExpression) {
          return `nvl(${ex1},${ex2})`;
        } else if (expression instanceof AndExpression) {
          return `(${ex1}&&${ex2})`;
        } else if (expression instanceof OrExpression) {
          return `(${ex1}||${ex2})`;
        } else if (expression instanceof IsExpression) {
          const myLiteral = myExpression.getLiteralValue();
          if (myLiteral instanceof Set) {
            return (
              '(' +
              myLiteral.elements
                .map(e => {
                  return `${ex1}==${DruidExpressionBuilder.escapeLiteral(e)}`;
                })
                .join('||') +
              ')'
            );
          } else {
            return `(${ex1}==${ex2})`;
          }
        } else if (expression instanceof OverlapExpression) {
          const myExpressionType = myExpression.type;
          switch (myExpressionType) {
            case 'NUMBER_RANGE':
            case 'TIME_RANGE':
              if (myExpression instanceof LiteralExpression) {
                const range: NumberRange | TimeRange = myExpression.value;
                return this.overlapExpression(
                  ex1,
                  DruidExpressionBuilder.escapeLiteral(range.start),
                  DruidExpressionBuilder.escapeLiteral(range.end),
                  range.bounds,
                );
              }
              throw new Error(`can not convert ${expression} to Druid expression`);

            case 'STRING_RANGE':
              if (myExpression instanceof LiteralExpression) {
                const stringRange: StringRange = myExpression.value;
                return this.overlapExpression(
                  ex1,
                  DruidExpressionBuilder.escapeLiteral(stringRange.start),
                  DruidExpressionBuilder.escapeLiteral(stringRange.end),
                  stringRange.bounds,
                );
              }
              throw new Error(`can not convert ${expression} to Druid expression`);

            case 'SET/NUMBER_RANGE':
            case 'SET/TIME_RANGE':
              if (myExpression instanceof LiteralExpression) {
                const setOfRange: Set = myExpression.value;
                return setOfRange.elements
                  .map((range: NumberRange | TimeRange) => {
                    return this.overlapExpression(
                      ex1,
                      DruidExpressionBuilder.escapeLiteral(range.start),
                      DruidExpressionBuilder.escapeLiteral(range.end),
                      range.bounds,
                    );
                  })
                  .join('||');
              }
              throw new Error(`can not convert ${expression} to Druid expression`);

            default:
              throw new Error(`can not convert ${expression} to Druid expression`);
          }
        } else if (expression instanceof IndexOfExpression) {
          return `strpos(${ex1},${ex2})`;
        }
      }
    }

    throw new Error(`can not convert ${expression} to Druid expression`);
  }

  private castToType(operand: string, sourceType: PlyType, destType: PlyType): string {
    switch (destType) {
      case 'TIME':
        if (sourceType === 'NUMBER') {
          return `cast(${operand},'LONG')`;
        } else {
          return `timestamp(${operand})`;
        }

      case 'STRING':
        return `cast(${operand},'STRING')`;

      case 'NUMBER':
        return `cast(${operand},'DOUBLE')`;

      default:
        throw new Error(`cast to ${destType} not implemented yet`);
    }
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

  public getAttributesInfo(attributeName: string) {
    return NamedArray.get(this.rawAttributes, attributeName);
  }
}
