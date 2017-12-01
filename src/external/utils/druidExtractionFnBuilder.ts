/*
 * Copyright 2016-2017 Imply Data, Inc.
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
  ChainableUnaryExpression,
  ConcatExpression,
  CustomTransformExpression,
  Expression,
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
  TransformCaseExpression
} from '../../expressions/index';

import { External } from '../baseExternal';
import { CustomDruidTransforms } from './druidTypes';


export interface DruidExtractionFnBuilderOptions {
  version: string;
  customTransforms: CustomDruidTransforms;
}

export class DruidExtractionFnBuilder {

  static SPAN_TO_FLOOR_FORMAT: Record<string, string> = {
    second: "yyyy-MM-dd'T'HH:mm:ss'Z",
    minute: "yyyy-MM-dd'T'HH:mm'Z",
    hour: "yyyy-MM-dd'T'HH':00'Z",
    day: "yyyy-MM-dd'T00:00'Z",
    month: "yyyy-MM'-01T00:00'Z",
    year: "yyyy'-01-01T00:00'Z"
  };

  static SPAN_TO_PROPERTY: Record<string, string> = {
    second: 'secondOfMinute',
    minute: 'minuteOfHour',
    hour: 'hourOfDay',
    day: 'dayOfMonth',
    week: 'weekOfWeekyear',
    month: 'monthOfYear',
    year: 'yearOfEra'
  };

  static CASE_TO_DRUID: Record<string, string> = {
    upperCase: 'upper',
    lowerCase: 'lower'
  };

  static TIME_PART_TO_FORMAT: Record<string, string> = {
    SECOND_OF_MINUTE: "s",
    MINUTE_OF_HOUR: "m",
    HOUR_OF_DAY: "H",
    DAY_OF_WEEK: "e",
    DAY_OF_MONTH: "d",
    DAY_OF_YEAR: "D",
    WEEK_OF_YEAR: "w",
    MONTH_OF_YEAR: "M",
    YEAR: "Y"
  };

  static TIME_PART_TO_EXPR: Record<string, string> = {
    SECOND_OF_MINUTE: "d.getSecondOfMinute()",
    SECOND_OF_HOUR: "d.getSecondOfHour()",
    SECOND_OF_DAY: "d.getSecondOfDay()",
    SECOND_OF_WEEK: "d.getDayOfWeek()*86400 + d.getSecondOfMinute()",
    SECOND_OF_MONTH: "d.getDayOfMonth()*86400 + d.getSecondOfHour()",
    SECOND_OF_YEAR: "d.getDayOfYear()*86400 + d.getSecondOfDay()",

    MINUTE_OF_HOUR: "d.getMinuteOfHour()",
    MINUTE_OF_DAY: "d.getMinuteOfDay()",
    MINUTE_OF_WEEK: "d.getDayOfWeek()*1440 + d.getMinuteOfDay()",
    MINUTE_OF_MONTH: "d.getDayOfMonth()*1440 + d.getMinuteOfDay()",
    MINUTE_OF_YEAR: "d.getDayOfYear()*1440 + d.getMinuteOfDay()",

    HOUR_OF_DAY: "d.getHourOfDay()",
    HOUR_OF_WEEK: "d.getDayOfWeek()*24 + d.getHourOfDay()",
    HOUR_OF_MONTH: "d.getDayOfMonth()*24 + d.getHourOfDay()",
    HOUR_OF_YEAR: "d.getDayOfYear()*24 + d.getHourOfDay()",

    DAY_OF_WEEK: "d.getDayOfWeek()",
    DAY_OF_MONTH: "d.getDayOfMonth()",
    DAY_OF_YEAR: "d.getDayOfYear()",

    WEEK_OF_YEAR: "d.getWeekOfWeekyear()",

    MONTH_OF_YEAR: "d.getMonthOfYear()",
    YEAR: "d.getYearOfEra()",
    QUARTER: "Math.ceil((d.getMonthOfYear()) / 3)"
  };

  static composeFns(f: Druid.ExtractionFn | null, g: Druid.ExtractionFn | null): Druid.ExtractionFn | null {
    if (!f || !g) return f || g;
    return {
      type: 'cascade',
      extractionFns: [].concat(
        (f.type === 'cascade' ? f.extractionFns : f),
        (g.type === 'cascade' ? g.extractionFns : g)
      )
    };
  }

  static getLastFn(fn: Druid.ExtractionFn): Druid.ExtractionFn {
    if (fn && fn.type === 'cascade') {
      const { extractionFns } = fn;
      return extractionFns[extractionFns.length - 1];
    } else {
      return fn;
    }
  }

  static wrapFunctionTryCatch(lines: string[]): string {
    return 'function(s){try{\n' + lines.filter(Boolean).join('\n') + '\n}catch(e){return null;}}';
  }


  public version: string;
  public customTransforms: CustomDruidTransforms;

  constructor(options: DruidExtractionFnBuilderOptions) {
    this.version = options.version;
    this.customTransforms = options.customTransforms;
  }

  public expressionToExtractionFn(expression: Expression): Druid.ExtractionFn | null {
    const extractionFn = this.expressionToExtractionFnPure(expression);
    if (extractionFn && extractionFn.type === 'cascade') {
      if (extractionFn.extractionFns.every(extractionFn => extractionFn.type === 'javascript')) {
        // If they are all JS anyway might as well make a single JS function (this is better for Druid < 0.9.0)
        return this.expressionToJavaScriptExtractionFn(expression);
      }

      if (this.versionBefore('0.9.0')) {
        try {
          // Try a Hail Mary pass - maybe we can plan the whole thing with JS
          return this.expressionToJavaScriptExtractionFn(expression);
        } catch (e) {
          throw new Error(`can not convert ${expression} to filter in Druid < 0.9.0`);
        }
      }
    }
    return extractionFn;
  }


  private expressionToExtractionFnPure(expression: Expression): Druid.ExtractionFn | null {
    let freeReferences = expression.getFreeReferences();
    if (freeReferences.length > 1) {
      throw new Error(`must have at most 1 reference (has ${freeReferences.length}): ${expression}`);
    }

    if (expression instanceof LiteralExpression) {
      return this.literalToExtractionFn(expression);

    } else if (expression instanceof RefExpression) {
      return this.refToExtractionFn(expression);

    } else if (expression instanceof ConcatExpression) {
      return this.concatToExtractionFn(expression);

    } else if (expression instanceof CustomTransformExpression) {
      return this.customTransformToExtractionFn(expression);

    } else if (expression instanceof NumberBucketExpression) {
      return this.numberBucketToExtractionFn(expression);

    } else if (expression instanceof SubstrExpression) {
      return this.substrToExtractionFn(expression);

    } else if (expression instanceof TimeBucketExpression || expression instanceof TimeFloorExpression) {
      return this.timeFloorToExtractionFn(expression);

    } else if (expression instanceof TimePartExpression) {
      return this.timePartToExtractionFn(expression);

    } else if (expression instanceof TransformCaseExpression) {
      return this.transformCaseToExtractionFn(expression);

    } else if (expression instanceof LengthExpression) {
      return this.lengthToExtractionFn(expression);

    } else if (expression instanceof ExtractExpression) {
      return this.extractToExtractionFn(expression);

    } else if (expression instanceof LookupExpression) {
      return this.lookupToExtractionFn(expression);

    } else if (expression instanceof FallbackExpression) {
      return this.fallbackToExtractionFn(expression);

    } else if (expression instanceof CastExpression) {
      return this.castToExtractionFn(expression);

    } else if (expression instanceof OverlapExpression) {
      return this.overlapToExtractionFn(expression);

    } else {
      return this.expressionToJavaScriptExtractionFn(expression);

    }
  }

  private literalToExtractionFn(expression: LiteralExpression): Druid.ExtractionFn {
    return {
      type: "lookup",
      retainMissingValue: false,
      replaceMissingValueWith: expression.getLiteralValue(),
      lookup: {
        type: "map",
        map: {}
      }
    };
  }

  private refToExtractionFn(expression: RefExpression): Druid.ExtractionFn | null {
    if (expression.type === 'BOOLEAN') {
      return {
        type: "lookup",
        lookup: {
          type: "map",
          map: {
            "0": "false",
            "1": "true",
            "false": "false",
            "true": "true"
          }
        }
      };
    } else {
      return null;
    }
  }

  private concatToExtractionFn(expression: ConcatExpression): Druid.ExtractionFn | null {
    if (this.versionBefore('0.9.1')) {
      // Druid < 0.9.1 behaves badly on null https://github.com/druid-io/druid/issues/2706 (does not have nullHandling: 'returnNull')
      return this.expressionToJavaScriptExtractionFn(expression);
    }

    let innerExtractionFn: Druid.ExtractionFn | null = null;
    let format = expression.getExpressionList().map(ex => {
      if (ex instanceof LiteralExpression) {
        return ex.value.replace(/%/g, '\\%');
      }
      if (!ex.isOp('literal')) {
        innerExtractionFn = this.expressionToExtractionFnPure(ex);
      }
      return '%s';
    }).join('');

    return DruidExtractionFnBuilder.composeFns(innerExtractionFn, {
      type: 'stringFormat',
      format,
      nullHandling: 'returnNull'
    });
  }

  private timeFloorToExtractionFn(expression: TimeFloorExpression | TimeBucketExpression): Druid.ExtractionFn | null {
    const { operand, duration } = expression;
    const timezone = expression.getTimezone();

    let myExtractionFn: Druid.ExtractionFn;
    if (this.versionBefore('0.9.2')) {
      let singleSpan = duration.getSingleSpan();
      let spanValue = duration.getSingleSpanValue();

      if (spanValue === 1 && DruidExtractionFnBuilder.SPAN_TO_FLOOR_FORMAT[singleSpan]) {
        myExtractionFn = {
          type: "timeFormat",
          format: DruidExtractionFnBuilder.SPAN_TO_FLOOR_FORMAT[singleSpan],
          timeZone: timezone.toString()
        };
      } else {
        let prop = DruidExtractionFnBuilder.SPAN_TO_PROPERTY[singleSpan];
        if (!prop) throw new Error(`can not floor on ${duration}`);
        myExtractionFn = {
          type: 'javascript',
          'function': DruidExtractionFnBuilder.wrapFunctionTryCatch([
            'var d = new org.joda.time.DateTime(s);',
            timezone.isUTC() ? null : `d = d.withZone(org.joda.time.DateTimeZone.forID(${JSON.stringify(timezone)}));`,
            `d = d.${prop}().roundFloorCopy();`,
            `d = d.${prop}().setCopy(Math.floor(d.${prop}().get() / ${spanValue}) * ${spanValue});`,
            'return d;'
          ])
        };
      }

    } else {
      myExtractionFn = {
        type: "timeFormat",
        granularity: {
          type: "period",
          period: duration.toString(),
          timeZone: timezone.toString()
        },
        format: DruidExtractionFnBuilder.SPAN_TO_FLOOR_FORMAT['second'],
        timeZone: 'Etc/UTC' // ToDo: is this necessary?
      };

    }

    // Druid < 0.10.0 had a bug where not setting locale would produce a NPE in the cache key calcualtion
    if (this.versionBefore('0.10.0') && myExtractionFn.type === "timeFormat") {
      myExtractionFn.locale = "en-US";
    }

    return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), myExtractionFn);
  }

  private timePartToExtractionFn(expression: TimePartExpression): Druid.ExtractionFn | null {
    const { operand, part } = expression;
    const timezone = expression.getTimezone();

    let myExtractionFn: Druid.ExtractionFn;
    let format = DruidExtractionFnBuilder.TIME_PART_TO_FORMAT[part];
    if (format) {
      myExtractionFn = {
        type: "timeFormat",
        format: format,
        locale: "en-US",
        timeZone: timezone.toString()
      };
    } else {
      let expr = DruidExtractionFnBuilder.TIME_PART_TO_EXPR[part];
      if (!expr) throw new Error(`can not part on ${part}`);
      myExtractionFn = {
        type: 'javascript',
        'function': DruidExtractionFnBuilder.wrapFunctionTryCatch([
          'var d = new org.joda.time.DateTime(s);',
          timezone.isUTC() ? null : `d = d.withZone(org.joda.time.DateTimeZone.forID(${JSON.stringify(timezone)}));`,
          `d = ${expr};`,
          'return d;'
        ])
      };
    }

    return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), myExtractionFn);
  }

  private numberBucketToExtractionFn(expression: NumberBucketExpression): Druid.ExtractionFn | null {
    if (this.versionBefore('0.9.2')) return this.expressionToJavaScriptExtractionFn(expression);
    const { operand, size, offset } = expression;

    let bucketExtractionFn: Druid.ExtractionFn = { type: "bucket" };
    if (size !== 1) bucketExtractionFn.size = size;
    if (offset !== 0) bucketExtractionFn.offset = offset;
    return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), bucketExtractionFn);
  }

  private substrToExtractionFn(expression: SubstrExpression): Druid.ExtractionFn | null {
    if (this.versionBefore('0.9.0')) return this.expressionToJavaScriptExtractionFn(expression);
    const { operand, position, len } = expression;
    return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), {
      type: "substring",
      index: position,
      length: len
    });
  }

  private transformCaseToExtractionFn(expression: TransformCaseExpression): Druid.ExtractionFn | null {
    const { operand, transformType } = expression;

    if (this.versionBefore('0.9.1')) return this.expressionToJavaScriptExtractionFn(expression);

    let type = DruidExtractionFnBuilder.CASE_TO_DRUID[transformType];
    if (!type) throw new Error(`unsupported case transformation '${type}'`);

    return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), {
      type: type
    });
  }

  private lengthToExtractionFn(expression: LengthExpression): Druid.ExtractionFn {
    const { operand } = expression;

    if (this.versionBefore('0.10.0')) return this.expressionToJavaScriptExtractionFn(expression);

    return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), {
      type: 'strlen'
    });
  }

  private extractToExtractionFn(expression: ExtractExpression): Druid.ExtractionFn | null {
    const { operand, regexp } = expression;

    if (this.versionBefore('0.9.0')) return this.expressionToJavaScriptExtractionFn(expression);

    return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), {
      type: "regex",
      expr: regexp,
      replaceMissingValue: true
    });
  }

  private lookupToExtractionFn(expression: LookupExpression): Druid.ExtractionFn | null {
    const { operand, lookupFn } = expression;

    let lookupExtractionFn: Druid.ExtractionFn = {
      type: "registeredLookup",
      lookup: lookupFn
    };

    if (this.versionBefore('0.9.1') || /-legacy-lookups/.test(this.version)) {
      lookupExtractionFn = {
        type: "lookup",
        lookup: {
          type: "namespace",
          "namespace": lookupFn
        }
      };
    }

    return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), lookupExtractionFn);
  }

  private fallbackToExtractionFn(expression: FallbackExpression): Druid.ExtractionFn | null {
    const { operand, expression: fallback } = expression;

    if (operand instanceof ExtractExpression) {
      let extractExtractionFn = this.extractToExtractionFn(operand);
      let extractExtractionFnLast = DruidExtractionFnBuilder.getLastFn(extractExtractionFn);

      if (fallback.isOp("ref")) {
        // the ref has to be the same as the argument because we can't refer to other dimensions
        // so the only option would be for it to be equal to original dimension
        delete extractExtractionFnLast.replaceMissingValue;

      } else if (fallback.isOp("literal")) {
        extractExtractionFnLast.replaceMissingValueWith = fallback.getLiteralValue();

      } else {
        throw new Error(`unsupported fallback: ${expression}`);
      }

      return extractExtractionFn;

    } else if (operand instanceof LookupExpression) {
      let lookupExtractionFn = this.lookupToExtractionFn(operand);
      let lookupExtractionFnLast = DruidExtractionFnBuilder.getLastFn(lookupExtractionFn);

      if (fallback.isOp("ref")) {
        // the ref has to be the same as the argument because we can't refer to other dimensions
        // so the only option would be for it to be equal to original dimension
        lookupExtractionFnLast.retainMissingValue = true;

      } else if (fallback.isOp("literal")) {
        lookupExtractionFnLast.replaceMissingValueWith = fallback.getLiteralValue();

      } else {
        throw new Error(`unsupported fallback: ${expression}`);
      }

      return lookupExtractionFn;
    }

    if (fallback instanceof LiteralExpression) {
      return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), {
        type: "lookup",
        retainMissingValue: true,
        lookup: {
          type: "map",
          map: {
            "": fallback.value
          }
        }
      });

    }

    return this.expressionToJavaScriptExtractionFn(expression);
  }

  private customTransformToExtractionFn(customTransform: CustomTransformExpression): Druid.ExtractionFn {
    const { operand, custom } = customTransform;
    let customExtractionFn = this.customTransforms[custom];
    if (!customExtractionFn) throw new Error(`could not find extraction function: '${custom}'`);
    let extractionFn = customExtractionFn.extractionFn;

    if (typeof extractionFn.type !== 'string') throw new Error(`must have type in custom extraction fn '${custom}'`);

    try {
      JSON.parse(JSON.stringify(customExtractionFn));
    } catch (e) {
      throw new Error(`must have JSON extraction Fn '${custom}'`);
    }

    return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), extractionFn);
  }

  private castToExtractionFn(cast: CastExpression): Druid.ExtractionFn {
    if (this.versionBefore('0.10.0') || cast.outputType === 'TIME') {
      return this.expressionToJavaScriptExtractionFn(cast);
    }
    // Do nothing, just swallow the cast
    return this.expressionToExtractionFnPure(cast.operand);
  }

  private overlapToExtractionFn(expression: OverlapExpression): Druid.ExtractionFn {
    let freeReferences = expression.operand.getFreeReferences();
    let rhsType = expression.expression.type;
    if (freeReferences[0] === '__time' && // hack
      expression.expression instanceof LiteralExpression &&
      (rhsType === 'TIME_RANGE' || rhsType === 'SET/TIME_RANGE')
    ) {
      expression = expression.operand.cast('NUMBER').overlap(r(expression.expression.getLiteralValue().changeToNumber()));
    }

    return this.expressionToJavaScriptExtractionFn(expression);
  }

  private expressionToJavaScriptExtractionFn(ex: Expression): Druid.ExtractionFn {
    let prefixFn: Druid.ExtractionFn = null;
    let jsExtractionFn: Druid.ExtractionFn = {
      type: "javascript",
      'function': null
    };

    // Hack
    if (ex.getFreeReferences()[0] === '__time') {
      ex = ex.substitute((e) => {
        if (e instanceof LiteralExpression) {
          if (e.value instanceof TimeRange) {
            return r(e.value.changeToNumber());
          } else {
            return null;
          }
        } else if (e instanceof RefExpression) {
          return $('__time');
        } else {
          return null;
        }
      });
    }

    try {
      jsExtractionFn['function'] = ex.getJSFn('d');
    } catch (e) {
      if (ex instanceof ChainableUnaryExpression) {
        prefixFn = this.expressionToExtractionFnPure(ex.operand);
        jsExtractionFn['function'] = ex.getAction().getJSFn('d');
      } else {
        throw e;
      }
    }

    if (ex.isOp('concat')) jsExtractionFn.injective = true;
    return DruidExtractionFnBuilder.composeFns(prefixFn, jsExtractionFn);
  }

  private versionBefore(neededVersion: string): boolean {
    const { version } = this;
    return version && External.versionLessThan(version, neededVersion);
  }
}
