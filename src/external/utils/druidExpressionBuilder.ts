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
  TransformCaseExpression
} from '../../expressions/index';

import { External } from '../baseExternal';

export interface DruidExpressionBuilderOptions {
}

/*
 private static String escape(final String s)
  {
    final StringBuilder escaped = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (Character.isLetterOrDigit(c) || Arrays.binarySearch(SAFE_CHARS, c) >= 0) {
        escaped.append(c);
      } else {
        escaped.append("\\u").append(BaseEncoding.base16().encode(Chars.toByteArray(c)));
      }
    }
    return escaped.toString();
  }
 */

export class DruidExpressionBuilder {
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
      return `'${DruidExpressionBuilder.escape(expression.getLiteralValue())}'`;

    } else if (expression instanceof RefExpression) {
      return `"${DruidExpressionBuilder.escape(expression.name)}"`;

    } else if (expression instanceof ChainableExpression) {
      const ex1 = this.expressionToDruidExpression(expression.operand);

      if (expression instanceof SubstrExpression) {
        return `substring(${ex1},${expression.position},${expression.len})`;

      } else if (expression instanceof ChainableUnaryExpression) {
        const ex2 = this.expressionToDruidExpression(expression.expression);

        if (expression instanceof ConcatExpression) {
          return `concat(${ex1},${ex2})`;

        } else if (expression instanceof OverlapExpression || expression instanceof IsExpression) {
          return `(${ex1} == ${ex2})`;

        }
      }
    }

    /*
    } else if (expression instanceof NumberBucketExpression) {
      return this.numberBucketToDruidExpression(expression);

    } else if (expression instanceof TimeBucketExpression || expression instanceof TimeFloorExpression) {
      return this.timeFloorToDruidExpression(expression);

    } else if (expression instanceof TimePartExpression) {
      return this.timePartToDruidExpression(expression);

    } else if (expression instanceof TransformCaseExpression) {
      return this.transformCaseToDruidExpression(expression);

    } else if (expression instanceof LengthExpression) {
      return this.lengthToDruidExpression(expression);

    } else if (expression instanceof ExtractExpression) {
      return this.extractToDruidExpression(expression);

    } else if (expression instanceof LookupExpression) {
      return this.lookupToDruidExpression(expression);

    } else if (expression instanceof FallbackExpression) {
      return this.fallbackToDruidExpression(expression);

    } else if (expression instanceof CastExpression) {
      return this.castToDruidExpression(expression);

    } else if (expression instanceof OverlapExpression) {
      return this.overlapToDruidExpression(expression);
    */

    throw new Error(`not implemented Druid expression for ${expression}`);
  }

  /*
  private timeFloorToDruidExpression(expression: TimeFloorExpression | TimeBucketExpression): string | null {
    const { operand, duration } = expression;
    const timezone = expression.getTimezone();

    let myExtractionFn: string;
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

    return DruidExtractionFnBuilder.composeFns(this.expressionToDruidExpressionPure(operand), myExtractionFn);
  }

  private timePartToDruidExpression(expression: TimePartExpression): string | null {
    const { operand, part } = expression;
    const timezone = expression.getTimezone();

    let myExtractionFn: string;
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

    return DruidExtractionFnBuilder.composeFns(this.expressionToDruidExpressionPure(operand), myExtractionFn);
  }

  private numberBucketToDruidExpression(expression: NumberBucketExpression): string | null {
    if (this.versionBefore('0.9.2')) return this.expressionToJavaScriptExtractionFn(expression);
    const { operand, size, offset } = expression;

    let bucketExtractionFn: string = { type: "bucket" };
    if (size !== 1) bucketExtractionFn.size = size;
    if (offset !== 0) bucketExtractionFn.offset = offset;
    return DruidExtractionFnBuilder.composeFns(this.expressionToDruidExpressionPure(operand), bucketExtractionFn);
  }

  private substrToDruidExpression(expression: SubstrExpression): string | null {
    if (this.versionBefore('0.9.0')) return this.expressionToJavaScriptExtractionFn(expression);
    const { operand, position, len } = expression;
    return DruidExtractionFnBuilder.composeFns(this.expressionToDruidExpressionPure(operand), {
      type: "substring",
      index: position,
      length: len
    });
  }

  private transformCaseToDruidExpression(expression: TransformCaseExpression): string | null {
    const { operand, transformType } = expression;

    if (this.versionBefore('0.9.1')) return this.expressionToJavaScriptExtractionFn(expression);

    let type = DruidExtractionFnBuilder.CASE_TO_DRUID[transformType];
    if (!type) throw new Error(`unsupported case transformation '${type}'`);

    return DruidExtractionFnBuilder.composeFns(this.expressionToDruidExpressionPure(operand), {
      type: type
    });
  }

  private lengthToDruidExpression(expression: LengthExpression): string {
    const { operand } = expression;

    if (this.versionBefore('0.10.0')) return this.expressionToJavaScriptExtractionFn(expression);

    return DruidExtractionFnBuilder.composeFns(this.expressionToDruidExpressionPure(operand), {
      type: 'strlen'
    });
  }

  private extractToDruidExpression(expression: ExtractExpression): string | null {
    const { operand, regexp } = expression;

    if (this.versionBefore('0.9.0')) return this.expressionToJavaScriptExtractionFn(expression);

    return DruidExtractionFnBuilder.composeFns(this.expressionToDruidExpressionPure(operand), {
      type: "regex",
      expr: regexp,
      replaceMissingValue: true
    });
  }

  private lookupToDruidExpression(expression: LookupExpression): string | null {
    const { operand, lookupFn } = expression;

    let lookupExtractionFn: string = {
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

    return DruidExtractionFnBuilder.composeFns(this.expressionToDruidExpressionPure(operand), lookupExtractionFn);
  }

  private fallbackToDruidExpression(expression: FallbackExpression): string | null {
    const { operand, expression: fallback } = expression;

    if (operand instanceof ExtractExpression) {
      let extractExtractionFn = this.extractToDruidExpression(operand);
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
      let lookupExtractionFn = this.lookupToDruidExpression(operand);
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
      return DruidExtractionFnBuilder.composeFns(this.expressionToDruidExpressionPure(operand), {
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

  private customTransformToDruidExpression(customTransform: CustomTransformExpression): string {
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

    return DruidExtractionFnBuilder.composeFns(this.expressionToDruidExpressionPure(operand), extractionFn);
  }

  private castToDruidExpression(cast: CastExpression): string {
    if (this.versionBefore('0.10.0') || cast.outputType === 'TIME') {
      return this.expressionToJavaScriptExtractionFn(cast);
    }
    // Do nothing, just swallow the cast
    return this.expressionToDruidExpressionPure(cast.operand);
  }

  private overlapToDruidExpression(expression: OverlapExpression): string {
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
  */
}
