/*
 * Copyright 2016-2020 Imply Data, Inc.
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
  TransformCaseExpression,
} from '../../expressions';

import { External } from '../baseExternal';
import { CustomDruidTransforms } from './druidTypes';

export interface DruidExtractionFnBuilderOptions {
  customTransforms: CustomDruidTransforms;
}

export class DruidExtractionFnBuilder {
  static CASE_TO_DRUID: Record<string, string> = {
    upperCase: 'upper',
    lowerCase: 'lower',
  };

  static TIME_PART_TO_FORMAT: Record<string, string> = {
    SECOND_OF_MINUTE: 's',
    MINUTE_OF_HOUR: 'm',
    HOUR_OF_DAY: 'H',
    DAY_OF_WEEK: 'e',
    DAY_OF_MONTH: 'd',
    DAY_OF_YEAR: 'D',
    WEEK_OF_YEAR: 'w',
    MONTH_OF_YEAR: 'M',
    YEAR: 'Y',
  };

  static composeFns(
    f: Druid.ExtractionFn | null,
    g: Druid.ExtractionFn | null,
  ): Druid.ExtractionFn | null {
    if (!f || !g) return f || g;
    return {
      type: 'cascade',
      extractionFns: [].concat(
        f.type === 'cascade' ? f.extractionFns : f,
        g.type === 'cascade' ? g.extractionFns : g,
      ),
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

  public customTransforms: CustomDruidTransforms;

  constructor(options: DruidExtractionFnBuilderOptions) {
    this.customTransforms = options.customTransforms;
  }

  public expressionToExtractionFn(expression: Expression): Druid.ExtractionFn | null {
    let freeReferences = expression.getFreeReferences();
    if (freeReferences.length > 1) {
      throw new Error(
        `must have at most 1 reference (has ${freeReferences.length}): ${expression}`,
      );
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
    } else if (
      expression instanceof TimeBucketExpression ||
      expression instanceof TimeFloorExpression
    ) {
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
      throw new Error(`can not convert ${expression} to extractionFn`);
    }
  }

  private literalToExtractionFn(expression: LiteralExpression): Druid.ExtractionFn {
    return {
      type: 'lookup',
      retainMissingValue: false,
      replaceMissingValueWith: expression.getLiteralValue(),
      lookup: {
        type: 'map',
        map: {},
      },
    };
  }

  private refToExtractionFn(expression: RefExpression): Druid.ExtractionFn | null {
    if (expression.type === 'BOOLEAN') {
      return {
        type: 'lookup',
        lookup: {
          type: 'map',
          map: {
            '0': 'false',
            '1': 'true',
            false: 'false',
            true: 'true',
          },
        },
      };
    } else {
      return null;
    }
  }

  private concatToExtractionFn(expression: ConcatExpression): Druid.ExtractionFn | null {
    let innerExpression: Expression | null = null;
    let format = expression
      .getExpressionList()
      .map(ex => {
        if (ex instanceof LiteralExpression) {
          return ex.value.replace(/%/g, '\\%');
        }
        if (innerExpression) {
          throw new Error(`can not have multiple expressions in concat '${expression}'`);
        }
        innerExpression = ex;
        return '%s';
      })
      .join('');

    if (!innerExpression) throw new Error(`invalid concat expression '${expression}'`);

    return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFn(innerExpression), {
      type: 'stringFormat',
      format,
      nullHandling: 'returnNull',
    });
  }

  private timeFloorToExtractionFn(
    expression: TimeFloorExpression | TimeBucketExpression,
  ): Druid.ExtractionFn | null {
    const { operand, duration } = expression;
    const timezone = expression.getTimezone();

    let myExtractionFn: Druid.ExtractionFn = {
      type: 'timeFormat',
      granularity: {
        type: 'period',
        period: duration.toString(),
        timeZone: timezone.toString(),
      },
      format: "yyyy-MM-dd'T'HH:mm:ss'Z",
      timeZone: 'Etc/UTC', // ToDo: is this necessary?
    };

    return DruidExtractionFnBuilder.composeFns(
      this.expressionToExtractionFn(operand),
      myExtractionFn,
    );
  }

  private timePartToExtractionFn(expression: TimePartExpression): Druid.ExtractionFn | null {
    const { operand, part } = expression;
    const timezone = expression.getTimezone();

    let myExtractionFn: Druid.ExtractionFn;
    let format = DruidExtractionFnBuilder.TIME_PART_TO_FORMAT[part];
    if (format) {
      myExtractionFn = {
        type: 'timeFormat',
        format: format,
        locale: 'en-US',
        timeZone: timezone.toString(),
      };
    } else {
      throw new Error(`can not part on ${part}`);
    }

    return DruidExtractionFnBuilder.composeFns(
      this.expressionToExtractionFn(operand),
      myExtractionFn,
    );
  }

  private numberBucketToExtractionFn(
    expression: NumberBucketExpression,
  ): Druid.ExtractionFn | null {
    const { operand, size, offset } = expression;

    let bucketExtractionFn: Druid.ExtractionFn = { type: 'bucket' };
    if (size !== 1) bucketExtractionFn.size = size;
    if (offset !== 0) bucketExtractionFn.offset = offset;
    return DruidExtractionFnBuilder.composeFns(
      this.expressionToExtractionFn(operand),
      bucketExtractionFn,
    );
  }

  private substrToExtractionFn(expression: SubstrExpression): Druid.ExtractionFn | null {
    const { operand, position, len } = expression;
    return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFn(operand), {
      type: 'substring',
      index: position,
      length: len,
    });
  }

  private transformCaseToExtractionFn(
    expression: TransformCaseExpression,
  ): Druid.ExtractionFn | null {
    const { operand, transformType } = expression;

    let type = DruidExtractionFnBuilder.CASE_TO_DRUID[transformType];
    if (!type) throw new Error(`unsupported case transformation '${type}'`);

    return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFn(operand), {
      type: type,
    });
  }

  private lengthToExtractionFn(expression: LengthExpression): Druid.ExtractionFn {
    const { operand } = expression;

    return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFn(operand), {
      type: 'strlen',
    });
  }

  private extractToExtractionFn(expression: ExtractExpression): Druid.ExtractionFn | null {
    const { operand, regexp } = expression;

    return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFn(operand), {
      type: 'regex',
      expr: regexp,
      replaceMissingValue: true,
    });
  }

  private lookupToExtractionFn(expression: LookupExpression): Druid.ExtractionFn | null {
    const { operand, lookupFn } = expression;

    let lookupExtractionFn: Druid.ExtractionFn = {
      type: 'registeredLookup',
      lookup: lookupFn,
    };

    return DruidExtractionFnBuilder.composeFns(
      this.expressionToExtractionFn(operand),
      lookupExtractionFn,
    );
  }

  private fallbackToExtractionFn(expression: FallbackExpression): Druid.ExtractionFn | null {
    const { operand, expression: fallback } = expression;

    if (operand instanceof ExtractExpression) {
      let extractExtractionFn = this.extractToExtractionFn(operand);
      let extractExtractionFnLast = DruidExtractionFnBuilder.getLastFn(extractExtractionFn);

      if (fallback.isOp('ref')) {
        // the ref has to be the same as the argument because we can't refer to other dimensions
        // so the only option would be for it to be equal to original dimension
        delete extractExtractionFnLast.replaceMissingValue;
      } else if (fallback.isOp('literal')) {
        extractExtractionFnLast.replaceMissingValueWith = fallback.getLiteralValue();
      } else {
        throw new Error(`unsupported fallback: ${expression}`);
      }

      return extractExtractionFn;
    } else if (operand instanceof LookupExpression) {
      let lookupExtractionFn = this.lookupToExtractionFn(operand);
      let lookupExtractionFnLast = DruidExtractionFnBuilder.getLastFn(lookupExtractionFn);

      if (fallback.isOp('ref')) {
        // the ref has to be the same as the argument because we can't refer to other dimensions
        // so the only option would be for it to be equal to original dimension
        lookupExtractionFnLast.retainMissingValue = true;
      } else if (fallback.isOp('literal')) {
        lookupExtractionFnLast.replaceMissingValueWith = fallback.getLiteralValue();
      } else {
        throw new Error(`unsupported fallback: ${expression}`);
      }

      return lookupExtractionFn;
    }

    if (fallback instanceof LiteralExpression) {
      throw new Error(`cant handle direct fallback: ${expression}`);
    }

    throw new Error(`can not convert fallback ${expression} to extractionFn`);
  }

  private customTransformToExtractionFn(
    customTransform: CustomTransformExpression,
  ): Druid.ExtractionFn {
    const { operand, custom } = customTransform;
    let customExtractionFn = this.customTransforms[custom];
    if (!customExtractionFn) throw new Error(`could not find extraction function: '${custom}'`);
    let extractionFn = customExtractionFn.extractionFn;

    if (typeof extractionFn.type !== 'string')
      throw new Error(`must have type in custom extraction fn '${custom}'`);

    try {
      JSON.parse(JSON.stringify(customExtractionFn));
    } catch (e) {
      throw new Error(`must have JSON extraction Fn '${custom}'`);
    }

    return DruidExtractionFnBuilder.composeFns(
      this.expressionToExtractionFn(operand),
      extractionFn,
    );
  }

  private castToExtractionFn(cast: CastExpression): Druid.ExtractionFn {
    if (cast.outputType === 'TIME') {
      throw new Error(`can not convert cast ${cast} to extractionFn`);
    }
    // Do nothing, just swallow the cast
    return this.expressionToExtractionFn(cast.operand);
  }

  private overlapToExtractionFn(expression: OverlapExpression): Druid.ExtractionFn {
    let freeReferences = expression.operand.getFreeReferences();
    let rhsType = expression.expression.type;
    if (
      freeReferences[0] === '__time' && // hack
      expression.expression instanceof LiteralExpression &&
      (rhsType === 'TIME_RANGE' || rhsType === 'SET/TIME_RANGE')
    ) {
      expression = expression.operand
        .cast('NUMBER')
        .overlap(r(expression.expression.getLiteralValue().changeToNumber()));
    }

    throw new Error(`can not convert overlap ${expression} to extractionFn`);
  }
}
