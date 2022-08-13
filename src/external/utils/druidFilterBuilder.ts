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

import { isDate } from 'chronoshift';
import { NamedArray } from 'immutable-class';

import {
  AttributeInfo,
  NumberRange,
  PlywoodRange,
  Range,
  Set,
  TimeRange,
} from '../../datatypes/index';
import {
  AndExpression,
  ContainsExpression,
  Expression,
  IpMatchExpression,
  IpSearchExpression,
  IpStringifyExpression,
  IsExpression,
  LiteralExpression,
  MatchExpression,
  MvContainsExpression,
  MvOverlapExpression,
  NotExpression,
  OrExpression,
  OverlapExpression,
  r,
  RefExpression,
} from '../../expressions';

import { DruidExpressionBuilder } from './druidExpressionBuilder';
import { DruidExtractionFnBuilder } from './druidExtractionFnBuilder';
import { CustomDruidTransforms } from './druidTypes';

export interface DruidFilterAndIntervals {
  filter: Druid.Filter;
  intervals: Druid.Intervals;
}

export interface DruidFilterBuilderOptions {
  rawAttributes: AttributeInfo[];
  timeAttribute: string;
  allowEternity: boolean;
  customTransforms: CustomDruidTransforms;
}

export class DruidFilterBuilder {
  static TIME_ATTRIBUTE = '__time';
  static TRUE_INTERVAL = '1000/3000';

  public rawAttributes: AttributeInfo[];
  public timeAttribute: string;
  public allowEternity: boolean;
  public customTransforms: CustomDruidTransforms;

  constructor(options: DruidFilterBuilderOptions) {
    this.rawAttributes = options.rawAttributes;
    this.timeAttribute = options.timeAttribute;
    this.allowEternity = options.allowEternity;
    this.customTransforms = options.customTransforms;
  }

  public filterToDruid(filter: Expression): DruidFilterAndIntervals {
    if (!filter.canHaveType('BOOLEAN')) throw new Error(`can not filter on ${filter.type}`);

    if (filter.equals(Expression.FALSE)) {
      return {
        intervals: [],
        filter: null,
      };
    } else {
      const { extract, rest } = filter.extractFromAnd(ex => {
        // time.is(Literal) || time.overlap(Literal)
        return (
          (ex instanceof IsExpression || ex instanceof OverlapExpression) &&
          this.isTimeRef(ex.operand) &&
          ex.expression instanceof LiteralExpression
        );
      });
      return {
        intervals: this.timeFilterToIntervals(extract),
        filter: this.timelessFilterToFilter(rest),
      };
    }
  }

  public timeFilterToIntervals(filter: Expression): Druid.Intervals {
    if (!filter.canHaveType('BOOLEAN')) throw new Error(`can not filter on ${filter.type}`);

    if (filter instanceof LiteralExpression) {
      if (!filter.value) return [];
      if (!this.allowEternity)
        throw new Error('must filter on time unless the allowEternity flag is set');
      return DruidFilterBuilder.TRUE_INTERVAL;
    } else if (filter instanceof IsExpression) {
      const { operand: lhs, expression: rhs } = filter;
      if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
        return this.valueToIntervals(rhs.value);
      } else {
        throw new Error(`can not convert ${filter} to Druid interval`);
      }
    } else if (filter instanceof OverlapExpression) {
      const { operand: lhs, expression: rhs } = filter;
      if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
        return this.valueToIntervals(rhs.value);
      } else {
        throw new Error(`can not convert ${filter} to Druid intervals`);
      }
    } else {
      throw new Error(`can not convert ${filter} to Druid intervals`);
    }
  }

  public timelessFilterToFilter(filter: Expression): Druid.Filter {
    if (!filter.canHaveType('BOOLEAN')) throw new Error(`can not filter on ${filter.type}`);

    if (filter instanceof RefExpression) {
      filter = filter.is(true);
    }

    if (filter instanceof LiteralExpression) {
      if (filter.value === true) {
        return null;
      } else {
        throw new Error('should never get here');
      }
    } else if (filter instanceof NotExpression) {
      return {
        type: 'not',
        field: this.timelessFilterToFilter(filter.operand),
      };
    } else if (filter instanceof AndExpression) {
      return {
        type: 'and',
        fields: filter.getExpressionList().map(p => this.timelessFilterToFilter(p)),
      };
    } else if (filter instanceof OrExpression) {
      return {
        type: 'or',
        fields: filter.getExpressionList().map(p => this.timelessFilterToFilter(p)),
      };
    } else if (filter instanceof IsExpression) {
      const { operand: lhs, expression: rhs } = filter;
      if (rhs instanceof LiteralExpression) {
        if (Set.isSetType(rhs.type)) {
          return this.makeInFilter(lhs, rhs.value);
        } else {
          return this.makeSelectorFilter(lhs, rhs.value);
        }
      } else {
        throw new Error(`can not convert ${filter} to Druid filter`);
      }
    } else if (filter instanceof OverlapExpression) {
      const { operand: lhs, expression: rhs } = filter;
      if (rhs instanceof LiteralExpression) {
        const rhsType = rhs.type;
        if (rhsType === 'SET/STRING' || rhsType === 'SET/NUMBER' || rhsType === 'SET/NULL') {
          return this.makeInFilter(lhs, rhs.value);
        } else if (Set.unwrapSetType(rhsType) === 'TIME_RANGE' && this.isTimeRef(lhs)) {
          return this.makeIntervalFilter(lhs, rhs.value);
        } else if (
          rhsType === 'NUMBER_RANGE' ||
          rhsType === 'TIME_RANGE' ||
          rhsType === 'STRING_RANGE'
        ) {
          return this.makeBoundFilter(lhs, rhs.value);
        } else if (
          rhsType === 'SET/NUMBER_RANGE' ||
          rhsType === 'SET/TIME_RANGE' ||
          rhsType === 'SET/STRING_RANGE'
        ) {
          return {
            type: 'or',
            fields: rhs.value.elements.map((range: PlywoodRange) =>
              this.makeBoundFilter(lhs, range),
            ),
          };
        } else {
          throw new Error(`not supported OVERLAP rhs type ${rhsType}`);
        }
      } else {
        throw new Error(`can not convert ${filter} to Druid filter`);
      }
    } else if (filter instanceof MatchExpression) {
      return this.makeRegexFilter(filter.operand, filter.regexp);
    } else if (filter instanceof ContainsExpression) {
      const { operand: lhs, expression: rhs, compare } = filter;
      return this.makeContainsFilter(lhs, rhs, compare);
    } else if (filter instanceof MvContainsExpression) {
      return this.makeExpressionFilter(filter.operand.mvContains(filter.mvArray));
    } else if (filter instanceof MvOverlapExpression) {
      return filter.mvArray.some(v => v === null)
        ? this.makeInFilter(filter, Set.fromJS(filter.mvArray))
        : this.makeExpressionFilter(filter.operand.mvOverlap(filter.mvArray));
    } else if (filter instanceof IpMatchExpression) {
      return this.makeExpressionFilter(
        filter.operand.ipMatch(filter.ipSearchString, filter.ipSearchType),
      );
    } else if (filter instanceof IpSearchExpression) {
      return this.makeExpressionFilter(
        filter.operand.ipSearch(filter.ipSearchString, filter.ipSearchType),
      );
    } else if (filter instanceof IpStringifyExpression) {
      return this.makeExpressionFilter(filter.operand.ipStringify());
    }

    throw new Error(`could not convert filter ${filter} to Druid filter`);
  }

  private valueToIntervals(value: Date | TimeRange | Set): Druid.Intervals {
    if (isDate(value)) {
      return TimeRange.intervalFromDate(value as Date);
    } else if (value instanceof TimeRange) {
      return value.toInterval();
    } else if (value instanceof Set) {
      return value.elements.map(v => {
        if (isDate(v)) {
          return TimeRange.intervalFromDate(v as Date);
        } else if (v instanceof TimeRange) {
          return v.toInterval();
        } else {
          throw new Error(`can not convert set value ${JSON.stringify(v)} to Druid interval`);
        }
      });
    } else {
      throw new Error(`can not convert ${JSON.stringify(value)} to Druid intervals`);
    }
  }

  // Makes a filter of (ex = value) or (value in ex) which are the same in Druid
  private makeSelectorFilter(ex: Expression, value: any): Druid.Filter {
    const attributeInfo = this.getSingleReferenceAttributeInfo(ex);
    if (!attributeInfo) {
      return this.makeExpressionFilter(ex.is(r(value)));
    }

    if (attributeInfo.unsplitable) {
      throw new Error(
        `can not convert ${ex} = ${value} to filter because it references an un-filterable metric '${attributeInfo.name}' which is most likely rolled up.`,
      );
    }

    let extractionFn: Druid.ExtractionFn;
    try {
      extractionFn = new DruidExtractionFnBuilder(this).expressionToExtractionFn(ex);
    } catch {
      return this.makeExpressionFilter(ex.is(r(value)));
    }

    // Kill range
    if (value instanceof Range) value = value.start;

    const druidFilter: Druid.Filter = {
      type: 'selector',
      dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
      value,
    };
    if (extractionFn) druidFilter.extractionFn = extractionFn;
    return druidFilter;
  }

  private makeInFilter(ex: Expression, valueSet: Set): Druid.Filter {
    const elements = valueSet.elements;

    const attributeInfo = this.getSingleReferenceAttributeInfo(ex);
    if (!attributeInfo) {
      const fields = elements.map((value: string) => {
        return this.makeSelectorFilter(ex, value);
      });

      return { type: 'or', fields };
    }

    let extractionFn: Druid.ExtractionFn;
    try {
      extractionFn = new DruidExtractionFnBuilder(this).expressionToExtractionFn(ex);
    } catch {
      return this.makeExpressionFilter(ex.is(r(valueSet)));
    }

    const inFilter: Druid.Filter = {
      type: 'in',
      dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
      values: elements,
    };
    if (extractionFn) inFilter.extractionFn = extractionFn;
    return inFilter;
  }

  private makeBoundFilter(ex: Expression, range: PlywoodRange): Druid.Filter {
    const r0 = range.start;
    const r1 = range.end;
    const bounds = range.bounds;

    const attributeInfo = this.getSingleReferenceAttributeInfo(ex);
    if (!attributeInfo) {
      return this.makeExpressionFilter(ex.overlap(range));
    }

    let extractionFn: Druid.ExtractionFn;
    try {
      extractionFn = new DruidExtractionFnBuilder(this).expressionToExtractionFn(ex);
    } catch {
      return this.makeExpressionFilter(ex.overlap(range));
    }

    const boundFilter: Druid.Filter = {
      type: 'bound',
      dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
    };

    if (extractionFn) boundFilter.extractionFn = extractionFn;

    if (range instanceof NumberRange || attributeInfo.nativeType === 'LONG') {
      boundFilter.ordering = 'numeric';
    }

    function dataToBound(d: Date) {
      if (attributeInfo.nativeType === 'LONG') {
        return d.valueOf();
      } else {
        return d.toISOString();
      }
    }

    if (r0 != null) {
      boundFilter.lower = isDate(r0) ? dataToBound(r0 as Date) : (r0 as number | string);
      if (bounds[0] === '(') boundFilter.lowerStrict = true;
    }
    if (r1 != null) {
      boundFilter.upper = isDate(r1) ? dataToBound(r1 as Date) : (r1 as number | string);
      if (bounds[1] === ')') boundFilter.upperStrict = true;
    }
    return boundFilter;
  }

  private makeIntervalFilter(ex: Expression, range: TimeRange | Set): Druid.Filter {
    const attributeInfo = this.getSingleReferenceAttributeInfo(ex);
    if (!attributeInfo) {
      return this.makeExpressionFilter(ex.overlap(range));
    }

    let extractionFn: Druid.ExtractionFn;
    try {
      extractionFn = new DruidExtractionFnBuilder(this).expressionToExtractionFn(ex);
    } catch {
      return this.makeExpressionFilter(ex.overlap(range));
    }

    const interval = this.valueToIntervals(range);
    const intervalFilter: Druid.Filter = {
      type: 'interval',
      dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
      intervals: Array.isArray(interval) ? interval : [interval],
    };
    if (extractionFn) intervalFilter.extractionFn = extractionFn;
    return intervalFilter;
  }

  private makeRegexFilter(ex: Expression, regex: string): Druid.Filter {
    const attributeInfo = this.getSingleReferenceAttributeInfo(ex);
    if (!attributeInfo) {
      return this.makeExpressionFilter(ex.match(regex));
    }

    let extractionFn: Druid.ExtractionFn;
    try {
      extractionFn = new DruidExtractionFnBuilder(this).expressionToExtractionFn(ex);
    } catch {
      return this.makeExpressionFilter(ex.match(regex));
    }

    const regexFilter: Druid.Filter = {
      type: 'regex',
      dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
      pattern: regex,
    };
    if (extractionFn) regexFilter.extractionFn = extractionFn;
    return regexFilter;
  }

  private makeContainsFilter(lhs: Expression, rhs: Expression, compare: string): Druid.Filter {
    if (rhs instanceof LiteralExpression) {
      const attributeInfo = this.getSingleReferenceAttributeInfo(lhs);
      if (!attributeInfo) {
        return this.makeExpressionFilter(lhs.contains(rhs, compare));
      }

      if (lhs instanceof RefExpression && attributeInfo.termsDelegate) {
        return {
          type: 'fullText',
          textColumn: this.getDimensionNameForAttributeInfo(attributeInfo),
          termsColumn: attributeInfo.termsDelegate,
          query: rhs.value,
          matchAll: true,
          usePrefixForLastTerm: true,
        };
      }

      let extractionFn: Druid.ExtractionFn;
      try {
        extractionFn = new DruidExtractionFnBuilder(this).expressionToExtractionFn(lhs);
      } catch {
        return this.makeExpressionFilter(lhs.contains(rhs, compare));
      }

      const searchFilter: Druid.Filter = {
        type: 'search',
        dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
        query: {
          type: 'contains',
          value: rhs.value,
          caseSensitive: compare === ContainsExpression.NORMAL,
        },
      };
      if (extractionFn) searchFilter.extractionFn = extractionFn;
      return searchFilter;
    } else {
      return this.makeExpressionFilter(lhs.contains(rhs, compare));
    }
  }

  private makeExpressionFilter(filter: Expression) {
    const druidExpression = new DruidExpressionBuilder(this).expressionToDruidExpression(filter);
    if (druidExpression === null) {
      throw new Error(`could not convert ${filter} to Druid expression for filter`);
    }

    return {
      type: 'expression',
      expression: druidExpression,
    };
  }

  private getSingleReferenceAttributeInfo(ex: Expression): AttributeInfo | null {
    const freeReferences = ex.getFreeReferences();
    if (freeReferences.length !== 1) return null;
    const referenceName = freeReferences[0];
    return this.getAttributesInfo(referenceName);
  }

  private getDimensionNameForAttributeInfo(attributeInfo: AttributeInfo): string {
    return attributeInfo.name === this.timeAttribute
      ? DruidFilterBuilder.TIME_ATTRIBUTE
      : attributeInfo.name;
  }

  public getAttributesInfo(attributeName: string) {
    return NamedArray.get(this.rawAttributes, attributeName);
  }

  public isTimeRef(ex: Expression): ex is RefExpression {
    return ex instanceof RefExpression && ex.name === this.timeAttribute;
  }
}
