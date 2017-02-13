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

import { isDate } from 'chronoshift';
import { NamedArray } from 'immutable-class';

import {
  Expression,
  LiteralExpression,
  RefExpression,
  ChainableExpression,
  ChainableUnaryExpression,

  AbsoluteExpression,
  AddExpression,
  AndExpression,
  ApplyExpression,
  AverageExpression,
  CardinalityExpression,
  CastExpression,
  CollectExpression,
  ConcatExpression,
  ContainsExpression,
  CountExpression,
  CountDistinctExpression,
  CustomAggregateExpression,
  CustomTransformExpression,
  DivideExpression,
  ExtractExpression,
  FallbackExpression,
  GreaterThanExpression,
  GreaterThanOrEqualExpression,
  InExpression,
  IsExpression,
  JoinExpression,
  LengthExpression,
  LessThanExpression,
  LessThanOrEqualExpression,
  IndexOfExpression,
  LookupExpression,
  LimitExpression,
  MatchExpression,
  MaxExpression,
  MinExpression,
  MultiplyExpression,
  NotExpression,
  NumberBucketExpression,
  OrExpression,
  OverlapExpression,
  PowerExpression,
  QuantileExpression,
  SplitExpression,
  SubstrExpression,
  SubtractExpression,
  SumExpression,
  TimeBucketExpression,
  TimeFloorExpression,
  TimePartExpression,
  TimeRangeExpression,
  TimeShiftExpression,
  TransformCaseExpression
} from '../../expressions/index';

import {
  AttributeInfo,
  NumberRange,
  Range,
  Set,
  PlywoodRange,
  TimeRange
} from '../../datatypes/index';

import { External } from '../baseExternal';
import { CustomDruidTransforms } from './druidTypes';
import { DruidExtractionFnBuilder } from './druidExtractionFnBuilder';


export interface DruidFilterAndIntervals {
  filter: Druid.Filter;
  intervals: Druid.Intervals;
}

export interface DruidFilterBuilderOptions {
  version: string;
  rawAttributes: AttributeInfo[];
  timeAttribute: string;
  allowEternity: boolean;
  customTransforms: CustomDruidTransforms;
}

export class DruidFilterBuilder {
  static TIME_ATTRIBUTE = '__time';
  static TRUE_INTERVAL = "1000/3000";
  static FALSE_INTERVAL = "1000/1001";


  public version: string;
  public rawAttributes: AttributeInfo[];
  public timeAttribute: string;
  public allowEternity: boolean;
  public customTransforms: CustomDruidTransforms;

  constructor(options: DruidFilterBuilderOptions) {
    this.version = options.version;
    this.rawAttributes = options.rawAttributes;
    this.timeAttribute = options.timeAttribute;
    this.allowEternity = options.allowEternity;
    this.customTransforms = options.customTransforms;
  }

  public filterToDruid(filter: Expression): DruidFilterAndIntervals {
    if (filter.type !== 'BOOLEAN') throw new Error("must be a BOOLEAN filter");

    if (filter.equals(Expression.FALSE)) {
      return {
        intervals: DruidFilterBuilder.FALSE_INTERVAL,
        filter: null
      };
    } else {
      const { extract, rest } = filter.extractFromAnd(ex => {
        if (ex instanceof ChainableUnaryExpression) {
          // time.is(Literal) // time.in(Literal)
          const { op, operand: lhs, expression: rhs } = ex;
          if (this.isTimeRef(lhs) && rhs instanceof LiteralExpression) {
            return op === 'is' || op === 'in';
          }
        }
        return false;
      });
      return {
        intervals: this.timeFilterToIntervals(extract),
        filter: this.timelessFilterToFilter(rest, false)
      };
    }
  }

  public timeFilterToIntervals(filter: Expression): Druid.Intervals {
    if (filter.type !== 'BOOLEAN') throw new Error("must be a BOOLEAN filter");

    if (filter instanceof LiteralExpression) {
      if (!filter.value) return DruidFilterBuilder.FALSE_INTERVAL;
      if (!this.allowEternity) throw new Error('must filter on time unless the allowEternity flag is set');
      return DruidFilterBuilder.TRUE_INTERVAL;

    } else if (filter instanceof IsExpression) {
      const { operand: lhs, expression: rhs } = filter;
      if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
        return TimeRange.intervalFromDate(rhs.value);
      } else {
        throw new Error(`can not convert ${filter} to Druid interval`);
      }

    } else if (filter instanceof InExpression) {
      const { operand: lhs, expression: rhs } = filter;
      if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
        let timeRanges: TimeRange[];
        let rhsType = rhs.type;
        if (rhsType === 'SET/TIME_RANGE') {
          timeRanges = rhs.value.elements;
        } else if (rhsType === 'TIME_RANGE') {
          timeRanges = [rhs.value];
        } else {
          throw new Error(`not supported ${rhsType} for time filtering`);
        }

        let intervals = timeRanges.map(timeRange => timeRange.toInterval());
        return intervals.length === 1 ? intervals[0] : intervals;
      } else {
        throw new Error(`can not convert ${filter} to Druid interval`);
      }

    } else {
      throw new Error(`can not convert ${filter} to Druid interval`);
    }
  }

  public timelessFilterToFilter(filter: Expression, aggregatorFilter: boolean): Druid.Filter {
    if (filter.type !== 'BOOLEAN') throw new Error("must be a BOOLEAN filter");

    if (filter instanceof RefExpression) {
      filter = filter.is(true);
    }

    if (filter instanceof LiteralExpression) {
      if (filter.value === true) {
        return null;
      } else {
        throw new Error("should never get here");
      }

    } else if (filter instanceof NotExpression) {
      return {
        type: 'not',
        field: this.timelessFilterToFilter(filter.operand, aggregatorFilter)
      };

    } else if (filter instanceof AndExpression) {
      return {
        type: 'and',
        fields: filter.getExpressionList().map(p => this.timelessFilterToFilter(p, aggregatorFilter))
      };

    } else if (filter instanceof OrExpression) {
      return {
        type: 'or',
        fields: filter.getExpressionList().map(p => this.timelessFilterToFilter(p, aggregatorFilter))
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

    } else if (aggregatorFilter && this.versionBefore('0.9.1')) {
      if (this.versionBefore('0.8.2')) throw new Error(`can not express aggregate filter ${filter} in druid < 0.8.2`);
      return this.makeExtractionFilter(filter);

    } else if (filter instanceof InExpression || filter instanceof OverlapExpression) {
      const { operand: lhs, expression: rhs } = filter;
      // Special handling for r('some_tag').in($tags)
      if (filter instanceof InExpression && lhs instanceof LiteralExpression) {
        return this.makeSelectorFilter(rhs, lhs.value);

      } else if (rhs instanceof LiteralExpression) {
        let rhsType = rhs.type;
        if (rhsType === 'SET/STRING' || rhsType === 'SET/NUMBER' || rhsType === 'SET/NULL') {
          return this.makeInFilter(lhs, rhs.value);

        } else if (rhsType === 'NUMBER_RANGE' || rhsType === 'TIME_RANGE' || rhsType === 'STRING_RANGE') {
          return this.makeBoundFilter(lhs, rhs.value);

        } else if (rhsType === 'SET/NUMBER_RANGE' || rhsType === 'SET/TIME_RANGE') {
          let elements = rhs.value.elements;
          let fields = elements.map((range: PlywoodRange) => {
            return this.makeBoundFilter(lhs, range);
          });

          return fields.length === 1 ? fields[0] : { type: "or", fields };

        } else {
          throw new Error(`not supported IN rhs type ${rhsType}`);

        }

      } else {
        throw new Error(`can not convert ${filter} to Druid filter`);
      }

    } else if (filter instanceof MatchExpression) {
      return this.makeRegexFilter(filter.operand, filter.regexp);

    } else if (filter instanceof ContainsExpression) {
      const { operand: lhs, expression: rhs, compare } = filter;
      return this.makeContainsFilter(lhs, rhs, compare);

    } else {
      throw new Error(`could not convert filter ${filter} to Druid filter`);
    }
  }

  private checkFilterExtractability(attributeInfo: AttributeInfo): void {
    if (this.versionBefore('0.9.2') && attributeInfo.name === this.timeAttribute) {
      throw new Error('can not do secondary filtering on primary time dimension (https://github.com/druid-io/druid/issues/2816)');
    }
  }

  private makeJavaScriptFilter(ex: Expression): Druid.Filter {
    let attributeInfo = this.getSingleReferenceAttributeInfo(ex);

    this.checkFilterExtractability(attributeInfo);

    return {
      type: "javascript",
      dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
      "function": ex.getJSFn('d')
    };
  }

  private makeExtractionFilter(ex: Expression): Druid.Filter {
    let attributeInfo = this.getSingleReferenceAttributeInfo(ex);
    let extractionFn = new DruidExtractionFnBuilder(this).expressionToExtractionFn(ex);

    if (extractionFn) this.checkFilterExtractability(attributeInfo);

    return {
      type: "extraction",
      dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
      extractionFn: extractionFn,
      value: "true"
    };
  }

  // Makes a filter of (ex = value) or (value in ex) which are the same in Druid
  private makeSelectorFilter(ex: Expression, value: any): Druid.Filter {
    let attributeInfo = this.getSingleReferenceAttributeInfo(ex);
    if (attributeInfo.unsplitable) {
      throw new Error(`can not convert ${ex} = ${value} to filter because it references an un-filterable metric '${attributeInfo.name}' which is most likely rolled up.`);
    }

    let extractionFn = new DruidExtractionFnBuilder(this).expressionToExtractionFn(ex);

    if (extractionFn) this.checkFilterExtractability(attributeInfo);

    // Kill range
    if (value instanceof Range) value = value.start;

    let druidFilter: Druid.Filter = {
      type: "selector",
      dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
      value: attributeInfo.serialize(value)
    };
    if (extractionFn) {
      druidFilter.extractionFn = extractionFn;
      if (this.versionBefore('0.9.1')) druidFilter.type = "extraction";
      if (this.versionBefore('0.9.0') && druidFilter.value === null) druidFilter.value = '';
    }
    return druidFilter;
  }

  private makeInFilter(ex: Expression, valueSet: Set): Druid.Filter {
    let attributeInfo = this.getSingleReferenceAttributeInfo(ex);
    let extractionFn = new DruidExtractionFnBuilder(this).expressionToExtractionFn(ex);

    if (extractionFn) this.checkFilterExtractability(attributeInfo);

    let elements = valueSet.elements;
    if (
      elements.length < 2 ||
      (this.versionBefore('0.9.1') && extractionFn) ||
      this.versionBefore('0.9.0')
    ) {
      let fields = elements.map((value: string) => {
        return this.makeSelectorFilter(ex, value);
      });

      return fields.length === 1 ? fields[0] : { type: "or", fields };
    }

    let inFilter: Druid.Filter = {
      type: 'in',
      dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
      values: elements.map((value: string) => attributeInfo.serialize(value))
    };
    if (extractionFn) inFilter.extractionFn = extractionFn;
    return inFilter;
  }

  private makeBoundFilter(ex: Expression, range: PlywoodRange): Druid.Filter {
    let r0 = range.start;
    let r1 = range.end;
    let bounds = range.bounds;

    if (this.versionBefore('0.9.0') || r0 < 0 || r1 < 0) {
      return this.makeJavaScriptFilter(ex.in(range));
    }

    if (ex instanceof IndexOfExpression) {
      return this.makeJavaScriptFilter(ex.in(range));
    }

    let attributeInfo = this.getSingleReferenceAttributeInfo(ex);
    let extractionFn = new DruidExtractionFnBuilder(this).expressionToExtractionFn(ex);

    if (this.versionBefore('0.9.1') && extractionFn) {
      return this.makeJavaScriptFilter(ex.in(range));
    }

    if (extractionFn) this.checkFilterExtractability(attributeInfo);

    let boundFilter: Druid.Filter = {
      type: "bound",
      dimension: this.getDimensionNameForAttributeInfo(attributeInfo)
    };

    if (extractionFn) boundFilter.extractionFn = extractionFn;
    if (range instanceof NumberRange) boundFilter.alphaNumeric = true;

    if (r0 != null) {
      boundFilter.lower = isDate(r0) ? (r0 as Date).toISOString() : (r0 as number | string);
      if (bounds[0] === '(') boundFilter.lowerStrict = true;
    }
    if (r1 != null) {
      boundFilter.upper = isDate(r1) ? (r1 as Date).toISOString() : (r1 as number | string);
      if (bounds[1] === ')') boundFilter.upperStrict = true;
    }
    return boundFilter;
  }

  private makeRegexFilter(ex: Expression, regex: string): Druid.Filter {
    let attributeInfo = this.getSingleReferenceAttributeInfo(ex);
    let extractionFn = new DruidExtractionFnBuilder(this).expressionToExtractionFn(ex);

    if (this.versionBefore('0.9.1') && extractionFn) {
      return this.makeExtractionFilter(ex.match(regex));
    }

    if (extractionFn) this.checkFilterExtractability(attributeInfo);

    let regexFilter: Druid.Filter = {
      type: "regex",
      dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
      pattern: regex
    };
    if (extractionFn) regexFilter.extractionFn = extractionFn;
    return regexFilter;
  }

  private makeContainsFilter(lhs: Expression, rhs: Expression, compare: string): Druid.Filter {
    if (rhs instanceof LiteralExpression) {
      let attributeInfo = this.getSingleReferenceAttributeInfo(lhs);
      let extractionFn = new DruidExtractionFnBuilder(this).expressionToExtractionFn(lhs);

      if (extractionFn) this.checkFilterExtractability(attributeInfo);

      if (this.versionBefore('0.9.0')) {
        if (compare === ContainsExpression.IGNORE_CASE) {
          return {
            type: "search",
            dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
            query: {
              type: "insensitive_contains",
              value: rhs.value
            }
          };
        } else {
          return this.makeJavaScriptFilter(lhs.contains(rhs, compare));
        }
      }

      if (this.versionBefore('0.9.1') && extractionFn) {
        return this.makeExtractionFilter(lhs.contains(rhs, compare));
      }

      let searchFilter: Druid.Filter = {
        type: "search",
        dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
        query: {
          type: "contains",
          value: rhs.value,
          caseSensitive: compare === ContainsExpression.NORMAL
        }
      };
      if (extractionFn) searchFilter.extractionFn = extractionFn;
      return searchFilter;

    } else {
      return this.makeJavaScriptFilter(lhs.contains(rhs, compare));
    }
  }

  private getSingleReferenceAttributeInfo(ex: Expression): AttributeInfo {
    let freeReferences = ex.getFreeReferences();
    if (freeReferences.length !== 1) throw new Error(`can not translate multi reference expression ${ex} to Druid`);
    let referenceName = freeReferences[0];
    return this.getAttributesInfo(referenceName);
  }

  private getDimensionNameForAttributeInfo(attributeInfo: AttributeInfo): string {
    return attributeInfo.name === this.timeAttribute ? DruidFilterBuilder.TIME_ATTRIBUTE : attributeInfo.name;
  }

  private versionBefore(neededVersion: string): boolean {
    const { version } = this;
    return version && External.versionLessThan(version, neededVersion);
  }

  public getAttributesInfo(attributeName: string) {
    return NamedArray.get(this.rawAttributes, attributeName);
  }

  public isTimeRef(ex: Expression): boolean {
    return ex instanceof RefExpression && ex.name === this.timeAttribute;
  }
}
