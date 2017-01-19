/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
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

import * as Promise from 'any-promise';
import * as Druid from 'druid.d.ts';
import * as hasOwnProp from 'has-own-prop';
import { PlywoodRequester } from 'plywood-base-api';
import * as toArray from 'stream-to-array';
import { dictEqual, nonEmptyLookup, shallowCopy, ExtendableError } from '../helper/utils';
import {
  $, Expression,
  RefExpression,
  ChainableExpression,
  ChainableUnaryExpression,

  AddExpression,
  AndExpression,
  ApplyExpression,
  CardinalityExpression,
  FilterExpression,
  GreaterThanExpression,
  GreaterThanOrEqualExpression,
  InExpression,
  IsExpression,
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
  SelectExpression,
  SortExpression, Direction,
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
} from '../expressions/index';
import {
  AttributeInfo,
  Attributes,
  UniqueAttributeInfo,
  HistogramAttributeInfo,
  ThetaAttributeInfo,
  Dataset,
  Datum,
  NumberRange,
  Range,
  Set,
  TimeRange,
  PlywoodValue
} from '../datatypes/index';
import { External, ExternalJS, ExternalValue, Inflater, NextFn, PostProcess, QueryAndPostProcess, TotalContainer } from './baseExternal';
import { CustomDruidAggregations, CustomDruidTransforms } from './utils/druidTypes';
import { DruidExtractionFnBuilder } from './utils/druidExtractionFnBuilder';
import { DruidFilterBuilder, DruidFilterAndIntervals } from './utils/druidFilterBuilder';
import { DruidHavingFilterBuilder } from './utils/druidHavingFilterBuilder';
import { DruidAggregationBuilder, AggregationsAndPostAggregations } from './utils/druidAggregationBuilder';
import { unwrapSetType } from '../datatypes/common';
import { PlywoodRange } from '../datatypes/range';

export class InvalidResultError extends ExtendableError {
  public result: any;

  constructor(message: string, result: any) {
    super(message);
    this.result = result;
  }
}

function expressionNeedsAlphaNumericSort(ex: Expression): boolean {
  let type = ex.type;
  return (type === 'NUMBER' || type === 'NUMBER_RANGE');
}

function simpleJSONEqual(a: any, b: any): boolean {
  return JSON.stringify(a) === JSON.stringify(b); // ToDo: fill this in;
}

export interface Normalizer {
  (result: any): Datum[];
}

export interface GranularityInflater {
  granularity: Druid.Granularity;
  inflater: Inflater;
}

export interface DimensionInflater {
  dimension: Druid.DimensionSpec;
  inflater?: Inflater;
}

export interface DimensionInflaterHaving extends DimensionInflater {
  having?: Expression;
}

export interface DruidSplit {
  queryType: string;
  timestampLabel?: string;
  granularity: Druid.Granularity | string;
  dimension?: Druid.DimensionSpec;
  dimensions?: Druid.DimensionSpec[];
  leftoverHavingFilter?: Expression;
  postProcess: PostProcess;
}

export interface IntrospectPostProcess {
  (result: any): Attributes;
}


export class DruidExternal extends External {
  static engine = 'druid';
  static type = 'DATASET';
  static DUMMY_NAME = '!DUMMY';
  static TIME_ATTRIBUTE = '__time';

  static VALID_INTROSPECTION_STRATEGIES = ['segment-metadata-fallback', 'segment-metadata-only', 'datasource-get'];
  static DEFAULT_INTROSPECTION_STRATEGY = 'segment-metadata-fallback';

  static SELECT_INIT_LIMIT = 50;
  static SELECT_MAX_LIMIT = 10000;

  static fromJS(parameters: ExternalJS, requester: PlywoodRequester<any>): DruidExternal {
    // Back compat:
    if (typeof (<any>parameters).druidVersion === 'string') {
      parameters.version = (<any>parameters).druidVersion;
      console.warn(`'druidVersion' parameter is deprecated, use 'version: ${parameters.version}' instead`);
    }

    let value: ExternalValue = External.jsToValue(parameters, requester);
    value.timeAttribute = parameters.timeAttribute;
    value.customAggregations = parameters.customAggregations || {};
    value.customTransforms = parameters.customTransforms || {};
    value.allowEternity = Boolean(parameters.allowEternity);
    value.allowSelectQueries = Boolean(parameters.allowSelectQueries);
    value.introspectionStrategy = parameters.introspectionStrategy;
    value.exactResultsOnly = Boolean(parameters.exactResultsOnly);
    value.context = parameters.context;
    return new DruidExternal(value);
  }

  static getSourceList(requester: PlywoodRequester<any>): Promise<string[]> {
    return toArray(requester({ query: { queryType: 'sourceList' } }))
      .then((sourcesArray) => {
        const sources = sourcesArray[0];
        if (!Array.isArray(sources)) throw new InvalidResultError('invalid sources response', sources);
        return sources.sort();
      });
  }

  static getVersion(requester: PlywoodRequester<any>): Promise<string> {
    return toArray(requester({
      query: {
        queryType: 'status'
      }
    }))
      .then((res) => {
        return res[0].version;
      });
  }

  static timeBoundaryPostProcessFactory(applies?: ApplyExpression[]): PostProcess {
    return (res: Datum[]): Date | TotalContainer => {
      let result: any = res[0];

      if (applies) {
        let datum: Datum = {};
        for (let apply of applies) {
          let name = apply.name;
          if (typeof result === 'string') {
            datum[name] = new Date(result);
          } else {
            if (apply.expression.op === 'max') {
              datum[name] = new Date((result['maxIngestedEventTime'] || result['maxTime']) as string);
            } else {
              datum[name] = new Date(result['minTime'] as string);
            }
          }
        }

        return new TotalContainer(datum);

      } else {
        return new Date((result['maxIngestedEventTime'] || result['maxTime'] || result['minTime']) as string);
      }
    };
  }

  static valuePostProcess(res: Datum[]): PlywoodValue {
    if (!res.length) return 0;
    return res[0][External.VALUE_NAME] as any;
  }

  static totalPostProcessFactory(applies: ApplyExpression[]): PostProcess {
    return (res: Datum[]): TotalContainer => {
      if (!res.length) return new TotalContainer(External.makeZeroDatum(applies));
      return new TotalContainer(res[0]);
    };
  }

  // ==========================

  static postProcessFactory(inflaters: Inflater[], attributes: Attributes) {
    return (data: Datum[]): Dataset => {
      let n = data.length;
      for (let inflater of inflaters) {
        for (let i = 0; i < n; i++) {
          inflater(data[i], i, data);
        }
      }
      return new Dataset({ data, attributes });
    };
  }

  static selectNextFactory(limit: number, descending: boolean): NextFn<Druid.Query> {
    let resultsSoFar = 0;
    return (prevQuery, prevResultLength, prevMeta: any) => {
      if (prevResultLength === 0) return null; // Out of results: done!

      let { pagingIdentifiers } = prevMeta;
      if (prevResultLength < prevQuery.pagingSpec.threshold) return null; // Less results than asked for: done!

      resultsSoFar += prevResultLength;
      if (resultsSoFar >= limit) return null; // Got enough results overall: done!

      pagingIdentifiers = DruidExternal.movePagingIdentifiers(pagingIdentifiers, descending ? -1 : 1);
      prevQuery.pagingSpec.pagingIdentifiers = pagingIdentifiers;
      prevQuery.pagingSpec.threshold = Math.min(limit - resultsSoFar, DruidExternal.SELECT_MAX_LIMIT);
      return prevQuery;
    };
  }

  static generateMaker(aggregation: Druid.Aggregation): Expression {
    if (!aggregation) return null;
    let { type, fieldName } = aggregation;

    // Hacky way to guess at a count
    if (type === 'longSum' && fieldName === 'count') {
      return Expression._.count();
    }

    if (!fieldName) {
      let { fieldNames } = aggregation;
      if (!Array.isArray(fieldNames) || fieldNames.length !== 1) return null;
      fieldName = fieldNames[0];
    }

    let expression = $(fieldName);
    switch (type) {
      case "count":
        return Expression._.count();

      case "doubleSum":
      case "longSum":
        return Expression._.sum(expression);

      case "javascript":
        const { fnAggregate, fnCombine } = aggregation;
        if (fnAggregate !== fnCombine || fnCombine.indexOf('+') === -1) return null;
        return Expression._.sum(expression);

      case "doubleMin":
      case "longMin":
        return Expression._.min(expression);

      case "doubleMax":
      case "longMax":
        return Expression._.max(expression);

      default:
        return null;
    }
  }

  static segmentMetadataPostProcessFactory(timeAttribute: string): IntrospectPostProcess {
    return (res: Druid.SegmentMetadataResults): Attributes => {
      let res0 = res[0];
      if (!res0 || !res0.columns) throw new InvalidResultError('malformed segmentMetadata response', res);
      let columns = res0.columns;
      let aggregators = res0.aggregators || {};

      let foundTime = false;
      let attributes: Attributes = [];
      for (let name in columns) {
        if (!hasOwnProp(columns, name)) continue;
        let columnData = columns[name];

        // Error conditions
        if (columnData.errorMessage || columnData.size < 0) continue;

        if (name === DruidExternal.TIME_ATTRIBUTE) {
          attributes.push(new AttributeInfo({ name: timeAttribute, type: 'TIME' }));
          foundTime = true;
        } else {
          if (name === timeAttribute) continue; // Ignore dimensions and metrics that clash with the timeAttribute name
          switch (columnData.type) {
            case 'FLOAT':
            case 'LONG':
              attributes.push(new AttributeInfo({
                name,
                type: 'NUMBER',
                unsplitable: true,
                maker: DruidExternal.generateMaker(aggregators[name])
              }));
              break;

            case 'STRING':
              attributes.push(new AttributeInfo({
                name,
                type: columnData.hasMultipleValues ? 'SET/STRING' : 'STRING'
              }));
              break;

            case 'hyperUnique':
              attributes.push(new UniqueAttributeInfo({ name }));
              break;

            case 'approximateHistogram':
              attributes.push(new HistogramAttributeInfo({ name }));
              break;

            case 'thetaSketch':
              attributes.push(new ThetaAttributeInfo({ name }));
              break;
          }
        }
      }

      if (!foundTime) throw new Error(`no valid ${DruidExternal.TIME_ATTRIBUTE} in segmentMetadata response`);
      return attributes;
    };
  }

  static introspectPostProcessFactory(timeAttribute: string): IntrospectPostProcess {
    return (res: Druid.DatasourceIntrospectResult[]): Attributes => {
      const res0 = res[0];
      if (!Array.isArray(res0.dimensions) || !Array.isArray(res0.metrics)) {
        throw new InvalidResultError('malformed GET introspect response', res);
      }

      let attributes: Attributes = [
        new AttributeInfo({ name: timeAttribute, type: 'TIME' })
      ];
      res0.dimensions.forEach(dimension => {
        if (dimension === timeAttribute) return; // Ignore dimensions that clash with the timeAttribute name
        attributes.push(new AttributeInfo({ name: dimension, type: 'STRING' }));
      });
      res0.metrics.forEach(metric => {
        if (metric === timeAttribute) return; // Ignore metrics that clash with the timeAttribute name
        attributes.push(new AttributeInfo({ name: metric, type: 'NUMBER', unsplitable: true }));
      });
      return attributes;
    };
  }

  /**
   * A paging identifier typically looks like this:
   * { "wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9": 4 }
   */
  static movePagingIdentifiers(pagingIdentifiers: Druid.PagingIdentifiers, increment: number): Druid.PagingIdentifiers {
    let newPagingIdentifiers: Druid.PagingIdentifiers = {};
    for (let key in pagingIdentifiers) {
      if (!hasOwnProp(pagingIdentifiers, key)) continue;
      newPagingIdentifiers[key] = pagingIdentifiers[key] + increment;
    }
    return newPagingIdentifiers;
  }


  public timeAttribute: string;
  public customAggregations: CustomDruidAggregations;
  public customTransforms: CustomDruidTransforms;
  public allowEternity: boolean;
  public allowSelectQueries: boolean;
  public introspectionStrategy: string;
  public exactResultsOnly: boolean;
  public context: Lookup<any>;

  constructor(parameters: ExternalValue) {
    super(parameters, dummyObject);
    this._ensureEngine("druid");
    this._ensureMinVersion("0.8.0");
    this.timeAttribute = parameters.timeAttribute || DruidExternal.TIME_ATTRIBUTE;
    this.customAggregations = parameters.customAggregations;
    this.customTransforms = parameters.customTransforms;
    this.allowEternity = parameters.allowEternity;
    this.allowSelectQueries = parameters.allowSelectQueries;

    let introspectionStrategy = parameters.introspectionStrategy || DruidExternal.DEFAULT_INTROSPECTION_STRATEGY;
    if (DruidExternal.VALID_INTROSPECTION_STRATEGIES.indexOf(introspectionStrategy) === -1) {
      throw new Error(`invalid introspectionStrategy '${introspectionStrategy}'`);
    }
    this.introspectionStrategy = introspectionStrategy;

    this.exactResultsOnly = parameters.exactResultsOnly;
    this.context = parameters.context;
  }

  public valueOf(): ExternalValue {
    let value: ExternalValue = super.valueOf();
    value.timeAttribute = this.timeAttribute;
    value.customAggregations = this.customAggregations;
    value.customTransforms = this.customTransforms;
    value.allowEternity = this.allowEternity;
    value.allowSelectQueries = this.allowSelectQueries;
    value.introspectionStrategy = this.introspectionStrategy;
    value.exactResultsOnly = this.exactResultsOnly;
    value.context = this.context;
    return value;
  }

  public toJS(): ExternalJS {
    let js: ExternalJS = super.toJS();
    if (this.timeAttribute !== DruidExternal.TIME_ATTRIBUTE) js.timeAttribute = this.timeAttribute;
    if (nonEmptyLookup(this.customAggregations)) js.customAggregations = this.customAggregations;
    if (nonEmptyLookup(this.customTransforms)) js.customTransforms = this.customTransforms;
    if (this.allowEternity) js.allowEternity = true;
    if (this.allowSelectQueries) js.allowSelectQueries = true;
    if (this.introspectionStrategy !== DruidExternal.DEFAULT_INTROSPECTION_STRATEGY) js.introspectionStrategy = this.introspectionStrategy;
    if (this.exactResultsOnly) js.exactResultsOnly = true;
    if (this.context) js.context = this.context;
    return js;
  }

  public equals(other: DruidExternal): boolean {
    return super.equals(other) &&
      this.timeAttribute === other.timeAttribute &&
      simpleJSONEqual(this.customAggregations, other.customAggregations) &&
      simpleJSONEqual(this.customTransforms, other.customTransforms) &&
      this.allowEternity === other.allowEternity &&
      this.allowSelectQueries === other.allowSelectQueries &&
      this.introspectionStrategy === other.introspectionStrategy &&
      this.exactResultsOnly === other.exactResultsOnly &&
      dictEqual(this.context, other.context);
  }

  // -----------------

  public canHandleTotal(): boolean {
    return true;
  }

  public canHandleFilter(filter: FilterExpression): boolean {
    return !filter.expression.some((ex) => ex.isOp('cardinality') ? true : null);
  }

  public canHandleSplit(split: SplitExpression): boolean {
    return true;
  }

  public canHandleSplitExpression(ex: Expression): boolean {
    return true;
  }

  public canHandleApply(apply: ApplyExpression): boolean {
    return true;
  }

  public canHandleSort(sort: SortExpression): boolean {
    if (this.isTimeseries()) {
      if (sort.direction !== 'ascending') return false;
      return sort.refName() === this.split.firstSplitName();

    } else if (this.mode === 'raw') {
      if (sort.refName() !== this.timeAttribute) return false;
      if (this.versionBefore('0.9.0')) return sort.direction === 'ascending';
      return true;

    } else {
      return true;
    }
  }

  public canHandleLimit(limit: LimitExpression): boolean {
    return !this.isTimeseries();
  }

  public canHandleHavingFilter(havingFilter: FilterExpression): boolean {
    return !this.limit;
  }

  // -----------------

  public isTimeseries(): boolean {
    const { split } = this;
    if (!split || split.isMultiSplit()) return false;
    let splitExpression = split.firstSplitExpression();
    if (this.isTimeRef(splitExpression)) return true;
    const { op } = splitExpression;
    return op === 'timeBucket' || op === 'timeFloor';
  }

  public getDruidDataSource(): Druid.DataSource {
    let source = this.source;
    if (Array.isArray(source)) {
      return {
        type: "union",
        dataSources: <string[]>source
      };
    } else {
      return <string>source;
    }
  }

  public getDimensionNameForAttributeInfo(attributeInfo: AttributeInfo): string {
    return attributeInfo.name === this.timeAttribute ? DruidExternal.TIME_ATTRIBUTE : attributeInfo.name;
  }

  // ========= FILTERS =========

  public isTimeRef(ex: Expression): boolean {
    return ex instanceof RefExpression && ex.name === this.timeAttribute;
  }

  public splitExpressionToGranularityInflater(splitExpression: Expression, label: string): GranularityInflater {
    if (this.isTimeRef(splitExpression)) {
      return {
        granularity: 'none',
        inflater: External.timeInflaterFactory(label)
      };

    } else if (splitExpression instanceof TimeBucketExpression || splitExpression instanceof TimeFloorExpression) {
      const { op, operand, duration } = splitExpression;
      const timezone = splitExpression.getTimezone();
      if (this.isTimeRef(operand)) {
        return {
          granularity: {
            type: "period",
            period: duration.toString(),
            timeZone: timezone.toString()
          },
          inflater: op === 'timeBucket' ?
            External.timeRangeInflaterFactory(label, duration, timezone) :
            External.timeInflaterFactory(label)
        };
      }
    }

    return null;
  }

  // ---------------------------------------------------------------------------------------------------------------------------
  // Extraction functions

  // ----------------------------

  public expressionToDimensionInflater(expression: Expression, label: string): DimensionInflater {
    let freeReferences = expression.getFreeReferences();
    if (freeReferences.length !== 1) {
      throw new Error(`must have 1 reference (has ${freeReferences.length}): ${expression}`);
    }
    let referenceName = freeReferences[0];

    let attributeInfo = this.getAttributesInfo(referenceName);
    if (attributeInfo.unsplitable) {
      throw new Error(`can not convert ${expression} to split because it references an un-splitable metric '${referenceName}' which is most likely rolled up.`);
    }

    let extractionFn = new DruidExtractionFnBuilder(this).expressionToExtractionFn(expression);

    let simpleInflater = External.getSimpleInflater(expression, label);

    let dimension: Druid.DimensionSpecFull = {
      type: "default",
      dimension: attributeInfo.name === this.timeAttribute ? DruidExternal.TIME_ATTRIBUTE : attributeInfo.name,
      outputName: label
    };
    if (extractionFn) {
      dimension.type = "extraction";
      dimension.extractionFn = extractionFn;
    }

    if (expression instanceof RefExpression) {
      return {
        dimension,
        inflater: simpleInflater
      };
    }

    if (expression instanceof TimeBucketExpression) {
      return {
        dimension,
        inflater: External.timeRangeInflaterFactory(label, expression.duration, expression.getTimezone())
      };
    }

    if (expression instanceof TimePartExpression) {
      return {
        dimension,
        inflater: simpleInflater
      };
    }

    if (expression instanceof NumberBucketExpression) {
      return {
        dimension,
        inflater: External.numberRangeInflaterFactory(label, expression.size)
      };
    }

    if (expression instanceof CardinalityExpression) {
      return {
        dimension,
        inflater: External.setCardinalityInflaterFactory(label)
      };
    }

    let effectiveType = unwrapSetType(expression.type);
    if (simpleInflater || effectiveType === 'STRING') {
      return {
        dimension,
        inflater: simpleInflater
      };
    }

    throw new Error(`could not convert ${expression} to a Druid dimension`);
  }

  public expressionToDimensionInflaterHaving(expression: Expression, label: string, havingFilter: Expression): DimensionInflaterHaving {
    let dimensionInflater: DimensionInflaterHaving = this.expressionToDimensionInflater(expression, label);
    dimensionInflater.having = havingFilter;
    if (expression.type !== 'SET/STRING') return dimensionInflater;

    const { extract, rest } = havingFilter.extractFromAnd((hf) => {
      if (hf instanceof ChainableExpression) {
        let hfOp = hf.op;
        let hfOperand = hf.operand;
        if (hfOperand instanceof RefExpression && hfOperand.name === label) {
          if (hfOp === 'match') return true;
          if (hfOp === 'is' || hfOp === 'in') return (hf as ChainableUnaryExpression).expression.isOp('literal');
        }
      }
      return false;
    });

    if (extract.equals(Expression.TRUE)) return dimensionInflater;

    if (extract instanceof MatchExpression) {
      return {
        dimension: {
          type: "regexFiltered",
          delegate: dimensionInflater.dimension,
          pattern: extract.regexp
        },
        inflater: dimensionInflater.inflater,
        having: rest
      };

    } else if (extract instanceof IsExpression) {
      return {
        dimension: {
          type: "listFiltered",
          delegate: dimensionInflater.dimension,
          values: [extract.expression.getLiteralValue()]
        },
        inflater: dimensionInflater.inflater,
        having: rest
      };

    } else if (extract instanceof InExpression) {
      return {
        dimension: {
          type: "listFiltered",
          delegate: dimensionInflater.dimension,
          values: extract.expression.getLiteralValue().elements
        },
        inflater: dimensionInflater.inflater,
        having: rest
      };

    }

    return dimensionInflater;
  }

  public splitToDruid(split: SplitExpression): DruidSplit {
    let leftoverHavingFilter = this.havingFilter;
    let selectedAttributes = this.getSelectedAttributes();

    if (split.isMultiSplit()) {
      let timestampLabel: string = null;
      let granularity: Druid.Granularity = null;
      let dimensions: Druid.DimensionSpec[] = [];
      let inflaters: Inflater[] = [];
      split.mapSplits((name, expression) => {
        if (!granularity && !this.limit && !this.sort) {
          // We have to add !this.limit && !this.sort because of a bug in groupBy sorting
          // Remove it when fixed https://github.com/druid-io/druid/issues/1926
          let granularityInflater = this.splitExpressionToGranularityInflater(expression, name);
          if (granularityInflater) {
            timestampLabel = name;
            granularity = granularityInflater.granularity;
            inflaters.push(granularityInflater.inflater);
            return;
          }
        }

        let { dimension, inflater, having } = this.expressionToDimensionInflaterHaving(expression, name, leftoverHavingFilter);
        leftoverHavingFilter = having;
        dimensions.push(dimension);
        if (inflater) {
          inflaters.push(inflater);
        }
      });
      return {
        queryType: 'groupBy',
        dimensions: dimensions,
        timestampLabel,
        granularity: granularity || 'all',
        leftoverHavingFilter,
        postProcess: DruidExternal.postProcessFactory(inflaters, selectedAttributes)
      };
    }

    let splitExpression = split.firstSplitExpression();
    let label = split.firstSplitName();

    // Can it be a time series?
    let granularityInflater = this.splitExpressionToGranularityInflater(splitExpression, label);
    if (granularityInflater) {
      return {
        queryType: 'timeseries',
        granularity: granularityInflater.granularity,
        leftoverHavingFilter,
        timestampLabel: label,
        postProcess: DruidExternal.postProcessFactory([granularityInflater.inflater], selectedAttributes)
      };
    }

    let dimensionInflater = this.expressionToDimensionInflaterHaving(splitExpression, label, leftoverHavingFilter);
    leftoverHavingFilter = dimensionInflater.having;

    let inflaters = [dimensionInflater.inflater].filter(Boolean);
    if (
      leftoverHavingFilter.equals(Expression.TRUE) && // There is no leftover having filter
      (this.limit || split.maxBucketNumber() < 1000) && // There is a limit (or the split range is limited)
      !this.exactResultsOnly // We do not care about exact results
    ) {
      return {
        queryType: 'topN',
        dimension: dimensionInflater.dimension,
        granularity: 'all',
        leftoverHavingFilter,
        timestampLabel: null,
        postProcess: DruidExternal.postProcessFactory(inflaters, selectedAttributes)
      };
    }

    return {
      queryType: 'groupBy',
      dimensions: [dimensionInflater.dimension],
      granularity: 'all',
      leftoverHavingFilter,
      timestampLabel: null,
      postProcess: DruidExternal.postProcessFactory(inflaters, selectedAttributes)
    };
  }


  public isMinMaxTimeExpression(applyExpression: Expression): boolean {
    if (applyExpression instanceof MinExpression || applyExpression instanceof MaxExpression) {
      return this.isTimeRef(applyExpression.expression);
    } else {
      return false;
    }
  }

  public getTimeBoundaryQueryAndPostProcess(): QueryAndPostProcess<Druid.Query> {
    const { mode, context } = this;
    let druidQuery: Druid.Query = {
      queryType: "timeBoundary",
      dataSource: this.getDruidDataSource()
    };

    if (context) {
      druidQuery.context = context;
    }

    let applies: ApplyExpression[] = null;
    if (mode === 'total') {
      applies = this.applies;
      if (applies.length === 1) {
        let loneApplyExpression = applies[0].expression;
        // Max time only
        druidQuery.bound = (loneApplyExpression as ChainableUnaryExpression).op + "Time";
        // druidQuery.queryType = "dataSourceMetadata";
      }
    } else if (mode === 'value') {
      const { valueExpression } = this;
      druidQuery.bound = (valueExpression as ChainableUnaryExpression).op + "Time";
    } else {
      throw new Error(`invalid mode '${mode}' for timeBoundary`);
    }

    return {
      query: druidQuery,
      context: { timestamp: null },
      postProcess: DruidExternal.timeBoundaryPostProcessFactory(applies)
    };
  }

  public getQueryAndPostProcess(): QueryAndPostProcess<Druid.Query> {
    const { mode, applies, sort, limit, context } = this;

    if (mode === 'total' && applies && applies.length && applies.every(apply => this.isMinMaxTimeExpression(apply.expression))) {
      return this.getTimeBoundaryQueryAndPostProcess();
    } else if (mode === 'value' && this.isMinMaxTimeExpression(this.valueExpression)) {
      return this.getTimeBoundaryQueryAndPostProcess();
    }

    let druidQuery: Druid.Query = {
      queryType: 'timeseries',
      dataSource: this.getDruidDataSource(),
      intervals: null,
      granularity: 'all'
    };

    let requesterContext: any = {
      timestamp: null,
      ignorePrefix: '!'
    };

    if (context) {
      druidQuery.context = shallowCopy(context);
    }

    // Filter
    let filterAndIntervals = new DruidFilterBuilder(this).filterToDruid(this.getQueryFilter());
    druidQuery.intervals = filterAndIntervals.intervals;
    if (filterAndIntervals.filter) {
      druidQuery.filter = filterAndIntervals.filter;
    }

    let aggregationsAndPostAggregations: AggregationsAndPostAggregations;
    switch (mode) {
      case 'raw':
        if (!this.allowSelectQueries) {
          throw new Error("to issues 'select' queries allowSelectQueries flag must be set");
        }

        let selectDimensions: Druid.DimensionSpec[] = [];
        let selectMetrics: string[] = [];
        let inflaters: Inflater[] = [];

        let timeAttribute = this.timeAttribute;
        let derivedAttributes = this.derivedAttributes;
        let selectedAttributes = this.getSelectedAttributes();
        selectedAttributes.forEach(attribute => {
          let { name, type, unsplitable } = attribute;

          if (name === timeAttribute) {
            requesterContext.timestamp = name;
          } else {
            if (unsplitable) {
              selectMetrics.push(name);
            } else {
              let derivedAttribute = derivedAttributes[name];
              if (derivedAttribute) {
                if (this.versionBefore('0.9.1')) {
                  throw new Error(`can not have derived attributes in Druid select in ${this.version}, upgrade to 0.9.1`);
                }
                let dimensionInflater = this.expressionToDimensionInflater(derivedAttribute, name);
                selectDimensions.push(dimensionInflater.dimension);
                if (dimensionInflater.inflater) inflaters.push(dimensionInflater.inflater);
                return; // No need to add default inflater
              } else {
                selectDimensions.push(name);
              }
            }
          }

          switch (type) {
            case 'BOOLEAN':
              inflaters.push(External.booleanInflaterFactory(name));
              break;

            case 'NUMBER':
              inflaters.push(External.numberInflaterFactory(name));
              break;

            case 'TIME':
              inflaters.push(External.timeInflaterFactory(name));
              break;

            case 'SET/STRING':
              inflaters.push(External.setStringInflaterFactory(name));
              break;
          }
        });

        // If dimensions or metrics are [] everything is returned, prevent this by asking for !DUMMY
        if (!selectDimensions.length) selectDimensions.push(DruidExternal.DUMMY_NAME);
        if (!selectMetrics.length) selectMetrics.push(DruidExternal.DUMMY_NAME);

        let resultLimit = limit ? limit.value : Infinity;
        druidQuery.queryType = 'select';
        druidQuery.dimensions = selectDimensions;
        druidQuery.metrics = selectMetrics;
        druidQuery.pagingSpec = {
          "pagingIdentifiers": {},
          "threshold": Math.min(resultLimit, DruidExternal.SELECT_INIT_LIMIT)
        };

        let descending = sort && sort.direction === 'descending';
        if (descending) {
          druidQuery.descending = true;
        }

        return {
          query: druidQuery,
          context: requesterContext,
          postProcess: DruidExternal.postProcessFactory(inflaters, selectedAttributes),
          next: DruidExternal.selectNextFactory(resultLimit, descending)
        };

      case 'value':
        aggregationsAndPostAggregations = new DruidAggregationBuilder(this).makeAggregationsAndPostAggregations([this.toValueApply()]);
        if (aggregationsAndPostAggregations.aggregations.length) {
          druidQuery.aggregations = aggregationsAndPostAggregations.aggregations;
        }
        if (aggregationsAndPostAggregations.postAggregations.length) {
          druidQuery.postAggregations = aggregationsAndPostAggregations.postAggregations;
        }

        return {
          query: druidQuery,
          context: requesterContext,
          postProcess: DruidExternal.valuePostProcess
        };

      case 'total':
        aggregationsAndPostAggregations = new DruidAggregationBuilder(this).makeAggregationsAndPostAggregations(this.applies);
        if (aggregationsAndPostAggregations.aggregations.length) {
          druidQuery.aggregations = aggregationsAndPostAggregations.aggregations;
        }
        if (aggregationsAndPostAggregations.postAggregations.length) {
          druidQuery.postAggregations = aggregationsAndPostAggregations.postAggregations;
        }

        return {
          query: druidQuery,
          context: requesterContext,
          postProcess: DruidExternal.totalPostProcessFactory(applies)
        };

      case 'split':
        // Split
        let split = this.getQuerySplit();
        let splitSpec = this.splitToDruid(split);
        druidQuery.queryType = splitSpec.queryType;
        druidQuery.granularity = splitSpec.granularity;
        if (splitSpec.dimension) druidQuery.dimension = splitSpec.dimension;
        if (splitSpec.dimensions) druidQuery.dimensions = splitSpec.dimensions;
        let leftoverHavingFilter = splitSpec.leftoverHavingFilter;
        let timestampLabel = splitSpec.timestampLabel;
        requesterContext.timestamp = timestampLabel;
        let postProcess = splitSpec.postProcess;

        // Apply
        aggregationsAndPostAggregations = new DruidAggregationBuilder(this).makeAggregationsAndPostAggregations(applies);
        if (aggregationsAndPostAggregations.aggregations.length) {
          druidQuery.aggregations = aggregationsAndPostAggregations.aggregations;
        } else {
          // Druid hates not having aggregates so add a dummy count
          druidQuery.aggregations = [{ name: DruidExternal.DUMMY_NAME, type: "count" }];
        }
        if (aggregationsAndPostAggregations.postAggregations.length) {
          druidQuery.postAggregations = aggregationsAndPostAggregations.postAggregations;
        }

        // Combine
        switch (druidQuery.queryType) {
          case 'timeseries':
            if (sort && (sort.direction !== 'ascending' || !split.hasKey(sort.refName()))) {
              throw new Error('can not sort within timeseries query');
            }
            if (limit) {
              throw new Error('can not limit within timeseries query');
            }

            // Plywood's concept of splits does not allocate buckets for which there is no data.
            if (!druidQuery.context || !hasOwnProp(druidQuery.context, 'skipEmptyBuckets')) {
              druidQuery.context = druidQuery.context || {};
              druidQuery.context.skipEmptyBuckets = "true"; // This needs to be the string "true" to work with older Druid versions
            }
            break;

          case 'topN':
            let metric: Druid.TopNMetricSpec;
            if (sort) {
              let inverted: boolean;
              if (this.sortOnLabel()) {
                if (expressionNeedsAlphaNumericSort(split.firstSplitExpression())) {
                  metric = { type: 'alphaNumeric' };
                } else {
                  metric = { type: 'lexicographic' };
                }
                inverted = sort.direction === 'descending';
              } else {
                metric = sort.refName();
                inverted = sort.direction === 'ascending';
              }

              if (inverted) {
                metric = { type: "inverted", metric: metric };
              }

            } else {
              metric = { type: 'lexicographic' };
            }
            druidQuery.metric = metric;
            druidQuery.threshold = limit ? limit.value : 1000;
            break;

          case 'groupBy':
            let orderByColumn: Druid.OrderByColumnSpecFull = null;
            if (sort) {
              let col = sort.refName();
              orderByColumn = {
                dimension: col,
                direction: sort.direction
              };
              if (this.sortOnLabel()) {
                if (expressionNeedsAlphaNumericSort(split.splits[col])) {
                  orderByColumn.dimensionOrder = 'alphanumeric';
                }
              }
            } else { // Going to sortOnLabel implicitly
              // Find the first non primary time key
              let splitKeys = split.keys.filter(k => k !== timestampLabel);
              if (!splitKeys.length) throw new Error('could not find order by column for group by');
              let splitKey = splitKeys[0];
              let keyExpression = split.splits[splitKey];
              orderByColumn = {
                dimension: splitKey
              };
              if (expressionNeedsAlphaNumericSort(keyExpression)) {
                orderByColumn.dimensionOrder = 'alphanumeric';
              }
            }

            druidQuery.limitSpec = {
              type: "default",
              columns: [orderByColumn || split.firstSplitName()]
            };
            if (limit) {
              druidQuery.limitSpec.limit = limit.value;
            }
            if (!leftoverHavingFilter.equals(Expression.TRUE)) {
              druidQuery.having = new DruidHavingFilterBuilder(this).filterToHavingFilter(leftoverHavingFilter);
            }
            break;
        }

        return {
          query: druidQuery,
          context: requesterContext,
          postProcess: postProcess
        };

      default:
        throw new Error("can not get query for: " + this.mode);
    }
  }

  protected getIntrospectAttributesWithSegmentMetadata(): Promise<Attributes> {
    let { requester, timeAttribute, context } = this;

    let query: Druid.Query = {
      queryType: 'segmentMetadata',
      dataSource: this.getDruidDataSource(),
      merge: true,
      analysisTypes: ['aggregators'],
      lenientAggregatorMerge: true
    };

    if (context) {
      query.context = context;
    }

    if (this.versionBefore('0.9.0')) {
      query.analysisTypes = [];
      delete query.lenientAggregatorMerge;
    }

    if (this.versionBefore('0.9.2') && (query.dataSource as Druid.DataSourceFull).type === 'union') {
      // https://github.com/druid-io/druid/issues/3128
      query.dataSource = (query.dataSource as Druid.DataSourceFull).dataSources[0];
    }

    return toArray(requester({ query }))
      .then(DruidExternal.segmentMetadataPostProcessFactory(timeAttribute));
  }

  protected getIntrospectAttributesWithGet(): Promise<Attributes> {
    let { requester, timeAttribute } = this;

    return toArray(requester({
      query: {
        queryType: 'introspect',
        dataSource: this.getDruidDataSource()
      }
    }))
      .then(DruidExternal.introspectPostProcessFactory(timeAttribute));
  }

  protected getIntrospectAttributes(): Promise<Attributes> {
    switch (this.introspectionStrategy) {
      case 'segment-metadata-fallback':
        return this.getIntrospectAttributesWithSegmentMetadata()
          .catch((err: Error) => {
            if (err.message.indexOf("querySegmentSpec can't be null") === -1) throw err;
            return this.getIntrospectAttributesWithGet();
          });

      case 'segment-metadata-only':
        return this.getIntrospectAttributesWithSegmentMetadata();

      case 'datasource-get':
        return this.getIntrospectAttributesWithGet();

      default:
        throw new Error('invalid introspectionStrategy');
    }
  }

}

External.register(DruidExternal);
