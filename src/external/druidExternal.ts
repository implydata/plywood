module Plywood {
  const UNIQUE_REGEXP = /^unique_|_unique$/;
  const HISTOGRAM_REGEXP = /^hist_|_hist$/;

  const DUMMY_NAME = '!DUMMY';

  const TIME_PART_TO_FORMAT: Lookup<string> = {
    SECOND_OF_MINUTE: "s",
    SECOND_OF_HOUR: "m'*60+'s",
    SECOND_OF_DAY: "H'*60+'m'*60+'s",
    SECOND_OF_WEEK: "e'*24+H'*60+'m'*60+'s",
    SECOND_OF_MONTH: "d'*24+H'*60+'m'*60+'s", // Off by one on d
    SECOND_OF_YEAR: "D'*24+H'*60+'m'*60+'s",

    MINUTE_OF_HOUR: "m",
    MINUTE_OF_DAY: "H'*60+'m",
    MINUTE_OF_WEEK: "e'*24+H'*60+'m",
    MINUTE_OF_MONTH: "d'*24+H'*60+'m", // Off by one on d
    MINUTE_OF_YEAR: "D'*24+H'*60+'m",

    HOUR_OF_DAY: "H",
    HOUR_OF_WEEK: "e'*24+H",
    HOUR_OF_MONTH: "d'*24+H", // Off by one on d
    HOUR_OF_YEAR: "D'*24+H",

    DAY_OF_WEEK: "e",
    DAY_OF_MONTH: "d", // Off by one on d
    DAY_OF_YEAR: "D",

    WEEK_OF_MONTH: null,
    WEEK_OF_YEAR: "w"
  };

  function simpleMath(exprStr: string): int {
    if (String(exprStr) === 'null') return null;
    var parts = exprStr.split(/(?=[*+])/);
    var acc = parseInt(parts.shift(), 10);
    for (let part of parts) {
      var v = parseInt(part.substring(1), 10);
      acc = part[0] === '*' ? acc * v : acc + v;
    }
    return acc;
  }

  export interface CustomDruidAggregation {
    aggregation: Druid.Aggregation;
    accessType?: string;
  }

  export type CustomDruidAggregations = Lookup<CustomDruidAggregation>;

  function customAggregationsEqual(customA: CustomDruidAggregations, customB: CustomDruidAggregations): boolean {
    return JSON.stringify(customA) === JSON.stringify(customB); // ToDo: fill this in;
  }

  export interface DruidFilterAndIntervals {
    filter: Druid.Filter;
    intervals: string[];
  }

  export interface AggregationsAndPostAggregations {
    aggregations: Druid.Aggregation[];
    postAggregations: Druid.PostAggregation[];
  }

  export interface DruidSplit extends AggregationsAndPostAggregations {
    queryType: string;
    granularity: any;
    dimension?: string | Druid.DimensionSpec;
    dimensions?: any[];
    postProcess: PostProcess;
  }

  interface LabelProcess {
    (v: any): any;
  }

  function cleanDatumInPlace(datum: Datum): void {
    if (hasOwnProperty(datum, DUMMY_NAME)) {
      delete datum[DUMMY_NAME];
    }
  }

  function correctTimeBoundaryResult(result: Druid.TimeBoundaryResults): boolean {
    return Array.isArray(result) && result.length === 1 && typeof result[0].result === 'object';
  }

  function correctTimeseriesResult(result: Druid.TimeseriesResults): boolean {
    return Array.isArray(result) && (result.length === 0 || typeof result[0].result === 'object');
  }

  function correctTopNResult(result: Druid.DruidResults): boolean {
    return Array.isArray(result) && (result.length === 0 || Array.isArray(result[0].result));
  }

  function correctGroupByResult(result: Druid.GroupByResults): boolean {
    return Array.isArray(result) && (result.length === 0 || typeof result[0].event === 'object');
  }

  function correctSelectResult(result: Druid.SelectResults): boolean {
    return Array.isArray(result) && (result.length === 0 || typeof result[0].result === 'object');
  }

  function postProcessTimeBoundaryFactory(applies: ApplyAction[]): PostProcess {
    return (res: Druid.TimeBoundaryResults): Dataset => {
      if (!correctTimeBoundaryResult(res)) {
        var err = new Error("unexpected result from Druid (timeBoundary)");
        (<any>err).result = res; // ToDo: special error type
        throw err;
      }

      var result = res[0].result;
      var datum: Datum = {};
      for (let apply of applies) {
        let name = apply.name;
        let aggregate = (<ChainExpression>apply.expression).actions[0].action;
        if (typeof result === 'string') {
          datum[name] = new Date(result);
        } else {
          if (aggregate === 'max') {
            datum[name] = new Date(<string>(result['maxIngestedEventTime'] || result['maxTime']));
          } else {
            datum[name] = new Date(<string>(result['minTime']));
          }
        }
      }

      return new Dataset({ data: [datum] });
    };
  }

  function makeZeroDatum(applies: ApplyAction[]): Datum {
    var newDatum = Object.create(null);
    for (var apply of applies) {
      var applyName = apply.name;
      if (applyName[0] === '_') continue;
      newDatum[applyName] = 0;
    }
    return newDatum;
  }

  function postProcessTotalFactory(applies: ApplyAction[]) {
    return (res: Druid.TimeseriesResults): Dataset => {
      if (!correctTimeseriesResult(res)) {
        var err = new Error("unexpected result from Druid (all)");
        (<any>err).result = res; // ToDo: special error type
        throw err;
      }
      if (!res.length) {
        return new Dataset({ data: [makeZeroDatum(applies)] });
      }
      return new Dataset({ data: [res[0].result] });
    };
  }

  function postProcessTimeseriesFactory(duration: Duration, timezone: Timezone, label: string): PostProcess {
    return (res: Druid.TimeseriesResults): Dataset => {
      if (!correctTimeseriesResult(res)) {
        var err = new Error("unexpected result from Druid (timeseries)");
        (<any>err).result = res; // ToDo: special error type
        throw err;
      }
      //var warp = split.warp;
      //var warpDirection = split.warpDirection;
      var canonicalDurationLengthAndThenSome = duration.getCanonicalLength() * 1.5;
      return new Dataset({
        data: res.map((d: any, i: int) => {
          var rangeStart = new Date(d.timestamp);
          var next = res[i + 1];
          var nextTimestamp: Date;
          if (next) {
            nextTimestamp = new Date(next.timestamp);
          }

          var rangeEnd = (nextTimestamp && rangeStart.valueOf() < nextTimestamp.valueOf() &&
                          nextTimestamp.valueOf() - rangeStart.valueOf() < canonicalDurationLengthAndThenSome) ?
                          nextTimestamp : duration.move(rangeStart, timezone, 1);

          //if (warp) {
          //  rangeStart = warp.move(rangeStart, timezone, warpDirection);
          //  range//End = warp.move(rangeEnd, timezone, warpDirection);
          //}

          var datum: Datum = d.result;
          cleanDatumInPlace(datum);
          datum[label] = new TimeRange({ start: rangeStart, end: rangeEnd });
          return datum;
        })
      });
    }
  }

  function postProcessNumberBucketFactory(rangeSize: number): LabelProcess {
    return (v: any) => {
      var start = Number(v);
      return new NumberRange({
        start: start,
        end: safeAdd(start, rangeSize)
      });
    }
  }

  function postProcessTopNFactory(labelProcess: LabelProcess, label: string): PostProcess {
    return (res: Druid.DruidResults): Dataset => {
      if (!correctTopNResult(res)) {
        var err = new Error("unexpected result from Druid (topN)");
        (<any>err).result = res; // ToDo: special error type
        throw err;
      }
      var data = res.length ? res[0].result : [];
      if (labelProcess) {
        return new Dataset({
          data: data.map((d: Datum) => {
            cleanDatumInPlace(d);
            var v: any = d[label];
            if (String(v) === "null") {
              v = null;
            } else {
              v = labelProcess(v);
            }
            d[label] = v;
            return d;
          })
        });
      } else {
        return new Dataset({
          data: data.map((d: Datum) => {
            cleanDatumInPlace(d);
            return d;
          })
        });
      }
    };
  }

  function postProcessGroupBy(res: Druid.GroupByResults): Dataset {
    if (!correctGroupByResult(res)) {
      var err = new Error("unexpected result from Druid (groupBy)");
      (<any>err).result = res; // ToDo: special error type
      throw err;
    }
    return new Dataset({
      data: res.map(r => {
        var datum = r.event;
        cleanDatumInPlace(datum);
        return datum;
      })
    });
  }

  function postProcessSelect(res: Druid.SelectResults): Dataset {
    if (!correctSelectResult(res)) {
      var err = new Error("unexpected result from Druid (select)");
      (<any>err).result = res; // ToDo: special error type
      throw err;
    }
    return new Dataset({
      data: res[0].result.events.map(event => event.event)
    });
  }

  function postProcessIntrospectFactory(timeAttribute: string): IntrospectPostProcess {
    return (res: Druid.DatasourceIntrospectResult): Attributes => {
      var attributes: Attributes = [
        new AttributeInfo({ name: timeAttribute, type: 'TIME' })
      ];
      res.dimensions.forEach(dimension => {
        attributes.push(new AttributeInfo({ name: dimension, type: 'STRING' }));
      });
      res.metrics.forEach(metric => {
        if (UNIQUE_REGEXP.test(metric)) {
          attributes.push(new UniqueAttributeInfo({ name: metric }));
        } else if (HISTOGRAM_REGEXP.test(metric)) {
          attributes.push(new HistogramAttributeInfo({ name: metric }));
        } else {
          attributes.push(new AttributeInfo({ name: metric, type: 'NUMBER', filterable: false, splitable: false }));
        }
      });
      return attributes;
    }
  }

  function postProcessSegmentMetadataFactory(timeAttribute: string): IntrospectPostProcess {
    return (res: Druid.SegmentMetadataResults): Attributes => {
      var attributes: Attributes = [];

      var columns = res[0].columns;
      for (var name in columns) {
        if (!hasOwnProperty(columns, name)) continue;

        if (name === '__time') {
          attributes.push(new AttributeInfo({ name: timeAttribute, type: 'TIME' }));
        } else {
          var columnData = columns[name];
          if (columnData.type === "STRING") {
            if (UNIQUE_REGEXP.test(name)) {
              attributes.push(new UniqueAttributeInfo({ name }));
            } else if (HISTOGRAM_REGEXP.test(name)) {
              attributes.push(new HistogramAttributeInfo({ name }));
            } else {
              attributes.push(new AttributeInfo({ name, type: 'STRING' }));
            }
          } else {
            attributes.push(new AttributeInfo({ name, type: 'NUMBER', filterable: false, splitable: false }));
          }
        }
      }
      return attributes;
    }
  }

  export class DruidExternal extends External {
    static type = 'DATASET';

    static TRUE_INTERVAL = ["1000-01-01/3000-01-01"];
    static FALSE_INTERVAL = ["1000-01-01/1000-01-02"];

    static fromJS(datasetJS: any): DruidExternal {
      var value: ExternalValue = External.jsToValue(datasetJS);
      value.dataSource = datasetJS.dataSource;
      value.timeAttribute = datasetJS.timeAttribute;
      value.customAggregations = datasetJS.customAggregations || {};
      value.allowEternity = Boolean(datasetJS.allowEternity);
      value.allowSelectQueries = Boolean(datasetJS.allowSelectQueries);
      value.exactResultsOnly = Boolean(datasetJS.exactResultsOnly);
      value.useSegmentMetadata = Boolean(datasetJS.useSegmentMetadata);
      value.context = datasetJS.context;
      return new DruidExternal(value);
    }


    public dataSource: string | string[];
    public timeAttribute: string;
    public customAggregations: CustomDruidAggregations;
    public allowEternity: boolean;
    public allowSelectQueries: boolean;
    public exactResultsOnly: boolean;
    public useSegmentMetadata: boolean;
    public context: Lookup<any>;

    constructor(parameters: ExternalValue) {
      super(parameters, dummyObject);
      this._ensureEngine("druid");
      this.dataSource = parameters.dataSource;
      this.timeAttribute = parameters.timeAttribute;
      this.customAggregations = parameters.customAggregations;
      if (typeof this.timeAttribute !== 'string') throw new Error("must have a timeAttribute");
      this.allowEternity = parameters.allowEternity;
      this.allowSelectQueries = parameters.allowSelectQueries;
      this.exactResultsOnly = parameters.exactResultsOnly;
      this.useSegmentMetadata = parameters.useSegmentMetadata;
      this.context = parameters.context;
    }

    public valueOf(): ExternalValue {
      var value: ExternalValue = super.valueOf();
      value.dataSource = this.dataSource;
      value.timeAttribute = this.timeAttribute;
      value.customAggregations = this.customAggregations;
      value.allowEternity = this.allowEternity;
      value.allowSelectQueries = this.allowSelectQueries;
      value.exactResultsOnly = this.exactResultsOnly;
      value.useSegmentMetadata = this.useSegmentMetadata;
      value.context = this.context;
      return value;
    }

    public toJS(): ExternalJS {
      var js: ExternalJS = super.toJS();
      js.dataSource = this.dataSource;
      js.timeAttribute = this.timeAttribute;
      if (Object.keys(this.customAggregations).length) js.customAggregations = this.customAggregations;
      if (this.allowEternity) js.allowEternity = true;
      if (this.allowSelectQueries) js.allowSelectQueries = true;
      if (this.exactResultsOnly) js.exactResultsOnly = true;
      if (this.useSegmentMetadata) js.useSegmentMetadata = true;
      js.context = this.context;
      return js;
    }

    public equals(other: DruidExternal): boolean {
      return super.equals(other) &&
        String(this.dataSource) === String(other.dataSource) &&
        this.timeAttribute === other.timeAttribute &&
        customAggregationsEqual(this.customAggregations, other.customAggregations) &&
        this.allowEternity === other.allowEternity &&
        this.allowSelectQueries === other.allowSelectQueries &&
        this.exactResultsOnly === other.exactResultsOnly &&
        this.useSegmentMetadata === other.useSegmentMetadata &&
        this.context === other.context;
    }

    public getId(): string {
      return super.getId() + ':' + this.dataSource;
    }

    // -----------------

    public canHandleFilter(ex: Expression): boolean {
      return true;
    }

    public canHandleTotal(): boolean {
      return true;
    }

    public canHandleSplit(ex: Expression): boolean {
      return true;
    }

    public canHandleApply(ex: Expression): boolean {
      return true;
    }

    public canHandleSort(sortAction: SortAction): boolean {
      var split = this.split;
      if (split.isMultiSplit()) return true;
      var splitExpression = split.firstSplitExpression();
      var label = split.firstSplitName();
      if (splitExpression instanceof ChainExpression) {
        if (splitExpression.actions.length === 1 && splitExpression.actions[0].action === 'timeBucket') {
          if (sortAction.direction !== 'ascending') return false;
          var sortExpression = sortAction.expression;
          if (sortExpression instanceof RefExpression) {
            return sortExpression.name === label;
          } else {
            return false;
          }
        } else {
          return true
        }
      } else {
        return true;
      }
    }

    public canHandleLimit(limitAction: LimitAction): boolean {
      var split = this.split;
      if (split.isMultiSplit()) return true;
      var splitExpression = split.firstSplitExpression();
      if (splitExpression instanceof ChainExpression) {
        if (splitExpression.getExpressionPattern('concat')) return true;
        if (splitExpression.actions.length !== 1) return false;
        return splitExpression.actions[0].action !== 'timeBucket';
      } else {
        return true;
      }
    }

    public canHandleHavingFilter(ex: Expression): boolean {
      return !this.limit;
    }

    // -----------------

    public getDruidDataSource(): string | Druid.DataSource {
      var dataSource = this.dataSource;
      if (Array.isArray(dataSource)) {
        return {
          type: "union",
          dataSources: <string[]>dataSource
        };
      } else {
        return <string>dataSource;
      }
    }

    public canUseNativeAggregateFilter(filter: Expression): boolean {
      if (filter instanceof ChainExpression) {
        var pattern: Expression[];
        if (pattern = (filter.getExpressionPattern('and') || filter.getExpressionPattern('or'))) {
          return pattern.every(ex => {
            return this.canUseNativeAggregateFilter(ex);
          }, this);
        }

        var prefix: Expression;
        if (prefix = filter.popAction('not')) {
          return this.canUseNativeAggregateFilter(prefix);
        }

        var actions = filter.actions;
        if (actions.length !== 1) return false;
        var firstAction = actions[0];
        return filter.expression.isOp('ref') &&
          (firstAction.action === 'is' || firstAction.action === 'in') &&
          firstAction.expression.isOp('literal');
      }
      return false;
    }

    public timelessFilterToDruid(filter: Expression): Druid.Filter {
      if (filter.type !== 'BOOLEAN') throw new Error("must be a BOOLEAN filter");

      var pattern: Expression[];
      if (pattern = filter.getExpressionPattern('and')) {
        return {
          type: 'and',
          fields: pattern.map(this.timelessFilterToDruid, this)
        };
      }
      if (pattern = filter.getExpressionPattern('or')) {
        return {
          type: 'or',
          fields: pattern.map(this.timelessFilterToDruid, this)
        };
      }

      var attributeInfo: AttributeInfo;
      if (filter instanceof LiteralExpression) {
        if (filter.value === true) {
          return null;
        } else {
          throw new Error("should never get here");
        }
      } else if (filter instanceof ChainExpression) {
        var prefix: Expression;
        if (prefix = filter.popAction('not')) {
          return {
            type: 'not',
            field: this.timelessFilterToDruid(prefix)
          };
        }

        var lhs = filter.expression;
        var actions = filter.actions;
        if (actions.length !== 1) throw new Error(`can not convert ${filter.toString()} to Druid interval`);
        var filterAction = actions[0];
        var rhs = filterAction.expression;

        if (filterAction instanceof IsAction) {
          if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
            attributeInfo = this.getAttributesInfo(lhs.name);
            return {
              type: "selector",
              dimension: lhs.name,
              value: attributeInfo.serialize(rhs.value)
            };
          } else {
            throw new Error("can not convert " + filter.toString() + " to Druid filter");
          }

        } else if (filterAction instanceof InAction) {
          if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
            attributeInfo = this.getAttributesInfo(lhs.name);
            var rhsType = rhs.type;
            if (rhsType === 'SET/STRING' || rhsType === 'SET/NULL') {
              var fields = rhs.value.elements.map((value: string) => {
                return {
                  type: "selector",
                  dimension: lhs.name,
                  value: attributeInfo.serialize(value)
                }
              });

              if (fields.length === 1) return fields[0];
              return { type: "or", fields };
            } else if (rhsType === 'NUMBER_RANGE') {
              var range: NumberRange = rhs.value;
              var r0 = range.start;
              var r1 = range.end;
              return {
                type: "javascript",
                dimension: lhs.name,
                "function": "function(a) { a = Number(a); return " + r0 + " <= a && a < " + r1 + "; }"
              };
            } else if (rhsType === 'TIME_RANGE') {
              throw new Error("can not time filter on non-primary time dimension");
            } else {
              throw new Error("not supported " + rhsType);
            }
          } else {
            throw new Error("can not convert " + filter.toString() + " to Druid filter");
          }

        } else if (filterAction instanceof MatchAction) {
          if (lhs instanceof RefExpression) {
            return {
              type: "regex",
              dimension: lhs.name,
              pattern: filterAction.regexp
            };
          } else {
            throw new Error("can not convert " + filter.toString() + " to Druid filter");
          }

        } else if (filterAction instanceof ContainsAction) {
          if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
            if (filterAction.compare === ContainsAction.IGNORE_CASE) {
              return {
                type: "search",
                dimension: lhs.name,
                query: {
                  type: "fragment",
                  values: [rhs.value]
                }
              };
            } else {
              return {
                type: "javascript",
                dimension: lhs.name,
                "function": filter.getJSFn('d')
              };
            }
          } else {
            throw new Error("can not convert " + filter.toString() + " to Druid filter");
          }

        }

      } else {
        throw new Error("could not convert filter " + filter.toString() + " to Druid filter");
      }
    }

    public timeFilterToIntervals(filter: Expression): string[] {
      if (filter.type !== 'BOOLEAN') throw new Error("must be a BOOLEAN filter");

      if (filter instanceof LiteralExpression) {
        if (!filter.value) return DruidExternal.FALSE_INTERVAL;
        if (!this.allowEternity) throw new Error('must filter on time unless the allowEternity flag is set');
        return DruidExternal.TRUE_INTERVAL;

      } else if (filter instanceof ChainExpression) {
        var lhs = filter.expression;
        var actions = filter.actions;
        if (actions.length !== 1) throw new Error(`can not convert ${filter.toString()} to Druid interval`);
        var filterAction = actions[0];

        if (filterAction instanceof InAction) {
          var rhs = filterAction.expression;

          if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
            var timeRanges: TimeRange[];
            var rhsType = rhs.type;
            if (rhsType === 'SET/TIME_RANGE') {
              timeRanges = rhs.value.elements;
            } else if (rhsType === 'TIME_RANGE') {
              timeRanges = [rhs.value];
            } else {
              throw new Error("not supported " + rhsType + " for time filtering");
            }

            return timeRanges.map(timeRange => timeRange.toInterval());
          } else {
            throw new Error(`can not convert ${filter.toString()} to Druid interval`);
          }
        }
        /*
          else if (filter instanceof AndExpression) {
          var mergedTimePart = AndExpression.mergeTimePart(filter);
          if (mergedTimePart) {
            return this.timeFilterToIntervals(mergedTimePart);
          } else {
            throw new Error(`can not convert ${filter.toString()} to Druid interval`);
          }
        }
        */

      } else {
        throw new Error(`can not convert ${filter.toString()} to Druid interval`);
      }
    }

    public filterToDruid(filter: Expression): DruidFilterAndIntervals {
      if (filter.type !== 'BOOLEAN') throw new Error("must be a BOOLEAN filter");

      if (filter.equals(Expression.FALSE)) {
        return {
          intervals: DruidExternal.FALSE_INTERVAL,
          filter: null
        }
      } else {
        var sep = filter.separateViaAnd(this.timeAttribute);
        if (!sep) throw new Error("could not separate time filter in " + filter.toString());

        return {
          intervals: this.timeFilterToIntervals(sep.included),
          filter: this.timelessFilterToDruid(sep.excluded)
        }
      }
    }

    public getRangeBucketingDimension(attributeInfo: RangeAttributeInfo, numberBucket: NumberBucketAction): Druid.ExtractionFn {
      var regExp = attributeInfo.getMatchingRegExpString();
      if (numberBucket && numberBucket.offset === 0 && numberBucket.size === attributeInfo.rangeSize) numberBucket = null;
      var bucketing = '';
      if (numberBucket) {
        bucketing = 's=' + continuousFloorExpression('s', 'Math.floor', numberBucket.size, numberBucket.offset) + ';';
      }
      return {
        type: "javascript",
        'function':
`function(d) {
var m = d.match(${regExp});
if(!m) return 'null';
var s = +m[1];
if(!(Math.abs(+m[2] - s - ${attributeInfo.rangeSize}) < 1e-6)) return 'null'; ${bucketing}
var parts = String(Math.abs(s)).split('.');
parts[0] = ('000000000' + parts[0]).substr(-10);
return (start < 0 ?'-':'') + parts.join('.');
}`
      };
    }

    public isTimeRef(ex: Expression) {
      return ex instanceof RefExpression && ex.name === this.timeAttribute;
    }

    public splitToDruid(): DruidSplit {
      var queryType: string;
      var dimension: string | Druid.DimensionSpec = null;
      var dimensions: any[] = null;
      var granularity: any = 'all';
      var aggregations: Druid.Aggregation[] = null;
      var postAggregations: Druid.PostAggregation[] = null;
      var postProcess: PostProcess = null;

      var split = this.split;
      if (split.isMultiSplit()) {
        throw new Error('not implemented multi-dim split yet');
      }

      var label = split.firstSplitName();
      var splitExpression = split.firstSplitExpression();

      if (splitExpression instanceof RefExpression) {
        var dimensionSpec = (splitExpression.name === label) ?
          label : {type: "default", dimension: splitExpression.name, outputName: label};

        if (this.havingFilter.equals(Expression.TRUE) && this.limit && !this.exactResultsOnly) {
          var attributeInfo = this.getAttributesInfo(splitExpression.name);
          queryType = 'topN';
          if (attributeInfo instanceof RangeAttributeInfo) {
            dimension = {
              type: "extraction",
              dimension: splitExpression.name,
              outputName: label,
              extractionFn: this.getRangeBucketingDimension(attributeInfo, null)
            };
            postProcess = postProcessTopNFactory(postProcessNumberBucketFactory(attributeInfo.rangeSize), label);
          } else {
            dimension = dimensionSpec;
            postProcess = postProcessTopNFactory(null, null);
          }

        } else {
          queryType = 'groupBy';
          dimensions = [dimensionSpec];
          postProcess = postProcessGroupBy;

        }

      } else if (splitExpression instanceof ChainExpression) {
        var pattern: Expression[];
        if (pattern = splitExpression.getExpressionPattern('concat')) {
          var concatRef: RefExpression = null;

          for (var i = 0; i < pattern.length; i++) {
            var p = pattern[i];
            if (p instanceof RefExpression) {
              if (concatRef) throw new Error(`can not currently have multiple references in a concat expression: ${splitExpression.toString()}`);
              concatRef = p;
            } else if (!(p instanceof LiteralExpression)) {
              throw new Error(`can not have '${p.toString()}' inside a concat`);
            }
          }

          var concatDimension = {
            type: "extraction",
            dimension: concatRef.name,
            outputName: label,
            extractionFn: {
              type: "javascript",
              'function': splitExpression.getJSFn('d'),
              injective: true
            }
          };

          if (this.havingFilter.equals(Expression.TRUE) && this.limit && !this.exactResultsOnly) {
            queryType = 'topN';
            dimension = concatDimension;
            postProcess = postProcessTopNFactory(null, null);
          } else {
            queryType = 'groupBy';
            dimensions = [concatDimension];
            postProcess = postProcessGroupBy;
          }
        } else {

          var refExpression = <RefExpression>splitExpression.expression;
          if (refExpression.op !== 'ref') throw new Error(`can not convert complex: ${refExpression.toString()}`);
          var actions = splitExpression.actions;
          if (actions.length !== 1) throw new Error('can not convert expression: ' + splitExpression.toString());
          var splitAction = actions[0];

          if (splitAction instanceof SubstrAction) {
            var substrDimension = {
              type: "extraction",
              dimension: refExpression.name,
              outputName: label,
              extractionFn: {
                type: "javascript",
                'function': splitExpression.getJSFn('d')
              }
            };

            if (this.havingFilter.equals(Expression.TRUE) && this.limit && !this.exactResultsOnly) {
              queryType = 'topN';
              dimension = substrDimension;
              postProcess = postProcessTopNFactory(null, null);
            } else {
              queryType = 'groupBy';
              dimensions = [substrDimension];
              postProcess = postProcessGroupBy;
            }

          } else if (splitAction instanceof TimePartAction) {
            queryType = 'topN';
            var format = TIME_PART_TO_FORMAT[splitAction.part];
            if (!format) throw new Error(`unsupported part in timePart expression ${splitAction.part}`);
            dimension = {
              type: "extraction",
              dimension: refExpression.name === this.timeAttribute ? '__time' : refExpression.name,
              outputName: label,
              extractionFn: {
                type: "timeFormat",
                format: format,
                timeZone: splitAction.timezone.toString(),
                locale: "en-US"
              }
            };
            postProcess = postProcessTopNFactory(simpleMath, label);

          } else if (splitAction instanceof TimeBucketAction) {
            if (!this.isTimeRef(refExpression)) {
              throw new Error(`can not convert complex time bucket: ${refExpression.toString()}`);
            }
            queryType = 'timeseries';
            granularity = {
              type: "period",
              period: splitAction.duration.toString(),
              timeZone: splitAction.timezone.toString()
            };
            postProcess = postProcessTimeseriesFactory(splitAction.duration, splitAction.timezone, label);

          } else if (splitAction instanceof NumberBucketAction) {
            var attributeInfo = this.getAttributesInfo(refExpression.name);
            queryType = "topN";
            if (attributeInfo.type === 'NUMBER') {
              var floorExpression = continuousFloorExpression("d", "Math.floor", splitAction.size, splitAction.offset);
              dimension = {
                type: "extraction",
                dimension: refExpression.name,
                outputName: label,
                extractionFn: {
                  type: "javascript",
                  'function': `function(d){d=Number(d); if(isNaN(d)) return 'null'; return ${floorExpression};}`
                }
              };
              postProcess = postProcessTopNFactory(Number, label);

            } else if (attributeInfo instanceof RangeAttributeInfo) {
              dimension = {
                type: "extraction",
                dimension: refExpression.name,
                outputName: label,
                extractionFn: this.getRangeBucketingDimension(<RangeAttributeInfo>attributeInfo, splitAction)
              };
              postProcess = postProcessTopNFactory(postProcessNumberBucketFactory(splitAction.size), label);

            } else if (attributeInfo instanceof HistogramAttributeInfo) {
              if (this.exactResultsOnly) {
                throw new Error("can not use approximate histograms in exactResultsOnly mode");
              }
              var aggregation: Druid.Aggregation = {
                type: "approxHistogramFold",
                fieldName: refExpression.name
              };
              if (splitAction.lowerLimit != null) {
                aggregation.lowerLimit = splitAction.lowerLimit;
              }
              if (splitAction.upperLimit != null) {
                aggregation.upperLimit = splitAction.upperLimit;
              }
              //var options = split.options || {};
              //if (hasOwnProperty(options, 'druidResolution')) {
              //  aggregation.resolution = options['druidResolution'];
              //}

              aggregations = [aggregation];
              postAggregations = [{
                type: "buckets",
                name: "histogram",
                fieldName: "histogram",
                bucketSize: splitAction.size,
                offset: splitAction.offset
              }];
            }

          } else {
            throw new Error('can not convert given split action: ' + splitExpression.toString());
          }
        }

      } else {
        throw new Error('can not convert expression: ' + splitExpression.toString());
      }

      return {
        queryType: queryType,
        granularity: granularity,
        dimension: dimension,
        dimensions: dimensions,
        aggregations: aggregations,
        postAggregations: postAggregations,
        postProcess: postProcess
      };
    }

    public operandsToArithmetic(operands: Expression[], fn: string, aggregations: Druid.Aggregation[]): Druid.PostAggregation {
      if (operands.length === 1) {
        return this.expressionToPostAggregation(operands[0], aggregations);
      } else {
        return {
          type: 'arithmetic',
          fn: fn,
          fields: operands.map(operand => {
            return this.expressionToPostAggregation(operand, aggregations);
          }, this)
        };
      }
    }

    public getAccessTypeForAggregation(aggregationType: string): string {
      if (aggregationType === 'hyperUnique' || aggregationType === 'cardinality') return 'hyperUniqueCardinality';

      var customAggregations = this.customAggregations;
      for (var customName in customAggregations) {
        if (!hasOwnProperty(customAggregations, customName)) continue;
        var customAggregation = customAggregations[customName];
        if (customAggregation.aggregation.type === aggregationType) {
          return customAggregation.accessType || 'fieldAccess';
        }
      }

      return 'fieldAccess';
    }

    public getAccessType(aggregations: Druid.Aggregation[], aggregationName: string): string {
      for (let aggregation of aggregations) {
        if (aggregation.name === aggregationName) {
          return this.getAccessTypeForAggregation(aggregation.type);
        }
      }
      throw new Error(`aggregation '${aggregationName}' not found`);
    }

    public expressionToPostAggregation(ex: Expression, aggregations: Druid.Aggregation[]): Druid.PostAggregation {
      if (ex instanceof RefExpression) {
        var refName = ex.name;
        return {
          type: this.getAccessType(aggregations, refName),
          fieldName: refName
        };

      } else if (ex instanceof LiteralExpression) {
        if (ex.type !== 'NUMBER') throw new Error("must be a NUMBER type");
        return {
          type: 'constant',
          value: ex.value
        };

      } else if (ex instanceof ChainExpression) {
        var pattern: Expression[];
        if (pattern = ex.getExpressionPattern('add')) {
          return {
            type: 'arithmetic',
            fn: '+',
            fields: pattern.map((e => this.expressionToPostAggregation(e, aggregations)), this)
          };
        }
        if (pattern = ex.getExpressionPattern('subtract')) {
          return {
            type: 'arithmetic',
            fn: '-',
            fields: pattern.map((e => this.expressionToPostAggregation(e, aggregations)), this)
          };
        }
        if (pattern = ex.getExpressionPattern('multiply')) {
          return {
            type: 'arithmetic',
            fn: '*',
            fields: pattern.map((e => this.expressionToPostAggregation(e, aggregations)), this)
          };
        }
        if (pattern = ex.getExpressionPattern('divide')) {
          return {
            type: 'arithmetic',
            fn: '/',
            fields: pattern.map((e => this.expressionToPostAggregation(e, aggregations)), this)
          };
        }
        throw new Error("can not convert chain to post agg: " + ex.toString());

      } else {
        throw new Error("can not convert expression to post agg: " + ex.toString());
      }
    }

    public applyToPostAggregation(action: ApplyAction, aggregations: Druid.Aggregation[]): Druid.PostAggregation {
      var postAgg = this.expressionToPostAggregation(action.expression, aggregations);
      postAgg.name = action.name;
      return postAgg;
    }

    public makeStandardAggregation(name: string, filterAction: FilterAction, aggregateAction: Action): Druid.Aggregation {
      var fn = aggregateAction.action;
      var attribute = aggregateAction.expression;
      var aggregation: Druid.Aggregation = {
        name: name,
        type: fn === "sum" ? "doubleSum" : fn
      };
      if (fn !== 'count') {
        if (attribute instanceof RefExpression) {
          aggregation.fieldName = attribute.name;
        } else {
          throw new Error('can not support complex derived attributes (yet)');
        }
      }

      // See if we want to do a filtered aggregate
      if (filterAction) {
        if (this.canUseNativeAggregateFilter(filterAction.expression)) {
          aggregation = {
            type: "filtered",
            name: name,
            filter: this.timelessFilterToDruid(filterAction.expression),
            aggregator: aggregation
          };
        } else {
          throw new Error(`no support for JS filters (yet)`);
        }
      }

      return aggregation;
    }

    public makeCountDistinctAggregation(name: string, filterAction: FilterAction, action: CountDistinctAction): Druid.Aggregation {
      if (this.exactResultsOnly) {
        throw new Error("approximate query not allowed");
      }
      if (filterAction) {
        throw new Error("filtering on countDistinct aggregator isn't supported");
      }

      var attribute = action.expression;
      if (attribute instanceof RefExpression) {
        var attributeInfo = this.getAttributesInfo(attribute.name);
        if (attributeInfo instanceof UniqueAttributeInfo) {
          return {
            name: name,
            type: "hyperUnique",
            fieldName: attribute.name
          };
        } else {
          return {
            name: name,
            type: "cardinality",
            fieldNames: [attribute.name],
            byRow: true
          };
        }

      } else {
        throw new Error('can not compute distinctCount on derived attribute');
      }
    }

    public applyToAggregation(action: ApplyAction): Druid.Aggregation {
      var applyExpression = <ChainExpression>action.expression;
      if (applyExpression.op !== 'chain') throw new Error(`can not convert apply: ${applyExpression.toString()}`);

      var actions = applyExpression.actions;
      var filterAction: FilterAction = null;
      var aggregateAction: Action = null;
      if (actions.length === 1) {
        aggregateAction = actions[0];
      } else if (actions.length === 2) {
        filterAction = <FilterAction>actions[0];
        aggregateAction = actions[1];
      } else {
        throw new Error(`can not convert strange apply: ${applyExpression.toString()}`);
      }

      switch (aggregateAction.action) {
        case "count":
        case "sum":
        case "min":
        case "max":
          return this.makeStandardAggregation(action.name, filterAction, aggregateAction);

        case "countDistinct":
          return this.makeCountDistinctAggregation(action.name, filterAction, <CountDistinctAction>aggregateAction);

        case "quantile":
          throw new Error(`ToDo: add quantile support`); // ToDo: add quantile support

        case "custom":
          var customAggregationName = (<CustomAction>aggregateAction).custom;
          var customAggregation = this.customAggregations[customAggregationName];
          if (!customAggregation) throw new Error(`could not find '${customAggregationName}'`);
          var aggregationObj = customAggregation.aggregation;
          if (typeof aggregationObj.type !== 'string') throw new Error(`must have type in custom aggregation '${customAggregationName}'`);
          try {
            aggregationObj = JSON.parse(JSON.stringify(aggregationObj));
          } catch (e) {
            throw new Error(`must have JSON custom aggregation '${customAggregationName}'`);
          }
          aggregationObj.name = action.name;
          return aggregationObj;

        default:
          throw new Error(`unsupported aggregate action ${aggregateAction.action}`);
      }
    }

    public processApply(apply: ApplyAction): Action[] {
      return this.separateAggregates(<ApplyAction>apply.applyToExpression(ex => {
        return this.inlineDerivedAttributes(ex).decomposeAverage().distribute();
      }));
    }

    public isAggregateExpression(expression: Expression): boolean {
      if (expression instanceof ChainExpression) {
        var { actions } = expression;
        if (actions.length === 1) {
          return actions[0].isAggregate();
        } else if (actions.length === 2) {
          return actions[0].action === 'filter' && actions[1].isAggregate();
        } else {
          return false;
        }
      }
      return false;
    }

    public getAggregationsAndPostAggregations(): AggregationsAndPostAggregations {
      var aggregations: Druid.Aggregation[] = [];
      var postAggregations: Druid.PostAggregation[] = [];

      this.applies.forEach(apply => {
        var applyName = apply.name;
        if (this.isAggregateExpression(apply.expression)) {
          var aggregation = this.applyToAggregation(apply);
          aggregations = aggregations.filter(a => a.name !== applyName);
          aggregations.push(aggregation);
        } else {
          var postAggregation = this.applyToPostAggregation(apply, aggregations);
          postAggregations = postAggregations.filter(a => a.name !== applyName);
          postAggregations.push(postAggregation);
        }
      });

      return {
        aggregations: aggregations,
        postAggregations: postAggregations
      };
    }

    public makeHavingComparison(agg: string, op: string, value: number): Druid.Having {
      // Druid does not support <= and >= filters so... improvise.
      switch (op) {
        case '<':  return { type: "lessThan", aggregation: agg, value: value };
        case '>':  return { type: "greaterThan", aggregation: agg, value: value };
        case '<=': return { type: 'not', havingSpec: { type: "greaterThan", aggregation: agg, value: value } };
        case '>=': return { type: 'not', havingSpec: { type: "lessThan", aggregation: agg, value: value } };
        default:   throw new Error('unknown op: ' + op);
      }
    }

    public inToHavingFilter(agg: string, range: NumberRange): Druid.Having {
      var havingSpecs: Druid.Having[] = [];
      if (range.start !== null) {
        havingSpecs.push(this.makeHavingComparison(agg, (range.bounds[0] === '[' ? '>=' : '>'), range.start));
      }
      if (range.end !== null) {
        havingSpecs.push(this.makeHavingComparison(agg, (range.bounds[1] === ']' ? '<=' : '<'), range.end));
      }
      return havingSpecs.length === 1 ? havingSpecs[0] : { type: 'or', havingSpecs };
    }

    public havingFilterToDruid(filter: Expression): Druid.Having {
      if (filter instanceof LiteralExpression) {
        if (filter.value === true) {
          return null;
        } else {
          throw new Error("should never get here");
        }

      } else if (filter instanceof ChainExpression) {
        var pattern: Expression[];
        if (pattern = filter.getExpressionPattern('and')) {
          return {
            type: 'and',
            havingSpecs: pattern.map(this.havingFilterToDruid, this)
          };
        }
        if (pattern = filter.getExpressionPattern('or')) {
          return {
            type: 'or',
            havingSpecs: pattern.map(this.havingFilterToDruid, this)
          };
        }

        var prefix: Expression;
        if (prefix = filter.popAction('not')) {
          return this.havingFilterToDruid(prefix);
        }

        var lhs = filter.expression;
        var actions = filter.actions;
        if (actions.length !== 1) throw new Error(`can not convert ${filter.toString()} to Druid interval`);
        var filterAction = actions[0];
        var rhs = filterAction.expression;

        if (filterAction instanceof IsAction) {
          if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
            return {
              type: "equalTo",
              aggregation: lhs.name,
              value: rhs.value
            };

          } else {
            throw new Error(`can not convert ${filter.toString()} to Druid filter`);
          }

        } else if (filterAction instanceof InAction) {
          if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
            var rhsType = rhs.type;
            if (rhsType === 'SET/STRING') {
              return {
                type: "or",
                havingSpecs: rhs.value.elements.map((value: string) => {
                  return {
                    type: "equalTo",
                    aggregation: lhs.name,
                    value: value
                  }
                })
              };

            } else if (rhsType === 'SET/NUMBER_RANGE') {
              return {
                type: "or",
                havingSpecs: rhs.value.elements.map((value: NumberRange) => {
                  return this.inToHavingFilter(lhs.name, value);
                }, this)
              };

            } else if (rhsType === 'NUMBER_RANGE') {
              return this.inToHavingFilter(lhs.name, rhs.value);

            } else if (rhsType === 'TIME_RANGE') {
              throw new Error("can not time filter on non-primary time dimension");

            } else {
              throw new Error("not supported " + rhsType);
            }
          } else {
            throw new Error(`can not convert ${filter.toString()} to Druid having filter`);
          }

        }

      } else {
        throw new Error(`could not convert filter ${filter.toString()} to Druid filter`);
      }
    }

    public isMinMaxTimeApply(apply: ApplyAction): boolean {
      var applyExpression = apply.expression;
      if (applyExpression instanceof ChainExpression) {
        var actions = applyExpression.actions;
        if (actions.length !== 1) return false;
        var minMaxAction = actions[0];
        return (minMaxAction.action === "min" || minMaxAction.action === "max") &&
          this.isTimeRef(minMaxAction.expression);
      } else {
        return false;
      }
    }

    public getTimeBoundaryQueryAndPostProcess(): QueryAndPostProcess<Druid.Query> {
      var druidQuery: Druid.Query = {
        queryType: "timeBoundary",
        dataSource: this.getDruidDataSource()
      };

      //if (queryBuilder.hasContext()) {
      //  druidQuery.context = queryBuilder.context;
      //}

      var applies = this.applies;
      if (applies.length === 1) {
        var loneApplyExpression = <ChainExpression>applies[0].expression;
        // Max time only
        druidQuery.bound = loneApplyExpression.actions[0].action + "Time";
        //if (this.useDataSourceMetadata) {
        //  druidQuery.queryType = "dataSourceMetadata";
        //}
      }

      return {
        query: druidQuery,
        postProcess: postProcessTimeBoundaryFactory(this.applies)
      };
    }

    public getQueryAndPostProcess(): QueryAndPostProcess<Druid.Query> {
      var applies = this.applies;
      if (applies && applies.length && applies.every(this.isMinMaxTimeApply, this)) {
        return this.getTimeBoundaryQueryAndPostProcess();
      }

      var druidQuery: Druid.Query = {
        queryType: 'timeseries',
        dataSource: this.getDruidDataSource(),
        intervals: null,
        granularity: 'all'
      };

      var filterAndIntervals = this.filterToDruid(this.filter);
      druidQuery.intervals = filterAndIntervals.intervals;
      if (filterAndIntervals.filter) {
        druidQuery.filter = filterAndIntervals.filter;
      }

      switch (this.mode) {
        case 'raw':
          if (!this.allowSelectQueries) {
            throw new Error("can issue make 'select' queries unless allowSelectQueries flag is set");
          }
          druidQuery.queryType = 'select';
          druidQuery.dimensions = [];
          druidQuery.metrics = [];
          druidQuery.pagingSpec = {
            "pagingIdentifiers": {},
            "threshold": 10000
          };

          return {
            query: druidQuery,
            postProcess: postProcessSelect
          };

        case 'total':
          var aggregationsAndPostAggregations = this.getAggregationsAndPostAggregations();
          if (aggregationsAndPostAggregations.aggregations.length) {
            druidQuery.aggregations = aggregationsAndPostAggregations.aggregations;
          }
          if (aggregationsAndPostAggregations.postAggregations.length) {
            druidQuery.postAggregations = aggregationsAndPostAggregations.postAggregations;
          }

          return {
            query: druidQuery,
            postProcess: postProcessTotalFactory(this.applies)
          };

        case 'split':
          var aggregationsAndPostAggregations = this.getAggregationsAndPostAggregations();
          if (aggregationsAndPostAggregations.aggregations.length) {
            druidQuery.aggregations = aggregationsAndPostAggregations.aggregations;
          } else {
            // Druid hates not having aggregates so add a dummy count
            druidQuery.aggregations = [{ name: DUMMY_NAME, type: "count" }];
          }
          if (aggregationsAndPostAggregations.postAggregations.length) {
            druidQuery.postAggregations = aggregationsAndPostAggregations.postAggregations;
          }

          var splitSpec = this.splitToDruid();
          druidQuery.queryType = splitSpec.queryType;
          druidQuery.granularity = splitSpec.granularity;
          if (splitSpec.dimension) druidQuery.dimension = splitSpec.dimension;
          if (splitSpec.dimensions) druidQuery.dimensions = splitSpec.dimensions;
          if (splitSpec.aggregations) druidQuery.aggregations = splitSpec.aggregations;
          if (splitSpec.postAggregations) druidQuery.postAggregations = splitSpec.postAggregations;
          var postProcess = splitSpec.postProcess;

          // Combine
          switch (druidQuery.queryType) {
            case 'timeseries':
              if (this.sort && (this.sort.direction !== 'ascending' || !this.split.hasKey(this.sort.refName()))) {
                throw new Error('can not sort within timeseries query');
              }
              if (this.limit) {
                throw new Error('can not limit within timeseries query');
              }
              break;

            case 'topN':
              var sortAction = this.sort;
              var metric: string | Druid.TopNMetricSpec;
              if (sortAction) {
                metric = (<RefExpression>sortAction.expression).name;
                if (this.sortOnLabel()) {
                  metric = { type: 'lexicographic' };
                }
                if (sortAction.direction === 'ascending') {
                  metric = { type: "inverted", metric: metric };
                }
              } else {
                metric = { type: 'lexicographic' };
              }
              druidQuery.metric = metric;
              if (this.limit) {
                druidQuery.threshold = this.limit.limit;
              }
              break;

            case 'groupBy':
              var sortAction = this.sort;
              druidQuery.limitSpec = {
                type: "default",
                limit: 500000,
                columns: [
                  sortAction ?
                  { dimension: (<RefExpression>sortAction.expression).name, direction: sortAction.direction }
                  : this.split.firstSplitName()
                ]
              };
              if (this.limit) {
                druidQuery.limitSpec.limit = this.limit.limit;
              }
              if (!this.havingFilter.equals(Expression.TRUE)) {
                druidQuery.having = this.havingFilterToDruid(this.havingFilter);
              }
              break;
          }

          return {
            query: druidQuery,
            postProcess: postProcess
          };

        default:
          throw new Error("can not get query for: " + this.mode);
      }
    }

    public getIntrospectQueryAndPostProcess(): IntrospectQueryAndPostProcess<Druid.Query> {
      if (this.useSegmentMetadata) {
        return {
          query: {
            queryType: 'segmentMetadata',
            dataSource: this.getDruidDataSource(),
            merge: true,
            analysisTypes: []
          },
          postProcess: postProcessSegmentMetadataFactory(this.timeAttribute)
        };
      } else {
        return {
          query: {
            queryType: 'introspect',
            dataSource: this.getDruidDataSource()
          },
          postProcess: postProcessIntrospectFactory(this.timeAttribute)
        };
      }
    }
  }
  External.register(DruidExternal);
}
