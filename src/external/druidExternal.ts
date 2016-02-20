module Plywood {
  const DUMMY_NAME = '!DUMMY';

  const DEFAULT_TIMEZONE = Timezone.UTC;

  const AGGREGATE_TO_DRUID: Lookup<string> = {
    count: "count",
    sum: "doubleSum",
    min: "doubleMin",
    max: "doubleMax"
  };

  const AGGREGATE_TO_FUNCTION: Lookup<Function> = {
    // count is handled before this and set as a "sum" action with arguments of literals with value 1
    sum: (a: string, b:string) => `${a}+${b}`,
    min: (a: string, b:string) => `Math.min(${a},${b})`,
    max: (a: string, b:string) => `Math.max(${a},${b})`
  };

  const AGGREGATE_TO_ZERO: Lookup<string> = {
    sum: "0",
    min: "Infinity",
    max: "-Infinity"
  };

  const TIME_PART_TO_FORMAT: Lookup<string> = {
    SECOND_OF_MINUTE: "s",
    SECOND_OF_HOUR: "m'*60+'s",
    SECOND_OF_DAY: "H'*60+'m'*60+'s",
    SECOND_OF_WEEK: "e'~*24+H'*60+'m'*60+'s",
    SECOND_OF_MONTH: "d'~*24+H'*60+'m'*60+'s",
    SECOND_OF_YEAR: "D'*24+H'*60+'m'*60+'s",

    MINUTE_OF_HOUR: "m",
    MINUTE_OF_DAY: "H'*60+'m",
    MINUTE_OF_WEEK: "e'~*24+H'*60+'m",
    MINUTE_OF_MONTH: "d'~*24+H'*60+'m",
    MINUTE_OF_YEAR: "D'*24+H'*60+'m",

    HOUR_OF_DAY: "H",
    HOUR_OF_WEEK: "e'~*24+H",
    HOUR_OF_MONTH: "d'~*24+H",
    HOUR_OF_YEAR: "D'*24+H",

    DAY_OF_WEEK: "e'~",
    DAY_OF_MONTH: "d'~",
    DAY_OF_YEAR: "D",

    WEEK_OF_MONTH: null,
    WEEK_OF_YEAR: "w",

    MONTH_OF_YEAR: "M~"
  };

  const TIME_BUCKET_FORMAT: Lookup<string> = {
    "PT1S": "yyyy-MM-dd'T'HH:mm:ss'Z",
    "PT1M": "yyyy-MM-dd'T'HH:mm'Z",
    "PT1H": "yyyy-MM-dd'T'HH':00Z",
    "P1D": "yyyy-MM-dd'Z",
    //"P1W":  "yyyy-MM'-01Z",
    "P1M": "yyyy-MM'-01Z",
    "P1Y": "yyyy'-01-01Z"
  };

  function simpleMath(exprStr: string): int {
    if (String(exprStr) === 'null') return null;
    var parts = exprStr.split(/(?=[*+~])/);
    var acc = parseInt(parts.shift(), 10);
    for (let part of parts) {
      var v = parseInt(part.substring(1), 10);
      switch (part[0]) {
        case '+':
          acc += v;
          break;

        case '*':
          acc *= v;
          break;

        case '~':
          acc--;
          break;
      }
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

  export interface DruidSplit {
    queryType: string;
    granularity: Druid.Granularity | string;
    dimension?: Druid.DimensionSpec;
    dimensions?: Druid.DimensionSpec[];
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

  function timeBoundaryPostProcessFactory(applies: ApplyAction[]): PostProcess {
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

  function totalPostProcessFactory(applies: ApplyAction[]) {
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


  // ==========================

  function timeseriesNormalizerFactory(timestampLabel: string = null): Normalizer {
    return (res: Druid.TimeseriesResults): Datum[] => {
      if (!correctTimeseriesResult(res)) {
        var err = new Error("unexpected result from Druid (timeseries)");
        (<any>err).result = res; // ToDo: special error type
        throw err;
      }

      return res.map(r => {
        var datum: Datum = r.result;
        cleanDatumInPlace(datum);
        if (timestampLabel) datum[timestampLabel] = r.timestamp;
        return datum;
      })
    }
  }

  function topNNormalizer(res: Druid.DruidResults): Datum[] {
    if (!correctTopNResult(res)) {
      var err = new Error("unexpected result from Druid (topN)");
      (<any>err).result = res; // ToDo: special error type
      throw err;
    }
    var data = res.length ? res[0].result : [];
    for (var d of data) cleanDatumInPlace(d);
    return data;
  }

  function groupByNormalizerFactory(timestampLabel: string = null): Normalizer {
    return (res: Druid.GroupByResults): Datum[] => {
      if (!correctGroupByResult(res)) {
        var err = new Error("unexpected result from Druid (groupBy)");
        (<any>err).result = res; // ToDo: special error type
        throw err;
      }
      return res.map(r => {
        var datum: Datum = r.event;
        cleanDatumInPlace(datum);
        if (timestampLabel) datum[timestampLabel] = r.timestamp;
        return datum;
      });
    }
  }

  function selectNormalizer(res: Druid.SelectResults): Datum[] {
    if (!correctSelectResult(res)) {
      var err = new Error("unexpected result from Druid (select)");
      (<any>err).result = res; // ToDo: special error type
      throw err;
    }
    return res[0].result.events.map(event => event.event)
  }

  function postProcessFactory(normalizer: Normalizer, inflaters: Inflater[]) {
    return (res: any): Dataset => {
      var data = normalizer(res);
      var n = data.length;
      for (var inflater of inflaters) {
        for (var i = 0; i < n; i++) {
          inflater(data[i], i, data);
        }
      }
      return new Dataset({ data: data });
    };
  }

  function simpleMathInflaterFactory(label: string): Inflater {
    return (d: any) => {
      var v = d[label];
      if ('' + v === "null") {
        d[label] = null;
        return;
      }

      d[label] = simpleMath(v);
    }
  }

  // Introspect

  export interface IntrospectPostProcess {
    (result: any): Attributes;
  }

  function generateMakerAction(aggregation: Druid.Aggregation): Action {
    if (!aggregation) return null;

    var expression = $(aggregation.fieldName);

    switch (aggregation.type) {
      case "count":
        return new CountAction({});

      case "doubleSum":
      case "longSum":
        return new SumAction({ expression });

      case "doubleMin":
      case "longMin":
        return new MinAction({ expression });

      case "doubleMax":
      case "longMax":
        return new MaxAction({ expression });

      default:
        return null;
    }
  }

  function segmentMetadataPostProcessFactory(timeAttribute: string): IntrospectPostProcess {
    return (res: Druid.SegmentMetadataResults): Attributes => {
      var res0 = res[0];
      if (!res0 || !res0.columns) throw new Error('malformed segmentMetadata response');
      var columns = res0.columns;
      var aggregators = res0.aggregators || {};

      var foundTime = false;
      var attributes: Attributes = [];
      for (var name in columns) {
        if (!hasOwnProperty(columns, name)) continue;
        var columnData = columns[name];

        // Error conditions
        if (columnData.errorMessage || columnData.size < 0) continue;

        if (name === '__time') {
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
                makerAction: generateMakerAction(aggregators[name])
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
          }
        }
      }

      if (!foundTime) throw new Error('no valid __time in segmentMetadata response');
      return attributes;
    }
  }

  function introspectPostProcessFactory(timeAttribute: string): IntrospectPostProcess {
    return (res: Druid.DatasourceIntrospectResult): Attributes => {
      if (!Array.isArray(res.dimensions) || !Array.isArray(res.metrics)) {
        throw new Error('malformed GET introspect response');
      }

      var attributes: Attributes = [
        new AttributeInfo({ name: timeAttribute, type: 'TIME' })
      ];
      res.dimensions.forEach(dimension => {
        if (dimension === timeAttribute) return; // Ignore dimensions that clash with the timeAttribute name
        attributes.push(new AttributeInfo({ name: dimension, type: 'STRING' }));
      });
      res.metrics.forEach(metric => {
        if (metric === timeAttribute) return; // Ignore metrics that clash with the timeAttribute name
        attributes.push(new AttributeInfo({ name: metric, type: 'NUMBER', unsplitable: true }));
      });
      return attributes;
    }
  }

  export class DruidExternal extends External {
    static type = 'DATASET';

    static TRUE_INTERVAL = ["1000-01-01/3000-01-01"];
    static FALSE_INTERVAL = ["1000-01-01/1000-01-02"];

    static VALID_INTROSPECTION_STRATEGIES = ['segment-metadata-fallback', 'segment-metadata-only', 'datasource-get'];
    static DEFAULT_INTROSPECTION_STRATEGY = 'segment-metadata-fallback';

    static fromJS(datasetJS: any): DruidExternal {
      var value: ExternalValue = External.jsToValue(datasetJS);
      value.dataSource = datasetJS.dataSource;
      value.timeAttribute = datasetJS.timeAttribute;
      value.customAggregations = datasetJS.customAggregations || {};
      value.allowEternity = Boolean(datasetJS.allowEternity);
      value.allowSelectQueries = Boolean(datasetJS.allowSelectQueries);
      value.introspectionStrategy = datasetJS.introspectionStrategy;
      value.exactResultsOnly = Boolean(datasetJS.exactResultsOnly);
      value.context = datasetJS.context;
      value.druidVersion = datasetJS.druidVersion;
      return new DruidExternal(value);
    }


    public dataSource: string | string[];
    public timeAttribute: string;
    public customAggregations: CustomDruidAggregations;
    public allowEternity: boolean;
    public allowSelectQueries: boolean;
    public introspectionStrategy: string;
    public exactResultsOnly: boolean;
    public context: Lookup<any>;
    public druidVersion: string;
    public finalizers: Druid.PostAggregation[];

    constructor(parameters: ExternalValue) {
      super(parameters, dummyObject);
      this._ensureEngine("druid");
      this.dataSource = parameters.dataSource;
      this.timeAttribute = parameters.timeAttribute;
      this.customAggregations = parameters.customAggregations;
      if (typeof this.timeAttribute !== 'string') throw new Error("must have a timeAttribute");
      this.allowEternity = parameters.allowEternity;
      this.allowSelectQueries = parameters.allowSelectQueries;

      var introspectionStrategy = parameters.introspectionStrategy || DruidExternal.DEFAULT_INTROSPECTION_STRATEGY;
      if (DruidExternal.VALID_INTROSPECTION_STRATEGIES.indexOf(introspectionStrategy) === -1) {
        throw new Error(`Invalid introspectionStrategy '${introspectionStrategy}'`);
      }
      this.introspectionStrategy = introspectionStrategy;

      this.exactResultsOnly = parameters.exactResultsOnly;
      this.context = parameters.context;

      var druidVersion = parameters.druidVersion || '0.8.0';
      if (druidVersion.length !== 5) throw new Error('druidVersion length must be 5');
      if (druidVersion < '0.8.0') throw new Error('only druidVersions >= 0.8.0 are supported');
      this.druidVersion = druidVersion;

      this.finalizers = parameters.finalizers || [];
    }

    public valueOf(): ExternalValue {
      var value: ExternalValue = super.valueOf();
      value.dataSource = this.dataSource;
      value.timeAttribute = this.timeAttribute;
      value.customAggregations = this.customAggregations;
      value.allowEternity = this.allowEternity;
      value.allowSelectQueries = this.allowSelectQueries;
      value.introspectionStrategy = this.introspectionStrategy;
      value.exactResultsOnly = this.exactResultsOnly;
      value.context = this.context;
      value.druidVersion = this.druidVersion;
      value.finalizers = this.finalizers;
      return value;
    }

    public toJS(): ExternalJS {
      var js: ExternalJS = super.toJS();
      js.dataSource = this.dataSource;
      js.timeAttribute = this.timeAttribute;
      if (Object.keys(this.customAggregations).length) js.customAggregations = this.customAggregations;
      if (this.allowEternity) js.allowEternity = true;
      if (this.allowSelectQueries) js.allowSelectQueries = true;
      if (this.introspectionStrategy !== DruidExternal.DEFAULT_INTROSPECTION_STRATEGY) js.introspectionStrategy = this.introspectionStrategy;
      if (this.exactResultsOnly) js.exactResultsOnly = true;
      js.context = this.context;
      js.druidVersion = this.druidVersion;
      return js;
    }

    public equals(other: DruidExternal): boolean {
      return super.equals(other) &&
        String(this.dataSource) === String(other.dataSource) &&
        this.timeAttribute === other.timeAttribute &&
        customAggregationsEqual(this.customAggregations, other.customAggregations) &&
        this.allowEternity === other.allowEternity &&
        this.allowSelectQueries === other.allowSelectQueries &&
        this.introspectionStrategy === other.introspectionStrategy &&
        this.exactResultsOnly === other.exactResultsOnly &&
        dictEqual(this.context, other.context) &&
        this.druidVersion === other.druidVersion;
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
      if (!split || split.isMultiSplit()) return true;
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
      if (!split || split.isMultiSplit()) return true;
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

    public versionBefore(neededVersion: string): boolean {
      return this.druidVersion < neededVersion;
    }

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

        if (filter.lastAction() instanceof NotAction) {
          return this.canUseNativeAggregateFilter(filter.popAction());
        }

        var actions = filter.actions;
        if (actions.length !== 1) return false;
        var firstAction = actions[0];
        return filter.expression.isOp('ref') &&
          (firstAction.action === 'is' || firstAction.action === 'in') &&
          firstAction.expression.isOp('literal') &&
          filter.expression.type === "STRING";
      }
      return false;
    }

    public javascriptDruidFilter(referenceName: string, filter: Expression): Druid.Filter {
      return {
        type: "javascript",
        dimension: referenceName,
        "function": filter.getJSFn('d')
      };
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

      if (filter instanceof LiteralExpression) {
        if (filter.value === true) {
          return null;
        } else {
          throw new Error("should never get here");
        }

      } else if (filter instanceof ChainExpression) {
        var filterAction = filter.lastAction();
        var rhs = filterAction.expression;
        var lhs = filter.popAction();
        var referenceName: string;
        var attributeInfo: AttributeInfo;

        // Special handling for r('some_tag').in($tags)
        if (lhs instanceof LiteralExpression) {
          if (rhs instanceof RefExpression) {
            referenceName = rhs.name;
          } else {
            throw new Error(`unsupported literal lhs must have ref rhs: ${rhs.toString()}`);
          }

          if (filterAction instanceof InAction) {
            attributeInfo = this.getAttributesInfo(referenceName);
            return {
              type: "selector",
              dimension: referenceName,
              value: attributeInfo.serialize(lhs.value)
            }
          }
          throw new Error(`unsupported rhs action for literal lhs: ${filterAction.toString()}`);
        }

        if (filterAction instanceof NotAction) {
          return {
            type: 'not',
            field: this.timelessFilterToDruid(lhs)
          };
        }

        var extractionFn = this.expressionToExtractionFn(lhs);
        referenceName = lhs.getFreeReferences()[0];
        attributeInfo = this.getAttributesInfo(referenceName);

        if (filterAction instanceof IsAction) {
          if (rhs instanceof LiteralExpression) {
            var druidFilter: Druid.Filter = {
              type: "selector",
              dimension: referenceName,
              value: attributeInfo.serialize(rhs.value)
            };
            if (extractionFn) {
              druidFilter.type = "extraction";
              druidFilter.extractionFn = extractionFn;
            }
            return druidFilter;
          } else {
            throw new Error("can not convert " + filter.toString() + " to Druid filter");
          }
        }

        if (filterAction instanceof InAction || filterAction instanceof OverlapAction) {
          if (rhs instanceof LiteralExpression) {
            var rhsType = rhs.type;
            if (rhsType === 'SET/STRING' || rhsType === 'SET/NULL') {
              var fields = rhs.value.elements.map((value: string) => {
                var druidFilter: Druid.Filter = {
                  type: "selector",
                  dimension: referenceName,
                  value: attributeInfo.serialize(value)
                };
                if (extractionFn) {
                  if (extractionFn.type === 'javascript') {
                    // Might as well just do a full on javascript filter
                    return this.javascriptDruidFilter(referenceName, filter);
                  }
                  druidFilter.type = "extraction";
                  druidFilter.extractionFn = extractionFn;
                }
                return druidFilter;
              });

              if (fields.length === 1) return fields[0];
              return { type: "or", fields };

            } else if (rhsType === 'NUMBER_RANGE') {
              var range: NumberRange = rhs.value;
              var r0 = range.start;
              var r1 = range.end;
              return {
                type: "javascript",
                dimension: referenceName,
                "function": `function(a) { a = Number(a); return ${r0} <= a && a < ${r1}; }`
              };

            } else if (rhsType === 'TIME_RANGE') {
              throw new Error("can not time filter on non-primary time dimension");

            } else {
              throw new Error("not supported " + rhsType);

            }
          } else {
            throw new Error("can not convert " + filter.toString() + " to Druid filter");
          }
        }

        if (filterAction instanceof MatchAction) {
          if (lhs instanceof RefExpression) {
            return {
              type: "regex",
              dimension: referenceName,
              pattern: filterAction.regexp
            };
          } else {
            throw new Error("can not convert " + filter.toString() + " to Druid filter");
          }
        }

        if (filterAction instanceof ContainsAction) {
          if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
            if (filterAction.compare === ContainsAction.IGNORE_CASE) {
              return {
                type: "search",
                dimension: referenceName,
                query: {
                  type: "fragment", // ToDo: change to 'insensitive_contains'
                  values: [rhs.value]
                }
              };
            } else {
              return this.javascriptDruidFilter(referenceName, filter);
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
        var rhs = filterAction.expression;

        if (filterAction instanceof IsAction) {
          if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
            return [TimeRange.intervalFromDate(rhs.value)];
          } else {
            throw new Error(`can not convert ${filter.toString()} to Druid interval`);
          }

        } else if (filterAction instanceof InAction) {
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

        } else {
          throw new Error(`can not convert ${filter.toString()} to Druid interval`);
        }

        /*
        else if (filter instanceof AndExpression) {
          var mergedTimePart = AndExpression.mergeTimePart(filter);
          if (mergedTimePart) {
          return this.timeFilterToIntervals(mergedTimePart);
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

    public getRangeBucketingExtractionFn(attributeInfo: RangeAttributeInfo, numberBucket: NumberBucketAction): Druid.ExtractionFn {
      var regExp = attributeInfo.getMatchingRegExpString();
      if (numberBucket && numberBucket.offset === 0 && numberBucket.size === attributeInfo.rangeSize) numberBucket = null;
      var bucketing = '';
      if (numberBucket) {
        bucketing = 's=' + continuousFloorExpression('s', 'Math.floor', numberBucket.size, numberBucket.offset) + ';';
      }
      return {
        type: "javascript",
        'function': `function(d) {
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

    public splitExpressionToGranularityInflater(splitExpression: Expression, label: string): GranularityInflater {
      if (splitExpression instanceof ChainExpression) {
        var splitActions = splitExpression.actions;
        if (this.isTimeRef(splitExpression.expression) && splitActions.length === 1 && splitActions[0].action === 'timeBucket') {

          var { duration, timezone } = <TimeBucketAction>splitActions[0];
          return {
            granularity: {
              type: "period",
              period: duration.toString(),
              timeZone: timezone.toString()
            },
            inflater: External.timeRangeInflaterFactory(label, duration, timezone)
          };
        }
      }

      return null;
    }

    public expressionToExtractionFn(expression: Expression): Druid.ExtractionFn {
      var freeReferences = expression.getFreeReferences();
      if (freeReferences.length !== 1) {
        throw new Error(`must have a single reference: ${expression.toString()}`);
      }
      var referenceName = freeReferences[0];

      if (expression instanceof RefExpression) {
        var attributeInfo = this.getAttributesInfo(referenceName);

        if (attributeInfo instanceof RangeAttributeInfo) {
          return this.getRangeBucketingExtractionFn(attributeInfo, null)
        }

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
        }

        return null; // A valid return for no extractionFn needed
      }

      if (expression.type === 'BOOLEAN') {
        return {
          type: "javascript",
          'function': expression.getJSFn('d')
        };
      }

      if (expression instanceof ChainExpression) {
        if (expression.getExpressionPattern('concat')) {
          // https://github.com/druid-io/druid/commit/3459a202ce751cb60884519ef902e35280550895
          // Also https://github.com/druid-io/druid/pull/2209/files
          return {
            type: "javascript",
            'function': expression.getJSFn('d'),
            injective: true
          };
        }

        // Concat is the only thing allowed to have a non leading ref, the rest must be $ref.someFunction
        if (!expression.expression.isOp('ref')) {
          throw new Error(`can not convert complex: ${expression.expression.toString()}`);
        }

        var actions = expression.actions;
        var mainAction = actions[0];
        var retainMissingValue = false;
        var replaceMissingValueWith: any = null;
        var fallbackAction : FallbackAction = null;

        if (actions.length === 2) {
          fallbackAction = <FallbackAction>actions[1];
          if (!(fallbackAction instanceof FallbackAction)) {
            throw new Error(`can not convert expression: ${expression.toString()}`);
          }
          var fallbackExpression = fallbackAction.expression;
          if (fallbackExpression.isOp("ref")) {
            // the ref has to be the same as the argument beacause we can't refer to other dimensions
            // so the only option would be for it to be equal to original dimension
            retainMissingValue = true;
          } else if (fallbackExpression.isOp("literal")) {
            replaceMissingValueWith = fallbackExpression.getLiteralValue();
          } else {
            // TODO: would be cool to support $foo.extract(...).fallback($foo ++ 'something')
            throw new Error(`unsupported fallback action: ${expression.toString()}`);
          }
        } else if (actions.length !== 1) {
          throw new Error(`can not convert expression: ${expression.toString()}`);
        }

        if (mainAction instanceof SubstrAction) {
          if (this.versionBefore('0.9.0')) {
            return {
              type: "javascript",
              'function': expression.getJSFn('d')
            };
          }

          return {
            type: "substring",
            index: mainAction.position,
            length: mainAction.length
          };
        }

        if (mainAction instanceof ExtractAction) {
          // retainMissingValue === false is not supported in old druid nor is replaceMissingValueWith in regex extractionFn
          // we want to use a js function if we are using an old version of druid and want to use this functionality
          if (this.versionBefore('0.9.0') && (retainMissingValue === false || replaceMissingValueWith !== null)) {
            return {
              type: "javascript",
              'function': expression.getJSFn('d')
            };
          }

          var regexExtractionFn: Druid.ExtractionFn = {
            type: "regex",
            expr: mainAction.regexp
          };

          if (!retainMissingValue) {
            regexExtractionFn.replaceMissingValue = true;
          }

          if (replaceMissingValueWith !== null) {
            regexExtractionFn.replaceMissingValueWith = replaceMissingValueWith;
          }

          return regexExtractionFn;
        }

        if (mainAction instanceof LookupAction) {

          var lookupExtractionFn: Druid.ExtractionFn = {
            type: "lookup",
            lookup: {
              type: "namespace",
              "namespace": mainAction.lookup
            }
          };

          if (retainMissingValue) {
            lookupExtractionFn.retainMissingValue = true;
          }

          if (replaceMissingValueWith !== null) {
            lookupExtractionFn.replaceMissingValueWith = replaceMissingValueWith;
          }

          return lookupExtractionFn;
        }

        if (mainAction instanceof TimeBucketAction) {
          if (fallbackAction !== null) throw new Error(`unsupported fallback in timeBucket expression ${mainAction.expression.toString()}`);
          var format = TIME_BUCKET_FORMAT[mainAction.duration.toString()];
          if (!format) throw new Error(`unsupported duration in timeBucket expression ${mainAction.duration.toString()}`);
          return {
            type: "timeFormat",
            format: format,
            timeZone: (mainAction.timezone || DEFAULT_TIMEZONE).toString(),
            locale: "en-US"
          };
        }

        if (mainAction instanceof TimePartAction) {
          if (fallbackAction !== null) throw new Error(`unsupported fallback in timePart expression ${mainAction.expression.toString()}`);
          var format = TIME_PART_TO_FORMAT[mainAction.part];
          if (!format) throw new Error(`unsupported part in timePart expression ${mainAction.part}`);
          return {
            type: "timeFormat",
            format: format,
            timeZone: (mainAction.timezone || DEFAULT_TIMEZONE).toString(),
            locale: "en-US"
          };
        }

        if (mainAction instanceof NumberBucketAction) {
          if (fallbackAction !== null) throw new Error(`unsupported fallback in numberBucket expression ${mainAction.expression.toString()}`);
          var attributeInfo = this.getAttributesInfo(referenceName);
          if (attributeInfo.type === 'NUMBER') {
            var floorExpression = continuousFloorExpression("d", "Math.floor", mainAction.size, mainAction.offset);
            return {
              type: "javascript",
              'function': `function(d){d=Number(d); if(isNaN(d)) return 'null'; return ${floorExpression};}`
            };
          }

          if (attributeInfo instanceof RangeAttributeInfo) {
            return this.getRangeBucketingExtractionFn(<RangeAttributeInfo>attributeInfo, mainAction);
          }

          if (attributeInfo instanceof HistogramAttributeInfo) {
            if (this.exactResultsOnly) {
              throw new Error("can not use approximate histograms in exactResultsOnly mode");
            }
            throw new Error("histogram splits do not work right now");
          }
        }

        if (mainAction instanceof AbsoluteAction) {
          return {
            type: "javascript",
            'function': `function(d){d=Number(d); return Math.abs(d);}`
          };
        }
      }

      throw new Error(`could not convert ${expression.toString()} to a Druid extractionFn`);
    }

    public splitExpressionToDimensionInflater(splitExpression: Expression, label: string): DimensionInflater {
      var extractionFn = this.expressionToExtractionFn(splitExpression);
      // expressionToExtractionFn already checked that there is only one ref name
      var referenceName = splitExpression.getFreeReferences()[0];

      var simpleInflater = External.getSimpleInflater(splitExpression, label);

      var dimension: Druid.DimensionSpec = {
        type: "default",
        dimension: referenceName === this.timeAttribute ? '__time' : referenceName,
        outputName: label
      };
      if (extractionFn) {
        dimension.type = "extraction";
        dimension.extractionFn = extractionFn;
      }

      if (splitExpression instanceof RefExpression) {
        var attributeInfo = this.getAttributesInfo(referenceName);
        if (attributeInfo instanceof RangeAttributeInfo) {
          return {
            dimension,
            inflater: External.numberRangeInflaterFactory(label, attributeInfo.rangeSize)
          };
        }

        return {
          dimension,
          inflater: simpleInflater
        };
      }

      if (splitExpression.type === 'BOOLEAN' || splitExpression.type === 'STRING') {
        return {
          dimension,
          inflater: simpleInflater
        };
      }

      if (splitExpression instanceof ChainExpression) {
        if (splitExpression.getExpressionPattern('concat')) {
          return {
            dimension,
            inflater: simpleInflater
          };
        }

        if (!splitExpression.expression.isOp('ref')) {
          throw new Error(`can not convert complex: ${splitExpression.expression.toString()}`);
        }
        var actions = splitExpression.actions;
        if (actions.length !== 1) throw new Error(`can not convert expression: ${splitExpression.toString()}`);
        var splitAction = actions[0];

        if (splitAction instanceof SubstrAction) {
          return {
            dimension,
            inflater: simpleInflater
          };
        }

        if (splitAction instanceof AbsoluteAction) {
          return {
            dimension,
            inflater: simpleInflater
          };
        }

        if (splitAction instanceof TimeBucketAction) {
          var format = TIME_BUCKET_FORMAT[splitAction.duration.toString()];
          if (!format) throw new Error(`unsupported part in timeBucket expression ${splitAction.duration.toString()}`);
          return {
            dimension,
            inflater: External.timeRangeInflaterFactory(label, splitAction.duration, splitAction.timezone || DEFAULT_TIMEZONE)
          };
        }

        if (splitAction instanceof TimePartAction) {
          var format = TIME_PART_TO_FORMAT[splitAction.part];
          if (!format) throw new Error(`unsupported part in timePart expression ${splitAction.part}`);
          return {
            dimension,
            inflater: simpleMathInflaterFactory(label)
          };
        }

        if (splitAction instanceof NumberBucketAction) {
          var attributeInfo = this.getAttributesInfo(referenceName);
          if (attributeInfo.type === 'NUMBER') {
            var floorExpression = continuousFloorExpression("d", "Math.floor", splitAction.size, splitAction.offset);
            return {
              dimension,
              inflater: External.numberRangeInflaterFactory(label, splitAction.size)
            };
          }

          if (attributeInfo instanceof RangeAttributeInfo) {
            return {
              dimension,
              inflater: External.numberRangeInflaterFactory(label, splitAction.size)
            }
          }

        }
      }

      throw new Error(`could not convert ${splitExpression.toString()} to a Druid Dimension`);
    }

    public splitToDruid(): DruidSplit {
      var split = this.split;
      if (split.isMultiSplit()) {
        var timestampLabel: string = null;
        var granularity: Druid.Granularity = null;
        var dimensions: Druid.DimensionSpec[] = [];
        var inflaters: Inflater[] = [];
        split.mapSplits((name, expression) => {
          if (!granularity && !this.limit && !this.sort) {
            // We have to add !this.limit && !this.sort because of a bug in groupBy sorting
            // Remove it when fixed https://github.com/druid-io/druid/issues/1926
            var granularityInflater = this.splitExpressionToGranularityInflater(expression, name);
            if (granularityInflater) {
              timestampLabel = name;
              granularity = granularityInflater.granularity;
              inflaters.push(granularityInflater.inflater);
              return;
            }
          }

          var { dimension, inflater } = this.splitExpressionToDimensionInflater(expression, name);
          dimensions.push(dimension);
          if (inflater) {
            inflaters.push(inflater);
          }
        });
        return {
          queryType: 'groupBy',
          dimensions: dimensions,
          granularity: granularity || 'all',
          postProcess: postProcessFactory(
            groupByNormalizerFactory(timestampLabel),
            inflaters
          )
        };
      }

      var splitExpression = split.firstSplitExpression();
      var label = split.firstSplitName();

      // Can it be a time series?
      var granularityInflater = this.splitExpressionToGranularityInflater(splitExpression, label);
      if (granularityInflater) {
        return {
          queryType: 'timeseries',
          granularity: granularityInflater.granularity,
          postProcess: postProcessFactory(
            timeseriesNormalizerFactory(label),
            [granularityInflater.inflater]
          )
        };
      }

      var dimensionInflater = this.splitExpressionToDimensionInflater(splitExpression, label);
      var inflaters = [dimensionInflater.inflater].filter(Boolean);
      if (
        this.havingFilter.equals(Expression.TRUE) && // There is no having filter
        (this.limit || split.maxBucketNumber() < 1000) && // There is a limit (or the split range is limited)
        !this.exactResultsOnly // We do not care about exact results
      ) {
        return {
          queryType: 'topN',
          dimension: dimensionInflater.dimension,
          granularity: 'all',
          postProcess: postProcessFactory(topNNormalizer, inflaters)
        };
      }

      return {
        queryType: 'groupBy',
        dimensions: [dimensionInflater.dimension],
        granularity: 'all',
        postProcess: postProcessFactory(groupByNormalizerFactory(), inflaters)
      };
    }

    public expressionToPostAggregation(ex: Expression): Druid.PostAggregation {
      if (ex instanceof RefExpression) {
        var refName = ex.name;
        return {
          type: 'fieldAccess',
          fieldName: refName
        };

      } else if (ex instanceof LiteralExpression) {
        if (ex.type !== 'NUMBER') throw new Error("must be a NUMBER type");
        return {
          type: 'constant',
          value: ex.value
        };

      } else if (ex instanceof ChainExpression) {
        var lastAction = ex.lastAction();

        if (lastAction instanceof AbsoluteAction || lastAction instanceof PowerAction || lastAction instanceof FallbackAction) {
          var fieldNameRefs = ex.getFreeReferences();
          return {
            type: 'javascript',
            fieldNames: fieldNameRefs,
            function: `function(${fieldNameRefs.toString()}) { return ${ex.getJS(null)}; }`
          };
        }

        var pattern: Expression[];
        if (pattern = ex.getExpressionPattern('add')) {
          return {
            type: 'arithmetic',
            fn: '+',
            fields: pattern.map(e => this.expressionToPostAggregation(e))
          };
        }
        if (pattern = ex.getExpressionPattern('subtract')) {
          return {
            type: 'arithmetic',
            fn: '-',
            fields: pattern.map(e => this.expressionToPostAggregation(e))
          };
        }
        if (pattern = ex.getExpressionPattern('multiply')) {
          return {
            type: 'arithmetic',
            fn: '*',
            fields: pattern.map(e => this.expressionToPostAggregation(e))
          };
        }
        if (pattern = ex.getExpressionPattern('divide')) {
          return {
            type: 'arithmetic',
            fn: '/',
            fields: pattern.map(e => this.expressionToPostAggregation(e))
          };
        }
        throw new Error("can not convert chain to post agg: " + ex.toString());

      } else {
        throw new Error("can not convert expression to post agg: " + ex.toString());
      }
    }

    public applyToPostAggregation(action: ApplyAction): Druid.PostAggregation {
      var postAgg = this.expressionToPostAggregation(action.expression);
      postAgg.name = action.name;
      return postAgg;
    }

    public makeNativeAggregateFilter(name: string, filterExpression: Expression, aggregator: Druid.Aggregation): Druid.Aggregation {
      return {
        type: "filtered",
        name: name,
        filter: this.timelessFilterToDruid(filterExpression),
        aggregator
      };
    }

    public makeStandardAggregation(name: string, aggregateAction: Action): Druid.Aggregation {
      var fn = aggregateAction.action;
      var aggregateExpression = aggregateAction.expression;
      var aggregation: Druid.Aggregation = {
        name: name,
        type: AGGREGATE_TO_DRUID[fn]
      };
      if (fn !== 'count') {
        if (aggregateExpression instanceof RefExpression) {
          aggregation.fieldName = aggregateExpression.name;
        } else {
          return this.makeJavaScriptAggregation(name, null, aggregateAction);
        }
      }

      return aggregation;
    }

    public makeCountDistinctAggregation(name: string, action: CountDistinctAction): Druid.Aggregation {
      if (this.exactResultsOnly) {
        throw new Error("approximate query not allowed");
      }

      var attribute = action.expression;
      if (attribute instanceof RefExpression) {
        var attributeName = attribute.name;
      } else {
        throw new Error(`can not compute countDistinct on derived attribute: ${attribute.toString()}`);
      }

      var attributeInfo = this.getAttributesInfo(attributeName);
      if (attributeInfo instanceof UniqueAttributeInfo) {
        return {
          name: name,
          type: "hyperUnique",
          fieldName: attributeName
        };
      } else {
        return {
          name: name,
          type: "cardinality",
          fieldNames: [attributeName],
          byRow: true
        };
      }
    }

    public makeCustomAggregation(name: string, action: CustomAction): Druid.Aggregation {
      var customAggregationName = action.custom;
      var customAggregation = this.customAggregations[customAggregationName];
      if (!customAggregation) throw new Error(`could not find '${customAggregationName}'`);
      var aggregationObj = customAggregation.aggregation;
      if (typeof aggregationObj.type !== 'string') throw new Error(`must have type in custom aggregation '${customAggregationName}'`);
      try {
        aggregationObj = JSON.parse(JSON.stringify(aggregationObj));
      } catch (e) {
        throw new Error(`must have JSON custom aggregation '${customAggregationName}'`);
      }
      aggregationObj.name = name;
      return aggregationObj;
    }

    public makeJavaScriptAggregation(name: string, filterExpression: Expression, aggregateAction: Action): Druid.Aggregation {
      var aggregateActionType = aggregateAction.action;
      var aggregateExpression = aggregateAction.expression;

      if (aggregateActionType === "count") {
        // special handling for count() == sum(1) -- a count can be thought of as a sum of literals with value 1
        aggregateActionType = "sum";
        aggregateExpression = r(1);
      }

      var zero =  AGGREGATE_TO_ZERO[aggregateActionType];
      var aggregateFunction = AGGREGATE_TO_FUNCTION[aggregateActionType];
      var fieldNames = aggregateExpression.getFreeReferences();

      var aggregateExpressionJS = aggregateExpression.getJS(null);

      if (filterExpression) {
        fieldNames = deduplicateSort(fieldNames.concat(filterExpression.getFreeReferences()));
        aggregateExpressionJS = `(${filterExpression.getJS(null)} ? ${aggregateExpressionJS} : ${zero})`
      }

      return {
        name,
        type: "javascript",
        fieldNames: fieldNames,
        fnAggregate: `function(_c,${fieldNames}) { return ${aggregateFunction('_c', aggregateExpressionJS)}; }`,
        fnCombine: `function(a,b) { return ${aggregateFunction('a', 'b')}; }`,
        fnReset: `function() { return ${zero}; }`
      }
    }

    public applyToAggregation(action: ApplyAction): Druid.Aggregation {
      var applyExpression = <ChainExpression>action.expression;
      if (applyExpression.op !== 'chain') throw new Error(`can not convert apply: ${applyExpression.toString()}`);

      var actions = applyExpression.actions;
      var filterExpression: Expression = null;
      var aggregateAction: Action = null;
      if (actions.length === 1) {
        aggregateAction = actions[0];
      } else if (actions.length === 2) {
        var filterAction = actions[0];
        if (filterAction instanceof FilterAction) {
          filterExpression = filterAction.expression;
        } else {
          throw new Error(`first action not a filter in: ${applyExpression.toString()}`);
        }
        aggregateAction = actions[1];
      } else {
        throw new Error(`can not convert strange apply: ${applyExpression.toString()}`);
      }

      switch (aggregateAction.action) {
        case "count":
        case "sum":
        case "min":
        case "max":
          if (filterExpression) {
            if (this.canUseNativeAggregateFilter(filterExpression)) {
              return this.makeNativeAggregateFilter(action.name, filterExpression, this.makeStandardAggregation(action.name, aggregateAction));
            } else {
              return this.makeJavaScriptAggregation(action.name, filterExpression, aggregateAction);
            }
          } else {
            return this.makeStandardAggregation(action.name, aggregateAction);
          }

        case "countDistinct":
          if (filterExpression) {
            if (this.canUseNativeAggregateFilter(filterExpression)) {
              return this.makeNativeAggregateFilter(action.name, filterExpression, this.makeCountDistinctAggregation(action.name, <CountDistinctAction>aggregateAction));
            } else {
              throw new Error(`can not use complex filter with countDistinct aggregation: ${filterExpression.toString()}`);
            }
          } else {
            return this.makeCountDistinctAggregation(action.name, <CountDistinctAction>aggregateAction);
          }

        case "quantile":
          throw new Error(`ToDo: add quantile support`); // ToDo: add quantile support

        case "custom":
          if (filterExpression) {
            if (this.canUseNativeAggregateFilter(filterExpression)) {
              return this.makeNativeAggregateFilter(action.name, filterExpression, this.makeCustomAggregation(action.name, <CustomAction>aggregateAction));
            } else {
              throw new Error(`can not use complex filter with custom aggregation: ${filterExpression.toString()}`);
            }
          } else {
            return this.makeCustomAggregation(action.name, <CustomAction>aggregateAction);
          }

        default:
          throw new Error(`unsupported aggregate action ${aggregateAction.action}`);
      }
    }

    public processApply(apply: ApplyAction): ApplyAction[] {
      return this.separateAggregates(<ApplyAction>apply.applyToExpression(ex => {
        return this.inlineDerivedAttributes(ex).decomposeAverage().distribute();
      }));
    }

    public getFinalizedName(aggregateApply: ApplyAction): string {
      var finalizerType = this.getFinalizerTypeForAggregateApply(aggregateApply);
      if ( finalizerType ) {
        var finalizer: Druid.PostAggregation = {
          type: finalizerType,
          fieldName: aggregateApply.name,
          name: aggregateApply.name + "_fin"
        };
        this.finalizers = this.finalizers.concat(finalizer);

        return finalizer.name;
      }
      return aggregateApply.name;
    }

    public getFinalizerTypeForAggregateApply(aggregateApply: ApplyAction): string {
      var aggregateAction = aggregateApply.expression.lastAction();
      if (aggregateAction instanceof CountDistinctAction) {
        return "hyperUniqueCardinality";
      }
      if (aggregateAction instanceof CustomAction) {
        var customAggregation = this.customAggregations[aggregateAction.custom];
        if (!customAggregation) {
          throw new Error("custom aggregation is not defined " + aggregateAction.custom);
        }
        return customAggregation.accessType || null;
      }
      return null;
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
          var postAggregation = this.applyToPostAggregation(apply);
          postAggregations = postAggregations.filter(a => a.name !== applyName);
          postAggregations.push(postAggregation);
        }
      });

      return {
        aggregations: aggregations,
        postAggregations: this.finalizers.concat(postAggregations)
      };
    }

    public makeHavingComparison(agg: string, op: string, value: number): Druid.Having {
      // Druid does not support <= and >= filters so... improvise.
      switch (op) {
        case '<':
          return { type: "lessThan", aggregation: agg, value: value };
        case '>':
          return { type: "greaterThan", aggregation: agg, value: value };
        case '<=':
          return { type: 'not', havingSpec: { type: "greaterThan", aggregation: agg, value: value } };
        case '>=':
          return { type: 'not', havingSpec: { type: "lessThan", aggregation: agg, value: value } };
        default:
          throw new Error('unknown op: ' + op);
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

        if (filter.lastAction() instanceof NotAction) {
          return this.havingFilterToDruid(filter.popAction());
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
        postProcess: timeBoundaryPostProcessFactory(this.applies)
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

      if (this.context) {
        druidQuery.context = this.context;
      }

      var filterAndIntervals = this.filterToDruid(this.filter);
      druidQuery.intervals = filterAndIntervals.intervals;
      if (filterAndIntervals.filter) {
        druidQuery.filter = filterAndIntervals.filter;
      }

      switch (this.mode) {
        case 'raw':
          if (!this.allowSelectQueries) {
            throw new Error("to issues 'select' queries allowSelectQueries flag must be set");
          }
          druidQuery.queryType = 'select';
          druidQuery.dimensions = [];
          druidQuery.metrics = [];
          druidQuery.pagingSpec = {
            "pagingIdentifiers": {},
            "threshold": this.limit ? this.limit.limit : 10000
          };

          return {
            query: druidQuery,
            postProcess: postProcessFactory(selectNormalizer, [])
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
            postProcess: totalPostProcessFactory(this.applies)
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

                var inverted: boolean;
                if (this.sortOnLabel()) {
                  var splitType = this.split.firstSplitExpression().type;
                  if (splitType === 'NUMBER' || splitType === 'NUMBER_RANGE') {
                    metric = { type: 'alphaNumeric' };
                  } else {
                    metric = { type: 'lexicographic' };
                  }
                  inverted = sortAction.direction === 'descending';
                } else {
                  metric = sortAction.refName();
                  inverted = sortAction.direction === 'ascending';
                }

                if (inverted) {
                  metric = { type: "inverted", metric: metric };
                }

              } else {
                metric = { type: 'lexicographic' };
              }
              druidQuery.metric = metric;
              druidQuery.threshold = this.limit ? this.limit.limit : 1000;
              break;

            case 'groupBy':
              var sortAction = this.sort;

              var column: Druid.OrderByColumnSpec = null;
              if (sortAction) {
                var col = sortAction.refName();
                column = {
                  dimension: col,
                  direction: sortAction.direction
                };
                if (this.sortOnLabel()) {
                  var sortSplitExpression = this.split.splits[col];
                  var sortSplitExpressionType = sortSplitExpression.type;
                  if (sortSplitExpressionType === 'NUMBER' || sortSplitExpressionType === 'NUMBER_RANGE') {
                    (column as any).dimensionOrder = 'alphaNumeric';
                  }
                }
              }

              druidQuery.limitSpec = {
                type: "default",
                limit: 500000,
                columns: [column || this.split.firstSplitName()]
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

    public getIntrospectAttributesWithSegmentMetadata(): Q.Promise<Attributes> {
      var { requester, timeAttribute } = this;

      return requester({
        query: {
          queryType: 'segmentMetadata',
          dataSource: this.getDruidDataSource(),
          merge: true,
          analysisTypes: ["aggregators"],
          lenientAggregatorMerge: true
        }
      }).catch((err: Error) => {
        if (err.message.indexOf('Can not construct instance of io.druid.query.metadata.metadata.SegmentMetadataQuery$AnalysisType') === -1) throw err;

        return requester({
          query: {
            queryType: 'segmentMetadata',
            dataSource: this.getDruidDataSource(),
            merge: true,
            analysisTypes: []
          }
        })
      }).then(segmentMetadataPostProcessFactory(timeAttribute));
    }

    public getIntrospectAttributesWithGet(): Q.Promise<Attributes> {
      var { requester, timeAttribute } = this;

      return requester({
        query: {
          queryType: 'introspect',
          dataSource: this.getDruidDataSource()
        }
      }).then(introspectPostProcessFactory(timeAttribute))
    }

    public getIntrospectAttributes(): Q.Promise<Attributes> {
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
          throw new Error('invalid params');
      }
    }
  }
  External.register(DruidExternal);
}
