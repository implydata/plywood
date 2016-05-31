module Plywood {
  const DUMMY_NAME = '!DUMMY';

  const DEFAULT_TIME_ATTRIBUTE = '__time';

  const AGGREGATE_TO_DRUID: Lookup<string> = {
    count: "count",
    sum: "doubleSum",
    min: "doubleMin",
    max: "doubleMax"
  };

  const AGGREGATE_TO_FUNCTION: Lookup<Function> = {
    sum: (a: string, b:string) => `${a}+${b}`,
    min: (a: string, b:string) => `Math.min(${a},${b})`,
    max: (a: string, b:string) => `Math.max(${a},${b})`
  };

  const AGGREGATE_TO_ZERO: Lookup<string> = {
    sum: "0",
    min: "Infinity",
    max: "-Infinity"
  };

  function expressionNeedsAlphaNumericSort(ex: Expression): boolean {
    var type = ex.type;
    return (type === 'NUMBER' || type === 'NUMBER_RANGE');
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
    intervals: Druid.Intervals;
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
    timestampLabel?: string;
    granularity: Druid.Granularity | string;
    dimension?: Druid.DimensionSpec;
    dimensions?: Druid.DimensionSpec[];
    postProcess: PostProcess;
  }

  function cleanDatumInPlace(datum: Datum): void {
    for (var k in datum) {
      if (k[0] === '!') delete datum[k];
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

  function valuePostProcess(res: Druid.TimeseriesResults): PlywoodValue {
    if (!correctTimeseriesResult(res)) {
      var err = new Error("unexpected result from Druid (all / value)");
      (<any>err).result = res; // ToDo: special error type
      throw err;
    }
    if (!res.length) {
      return 0;
    }
    return res[0].result[External.VALUE_NAME];
  }

  function totalPostProcessFactory(applies: ApplyAction[]) {
    return (res: Druid.TimeseriesResults): Dataset => {
      if (!correctTimeseriesResult(res)) {
        var err = new Error("unexpected result from Druid (all)");
        (<any>err).result = res; // ToDo: special error type
        throw err;
      }
      if (!res.length) {
        return new Dataset({ data: [External.makeZeroDatum(applies)] });
      }
      var datum = res[0].result;
      cleanDatumInPlace(datum);
      return new Dataset({ data: [datum] });
    };
  }

  function wrapFunctionTryCatch(lines: string[]): string {
    return 'function(s){try{\n' + lines.filter(Boolean).join('\n') + '\n}catch(e){return null;}}';
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

  function selectNormalizerFactory(timestampLabel: string): Normalizer {
    return (results: Druid.SelectResults[]): Datum[] => {
      var data: Datum[] = [];
      for (var result of results) {
        if (!correctSelectResult(result)) {
          var err = new Error("unexpected result from Druid (select)");
          (<any>err).result = result; // ToDo: special error type
          throw err;
        }
        if (result.length === 0) continue;
        var events = result[0].result.events;
        for (var event of events) {
          var datum: Datum = event.event;
          if (timestampLabel != null) {
            // The __time dimension always returns as 'timestamp' for some reason
            datum[timestampLabel] = datum['timestamp'];
          }
          delete datum['timestamp'];
          cleanDatumInPlace(datum);
          data.push(datum);
        }
      }
      return data;
    };
  }

  function postProcessFactory(normalizer: Normalizer, inflaters: Inflater[], attributes: Attributes) {
    return (res: any): Dataset => {
      var data = normalizer(res);
      var n = data.length;
      for (var inflater of inflaters) {
        for (var i = 0; i < n; i++) {
          inflater(data[i], i, data);
        }
      }
      return new Dataset({ data, attributes });
    };
  }

  function selectNextFactory(limit: number, descending: boolean): NextFn<Druid.Query, Druid.SelectResults> {
    var resultsSoFar = 0;
    return (prevQuery, prevResult) => {
      if (!correctSelectResult(prevResult)) {
        var err = new Error("unexpected result from Druid (select / partial)");
        (<any>err).result = prevResult; // ToDo: special error type
        throw err;
      }

      if (prevResult.length === 0) return null; // Out of results: done!

      var { pagingIdentifiers, events } = prevResult[0].result;
      if (events.length < prevQuery.pagingSpec.threshold) return null; // Less results than asked for: done!

      resultsSoFar += events.length;
      if (resultsSoFar >= limit) return null; // Got enough results overall: done!

      var pagingIdentifiers = DruidExternal.movePagingIdentifiers(pagingIdentifiers, descending ? -1 : 1);
      prevQuery.pagingSpec.pagingIdentifiers = pagingIdentifiers;
      prevQuery.pagingSpec.threshold = Math.min(limit - resultsSoFar, DruidExternal.SELECT_MAX_LIMIT);
      return prevQuery;
    }
  }

  // Introspect

  export interface IntrospectPostProcess {
    (result: any): Attributes;
  }

  function generateMakerAction(aggregation: Druid.Aggregation): Action {
    if (!aggregation) return null;
    var { type, fieldName } = aggregation;

    // Hacky way to guess at a count
    if (type === 'longSum' && fieldName === 'count') {
      return new CountAction({});
    }

    if (!fieldName) {
      var { fieldNames } = aggregation;
      if (!Array.isArray(fieldNames) || fieldNames.length !== 1) return null;
      fieldName = fieldNames[0];
    }

    var expression = $(fieldName);
    switch (type) {
      case "count":
        return new CountAction({});

      case "doubleSum":
      case "longSum":
        return new SumAction({ expression });

      case "javascript":
        const { fnAggregate, fnCombine } = aggregation;
        if (fnAggregate !== fnCombine || fnCombine.indexOf('+') === -1) return null;
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

            case 'thetaSketch':
              attributes.push(new ThetaAttributeInfo({ name }));
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

    static TRUE_INTERVAL = "1000/3000";
    static FALSE_INTERVAL = "1000/1001";

    static VALID_INTROSPECTION_STRATEGIES = ['segment-metadata-fallback', 'segment-metadata-only', 'datasource-get'];
    static DEFAULT_INTROSPECTION_STRATEGY = 'segment-metadata-fallback';

    static SELECT_INIT_LIMIT = 50;
    static SELECT_MAX_LIMIT = 10000;

    static fromJS(parameters: ExternalJS, requester: Requester.PlywoodRequester<any>): DruidExternal {
      // Back compat:
      if (typeof (<any>parameters).druidVersion === 'string') {
        parameters.version = (<any>parameters).druidVersion;
        console.warn(`'druidVersion' parameter is deprecated, use 'version: ${parameters.version}' instead`);
      }

      var value: ExternalValue = External.jsToValue(parameters, requester);
      value.dataSource = parameters.dataSource;
      value.timeAttribute = parameters.timeAttribute;
      value.customAggregations = parameters.customAggregations || {};
      value.allowEternity = Boolean(parameters.allowEternity);
      value.allowSelectQueries = Boolean(parameters.allowSelectQueries);
      value.introspectionStrategy = parameters.introspectionStrategy;
      value.exactResultsOnly = Boolean(parameters.exactResultsOnly);
      value.context = parameters.context;
      return new DruidExternal(value);
    }

    static getSourceList(requester: Requester.PlywoodRequester<any>): Q.Promise<string[]> {
      return requester({ query: { queryType: 'sourceList' } })
        .then((sources) => {
          if (!Array.isArray(sources)) throw new Error('invalid sources response');
          return sources.sort();
        })
    }

    static getVersion(requester: Requester.PlywoodRequester<any>): Q.Promise<string> {
      return requester({
        query: {
          queryType: 'status'
        }
      })
        .then(
          ((res) => External.extractVersion(res.version)),
          () => null
        )
    }

    /**
     * A paging identifier typically looks like this:
     * { "wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9": 4 }
     */
    static movePagingIdentifiers(pagingIdentifiers: Druid.PagingIdentifiers, increment: number): Druid.PagingIdentifiers {
      var newPagingIdentifiers: Druid.PagingIdentifiers = {};
      for (var key in pagingIdentifiers) {
        if (!hasOwnProperty(pagingIdentifiers, key)) continue;
        newPagingIdentifiers[key] = pagingIdentifiers[key] + increment;
      }
      return newPagingIdentifiers;
    }


    static TIME_PART_TO_FORMAT: Lookup<string> = {
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

    static TIME_PART_TO_EXPR: Lookup<string> = {
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
      YEAR: "d.getYearOfEra()"
    };

    static timePartToExtraction(part: string, timezone: Timezone): Druid.ExtractionFn {
      var format = DruidExternal.TIME_PART_TO_FORMAT[part];
      if (format) {
        return {
          "format": format,
          "locale": "en-US",
          "timeZone": timezone.toString(),
          "type": "timeFormat"
        }
      } else {
        var expr = DruidExternal.TIME_PART_TO_EXPR[part];
        if (!expr) throw new Error(`can not part on ${part}`);
        return {
          type: 'javascript',
          'function': wrapFunctionTryCatch([
            'var d = new org.joda.time.DateTime(s);',
            timezone.isUTC() ? null : `d = d.withZone(org.joda.time.DateTimeZone.forID(${JSON.stringify(timezone)}));`,
            `d = ${expr};`,
            'return d;'
          ])
        }
      }
    }


    static SPAN_TO_FLOOR_FORMAT: Lookup<string> = {
      second: "yyyy-MM-dd'T'HH:mm:ss'Z",
      minute: "yyyy-MM-dd'T'HH:mm'Z",
      hour: "yyyy-MM-dd'T'HH':00Z",
      day: "yyyy-MM-dd'Z",
      month: "yyyy-MM'-01Z",
      year: "yyyy'-01-01Z"
    };

    static SPAN_TO_PROPERTY: Lookup<string> = {
      second: 'secondOfMinute',
      minute: 'minuteOfHour',
      hour: 'hourOfDay',
      day: 'dayOfMonth',
      week: 'weekOfWeekyear',
      month: 'monthOfYear',
      year: 'yearOfEra'
    };

    static timeFloorToExtraction(duration: Duration, timezone: Timezone): Druid.ExtractionFn {
      var singleSpan = duration.getSingleSpan();
      var spanValue = duration.getSingleSpanValue();

      if (spanValue === 1 && DruidExternal.SPAN_TO_FLOOR_FORMAT[singleSpan]) {
        return {
          "format": DruidExternal.SPAN_TO_FLOOR_FORMAT[singleSpan],
          "locale": "en-US",
          "timeZone": timezone.toString(),
          "type": "timeFormat"
        }
      } else {
        var prop = DruidExternal.SPAN_TO_PROPERTY[singleSpan];
        if (!prop) throw new Error(`can not floor on ${duration}`);
        return {
          type: 'javascript',
          'function': wrapFunctionTryCatch([
            'var d = new org.joda.time.DateTime(s);',
            timezone.isUTC() ? null : `d = d.withZone(org.joda.time.DateTimeZone.forID(${JSON.stringify(timezone)}));`,
            `d = d.${prop}().roundFloorCopy();`,
            `d = d.${prop}().setCopy(Math.floor(d.${prop}().get() / ${spanValue}) * ${spanValue});`,
            'return d;'
          ])
        }
      }
    }


    public dataSource: string | string[];
    public timeAttribute: string;
    public customAggregations: CustomDruidAggregations;
    public allowEternity: boolean;
    public allowSelectQueries: boolean;
    public introspectionStrategy: string;
    public exactResultsOnly: boolean;
    public context: Lookup<any>;

    constructor(parameters: ExternalValue) {
      super(parameters, dummyObject);
      this._ensureEngine("druid");
      this._ensureMinVersion("0.8.0");
      this.dataSource = parameters.dataSource;
      this.timeAttribute = parameters.timeAttribute || DEFAULT_TIME_ATTRIBUTE;
      this.customAggregations = parameters.customAggregations;
      this.allowEternity = parameters.allowEternity;
      this.allowSelectQueries = parameters.allowSelectQueries;

      var introspectionStrategy = parameters.introspectionStrategy || DruidExternal.DEFAULT_INTROSPECTION_STRATEGY;
      if (DruidExternal.VALID_INTROSPECTION_STRATEGIES.indexOf(introspectionStrategy) === -1) {
        throw new Error(`invalid introspectionStrategy '${introspectionStrategy}'`);
      }
      this.introspectionStrategy = introspectionStrategy;

      this.exactResultsOnly = parameters.exactResultsOnly;
      this.context = parameters.context;
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
      return value;
    }

    public toJS(): ExternalJS {
      var js: ExternalJS = super.toJS();
      js.dataSource = this.dataSource;
      if (this.timeAttribute !== DEFAULT_TIME_ATTRIBUTE) js.timeAttribute = this.timeAttribute;
      if (helper.nonEmptyLookup(this.customAggregations)) js.customAggregations = this.customAggregations;
      if (this.allowEternity) js.allowEternity = true;
      if (this.allowSelectQueries) js.allowSelectQueries = true;
      if (this.introspectionStrategy !== DruidExternal.DEFAULT_INTROSPECTION_STRATEGY) js.introspectionStrategy = this.introspectionStrategy;
      if (this.exactResultsOnly) js.exactResultsOnly = true;
      if (this.context) js.context = this.context;
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
        dictEqual(this.context, other.context);
    }

    public getSingleReferenceAttributeInfo(ex: Expression): AttributeInfo {
      var freeReferences = ex.getFreeReferences();
      if (freeReferences.length !== 1) throw new Error(`can not translate multi reference expression ${ex} to Druid`);
      var referenceName = freeReferences[0];
      return this.getAttributesInfo(referenceName);
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
      if (this.isTimeseries()) {
        if (sortAction.direction !== 'ascending') return false;
        return sortAction.refName() === this.split.firstSplitName();

      } else if (this.mode === 'raw') {
        if (sortAction.refName() !== this.timeAttribute) return false;
        if (this.versionBefore('0.9.0')) return sortAction.direction === 'ascending';
        return true;

      } else {
        return true;
      }
    }

    public canHandleLimit(limitAction: LimitAction): boolean {
      return !this.isTimeseries();
    }

    public canHandleHavingFilter(ex: Expression): boolean {
      return !this.limit;
    }

    // -----------------

    public isTimeseries(): boolean {
      const { split } = this;
      if (!split || split.isMultiSplit()) return false;
      var splitExpression = split.firstSplitExpression();
      if (this.isTimeRef(splitExpression)) return true;

      if (splitExpression instanceof ChainExpression) {
        var actions = splitExpression.actions;
        if (actions.length !== 1) return false;
        var action = actions[0].action;
        return action === 'timeBucket' || action === 'timeFloor';
      }
      return false;
    }

    public getDruidDataSource(): Druid.DataSource {
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

    // ========= FILTERS =========

    public makeJavaScriptFilter(ex: Expression): Druid.Filter {
      var attributeInfo = this.getSingleReferenceAttributeInfo(ex);
      return {
        type: "javascript",
        dimension: attributeInfo.name,
        "function": ex.getJSFn('d')
      };
    }

    public makeExtractionFilter(ex: Expression): Druid.Filter {
      var attributeInfo = this.getSingleReferenceAttributeInfo(ex);
      var extractionFn = this.expressionToExtractionFn(ex);

      return {
        type: "extraction",
        dimension: attributeInfo.name,
        extractionFn: extractionFn,
        value: "true"
      }
    }

    // Makes a filter of (ex = value) or (value in ex) which are the same in Druid
    public makeSelectorFilter(ex: Expression, value: any): Druid.Filter {
      var attributeInfo = this.getSingleReferenceAttributeInfo(ex);
      if (attributeInfo.unsplitable) {
        throw new Error(`can not convert ${ex} = ${value} to filter because it references an un-filterable metric '${attributeInfo.name}' which is most likely rolled up.`)
      }

      var extractionFn = this.expressionToExtractionFn(ex);
      var dimensionName = attributeInfo.name === this.timeAttribute ? '__time' : attributeInfo.name;

      // Kill range
      if (Range.isRange(value)) value = value.start;

      var druidFilter: Druid.Filter = {
        type: "selector",
        dimension: dimensionName,
        value: attributeInfo.serialize(value)
      };
      if (extractionFn) {
        druidFilter.type = "extraction";
        druidFilter.extractionFn = extractionFn;
        if (this.versionBefore('0.9.0') && druidFilter.value === null) druidFilter.value = '';
      }
      return druidFilter;
    }

    public makeInFilter(ex: Expression, valueSet: Set): Druid.Filter {
      var attributeInfo = this.getSingleReferenceAttributeInfo(ex);
      var extractionFn = this.expressionToExtractionFn(ex);
      var elements = valueSet.elements;
      if (elements.length < 2 || extractionFn || this.versionBefore('0.9.0')) {
        var fields = elements.map((value: string) => {
          return this.makeSelectorFilter(ex, value);
        });

        return fields.length === 1 ? fields[0] : { type: "or", fields };
      }

      return {
        type: 'in',
        dimension: attributeInfo.name,
        values: elements.map((value: string) => attributeInfo.serialize(value))
      };
    }

    public makeBoundFilter(ex: Expression, range: PlywoodRange): Druid.Filter {
      var r0 = range.start;
      var r1 = range.end;
      var bounds = range.bounds;

      if (this.versionBefore('0.9.0') || r0 < 0 || r1 < 0) {
        return this.makeJavaScriptFilter(ex.in(range));
      }

      var attributeInfo = this.getSingleReferenceAttributeInfo(ex);

      var boundFilter: Druid.Filter = {
        type: "bound",
        dimension: attributeInfo.name
      };
      if (NumberRange.isNumberRange(range)) {
        boundFilter.alphaNumeric = true;
      }
      if (r0 != null) {
        boundFilter.lower = isDate(r0) ? r0.toISOString() : r0;
        if (bounds[0] === '(') boundFilter.lowerStrict = true;
      }
      if (r1 != null) {
        boundFilter.upper = isDate(r1) ? r1.toISOString() : r1;
        if (bounds[1] === ')') boundFilter.upperStrict = true;
      }
      return boundFilter;
    }

    public makeRegexFilter(ex: Expression, regex: string): Druid.Filter {
      var attributeInfo = this.getSingleReferenceAttributeInfo(ex);
      var extractionFn = this.expressionToExtractionFn(ex);

      if (extractionFn) {
        return this.makeExtractionFilter(ex.match(regex));
      }

      return {
        type: "regex",
        dimension: attributeInfo.name,
        pattern: regex
      };
    }

    public makeContainsFilter(lhs: Expression, rhs: Expression, compare: string): Druid.Filter {
      if (rhs instanceof LiteralExpression) {
        var attributeInfo = this.getSingleReferenceAttributeInfo(lhs);
        var extractionFn = this.expressionToExtractionFn(lhs);

        if (extractionFn) {
          return this.makeExtractionFilter(lhs.contains(rhs, compare));
        }

        if (compare === ContainsAction.IGNORE_CASE) {
          return {
            type: "search",
            dimension: attributeInfo.name,
            query: {
              type: "insensitive_contains",
              value: rhs.value
            }
          };
        } else {
          return this.makeJavaScriptFilter(lhs.contains(rhs, compare));
        }
      } else {
        return this.makeJavaScriptFilter(lhs.contains(rhs, compare));
      }
    }

    public timelessFilterToDruid(filter: Expression, aggregatorFilter: boolean): Druid.Filter {
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

      } else if (filter instanceof ChainExpression) {
        var pattern: Expression[];
        if (pattern = filter.getExpressionPattern('and')) {
          return {
            type: 'and',
            fields: pattern.map(p => this.timelessFilterToDruid(p, aggregatorFilter))
          };
        }
        if (pattern = filter.getExpressionPattern('or')) {
          return {
            type: 'or',
            fields: pattern.map(p => this.timelessFilterToDruid(p, aggregatorFilter))
          };
        }

        var filterAction = filter.lastAction();
        var rhs = filterAction.expression;
        var lhs = filter.popAction();

        if (filterAction instanceof NotAction) {
          return {
            type: 'not',
            field: this.timelessFilterToDruid(lhs, aggregatorFilter)
          };
        }

        // Do the basic selectors

        if (lhs instanceof LiteralExpression) {
          // Special handling for r('some_tag').in($tags)
          if (filterAction.action !== 'in') throw new Error(`can not convert ${filter} to Druid filter`);
          return this.makeSelectorFilter(rhs, lhs.value);
        }

        if (filterAction instanceof IsAction) {
          if (rhs instanceof LiteralExpression) {
            return this.makeSelectorFilter(lhs, rhs.value);
          } else {
            throw new Error(`can not convert ${filter} to Druid filter`);
          }
        }


        var freeReferences = filter.getFreeReferences();
        if (freeReferences.length !== 1) throw new Error(`can not convert multi reference filter ${filter} to Druid filter`);
        var referenceName = freeReferences[0];
        var attributeInfo = this.getAttributesInfo(referenceName);
        if (attributeInfo.unsplitable) {
          throw new Error(`can not convert ${filter} to filter because it references an un-filterable metric '${referenceName}' which is most likely rolled up.`)
        }

        if (filterAction instanceof InAction || filterAction instanceof OverlapAction) {
          if (rhs instanceof LiteralExpression) {
            var rhsType = rhs.type;
            if (rhsType === 'SET/STRING' || rhsType === 'SET/NUMBER' || rhsType === 'SET/NULL') {
              return this.makeInFilter(lhs, rhs.value);

            } else if (rhsType === 'NUMBER_RANGE' || rhsType === 'TIME_RANGE') {
              return this.makeBoundFilter(lhs, rhs.value);

            } else if (rhsType === 'SET/NUMBER_RANGE' || rhsType === 'SET/TIME_RANGE') {
              var elements = rhs.value.elements;
              var fields = elements.map((range: PlywoodRange) => {
                return this.makeBoundFilter(lhs, range);
              });

              return fields.length === 1 ? fields[0] : { type: "or", fields };

            } else {
              throw new Error(`not supported IN rhs type ${rhsType}`);

            }
          } else {
            throw new Error(`can not convert ${filter} to Druid filter`);
          }
        }

        if (aggregatorFilter) {
          if (this.versionBefore('0.8.2')) throw new Error(`can not express aggregate filter ${filter} in druid < 0.8.2`);
          return this.makeExtractionFilter(filter);
        }

        if (filterAction instanceof MatchAction) {
          return this.makeRegexFilter(lhs, filterAction.regexp);
        }

        if (filterAction instanceof ContainsAction) {
          return this.makeContainsFilter(lhs, rhs, filterAction.compare);
        }

      }

      throw new Error(`could not convert filter ${filter} to Druid filter`);
    }

    public timeFilterToIntervals(filter: Expression): Druid.Intervals {
      if (filter.type !== 'BOOLEAN') throw new Error("must be a BOOLEAN filter");

      if (filter instanceof LiteralExpression) {
        if (!filter.value) return DruidExternal.FALSE_INTERVAL;
        if (!this.allowEternity) throw new Error('must filter on time unless the allowEternity flag is set');
        return DruidExternal.TRUE_INTERVAL;

      } else if (filter instanceof ChainExpression) {
        var lhs = filter.expression;
        var actions = filter.actions;
        if (actions.length !== 1) throw new Error(`can not convert ${filter} to Druid interval`);
        var filterAction = actions[0];
        var rhs = filterAction.expression;

        if (filterAction instanceof IsAction) {
          if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
            return TimeRange.intervalFromDate(rhs.value);
          } else {
            throw new Error(`can not convert ${filter} to Druid interval`);
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
              throw new Error(`not supported ${rhsType} for time filtering`);
            }

            var intervals = timeRanges.map(timeRange => timeRange.toInterval());
            return intervals.length === 1 ? intervals[0] : intervals;
          } else {
            throw new Error(`can not convert ${filter} to Druid interval`);
          }

        } else {
          throw new Error(`can not convert ${filter} to Druid interval`);
        }

      } else {
        throw new Error(`can not convert ${filter} to Druid interval`);
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
        const { timeAttribute } = this;
        const { extract, rest } = filter.extractFromAnd(ex => {
          if (ex instanceof ChainExpression) {
            var op = ex.expression;
            var actions = ex.actions;
            if (op instanceof RefExpression) {
              if (!(op.name === timeAttribute && actions.length === 1)) return false;
              var action = actions[0].action;
              return action === 'is' || action === 'in';
            }
          }
          return false;
        });
        return {
          intervals: this.timeFilterToIntervals(extract),
          filter: this.timelessFilterToDruid(rest, false)
        }
      }
    }

    public isTimeRef(ex: Expression) {
      return ex instanceof RefExpression && ex.name === this.timeAttribute;
    }

    public splitExpressionToGranularityInflater(splitExpression: Expression, label: string): GranularityInflater {
      if (this.isTimeRef(splitExpression)) {
        return {
          granularity: 'none',
          inflater: External.timeInflaterFactory(label)
        };

      } else if (splitExpression instanceof ChainExpression) {
        var splitActions = splitExpression.actions;
        if (this.isTimeRef(splitExpression.expression) && splitActions.length === 1) {
          var action = splitActions[0];
          if (action instanceof TimeBucketAction || action instanceof TimeFloorAction) {
            var duration = action.duration;
            var timezone = action.getTimezone();
            return {
              granularity: {
                type: "period",
                period: duration.toString(),
                timeZone: timezone.toString()
              },
              inflater: action.action === 'timeBucket' ?
                External.timeRangeInflaterFactory(label, duration, timezone) :
                External.timeInflaterFactory(label)
            };
          }
        }
      }

      return null;
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // Extraction functions

    public expressionToExtractionFn(expression: Expression): Druid.ExtractionFn {
      var extractionFns: Druid.ExtractionFn[] = [];
      this._expressionToExtractionFns(expression, extractionFns);
      switch (extractionFns.length) {
        case 0: return null;
        case 1: return extractionFns[0];
        default:
          if (extractionFns.every(extractionFn => extractionFn.type === 'javascript')) {
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

          return { type: 'cascade', extractionFns };
      }
    }

    private _expressionToExtractionFns(expression: Expression, extractionFns: Druid.ExtractionFn[]): void {
      var freeReferences = expression.getFreeReferences();
      if (freeReferences.length !== 1) {
        throw new Error(`must have 1 reference (has ${freeReferences.length}): ${expression}`);
      }

      if (expression instanceof RefExpression) {
        this._processRefExtractionFn(expression, extractionFns);
        return;
      }

      if (expression instanceof ChainExpression) {
        var lead = expression.expression;
        var actions = expression.actions;

        var i = 0;
        var curAction: Action = actions[0];
        var concatPrefix: Expression[] = [];
        if (curAction.action === 'concat') {
          concatPrefix.push(lead);
          while (curAction && curAction.action === 'concat') {
            concatPrefix.push(curAction.expression);
            curAction = actions[++i];
          }
          this._processConcatExtractionFn(concatPrefix, extractionFns);

        } else if (lead.type === 'NUMBER' && (expression.type === 'NUMBER' || expression.type === 'NUMBER_RANGE')) {
          extractionFns.push(this.expressionToJavaScriptExtractionFn(expression));
          return;

        } else if (!lead.isOp('ref')) {
          // Concat is the only thing allowed to have a non leading ref, the rest must be $ref.someFunction
          throw new Error(`can not convert complex: ${lead}`);
        }

        while (curAction) {
          var nextAction = actions[i + 1];
          var extractionFn: Druid.ExtractionFn;
          if (nextAction instanceof FallbackAction) {
            extractionFn = this.actionToExtractionFn(curAction, nextAction);
            i++; // Skip it
          } else {
            extractionFn = this.actionToExtractionFn(curAction, null);
          }
          extractionFns.push(extractionFn);
          curAction = actions[++i];
        }
      }
    }

    private _processRefExtractionFn(ref: RefExpression, extractionFns: Druid.ExtractionFn[]): void {
      var attributeInfo = this.getAttributesInfo(ref.name);

      if (attributeInfo instanceof RangeAttributeInfo) {
        extractionFns.push(this.getRangeBucketingExtractionFn(attributeInfo, null));
        return;
      }

      if (ref.type === 'BOOLEAN') {
        extractionFns.push({
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
        });
        return;
      }
    }

    public actionToExtractionFn(action: Action, fallbackAction: FallbackAction): Druid.ExtractionFn {
      if (action.action === 'extract' || action.action === 'lookup') {
        var retainMissingValue = false;
        var replaceMissingValueWith: any = null;

        if (fallbackAction) {
          var fallbackExpression = fallbackAction.expression;
          if (fallbackExpression.isOp("ref")) {
            // the ref has to be the same as the argument because we can't refer to other dimensions
            // so the only option would be for it to be equal to original dimension
            retainMissingValue = true;
          } else if (fallbackExpression.isOp("literal")) {
            replaceMissingValueWith = fallbackExpression.getLiteralValue();
          } else {
            throw new Error(`unsupported fallback expression: ${fallbackExpression}`);
          }
        }

        if (action instanceof ExtractAction) {
          // retainMissingValue === false is not supported in old druid nor is replaceMissingValueWith in regex extractionFn
          // we want to use a js function if we are using an old version of Druid
          if (this.versionBefore('0.9.0') && (retainMissingValue === false || replaceMissingValueWith !== null)) {
            return this.actionToJavaScriptExtractionFn(action);
          }

          var regexExtractionFn: Druid.ExtractionFn = {
            type: "regex",
            expr: action.regexp
          };

          if (!retainMissingValue) {
            regexExtractionFn.replaceMissingValue = true;
          }

          if (replaceMissingValueWith !== null) {
            regexExtractionFn.replaceMissingValueWith = replaceMissingValueWith;
          }

          return regexExtractionFn;
        }

        if (action instanceof LookupAction) {
          var lookupExtractionFn: Druid.ExtractionFn = {
            type: "lookup",
            lookup: {
              type: "namespace",
              "namespace": action.lookup
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
      }

      // After this point nothing supports a native fallback
      if (fallbackAction) {
        throw new Error(`unsupported fallback after ${action.action} action`);
      }

      // This is an action that returns a boolean
      if (action.getOutputType(null) === 'BOOLEAN') {
        return this.actionToJavaScriptExtractionFn(action);
      }

      if (action instanceof SubstrAction) {
        if (this.versionBefore('0.9.0')) return this.actionToJavaScriptExtractionFn(action);
        return {
          type: "substring",
          index: action.position,
          length: action.length
        };
      }

      if (action instanceof TimeBucketAction || action instanceof TimeFloorAction) {
        return DruidExternal.timeFloorToExtraction(action.duration, action.getTimezone());
      }

      if (action instanceof TimePartAction) {
        return DruidExternal.timePartToExtraction(action.part, action.getTimezone());
      }

      if (action instanceof NumberBucketAction) {
        return this.actionToJavaScriptExtractionFn(action);
      }

      if (action instanceof AbsoluteAction || action instanceof PowerAction) {
        return this.actionToJavaScriptExtractionFn(action);
      }

      if (action instanceof FallbackAction && action.expression.isOp('literal')) {
        return {
          type: "lookup",
          retainMissingValue: true,
          lookup: {
            type: "map",
            map: {
              "": action.getLiteralValue()
            }
          }
        };
      }

      throw new Error(`can not covert ${action} to extractionFn`);
    }

    private _processConcatExtractionFn(pattern: Expression[], extractionFns: Druid.ExtractionFn[]): void {
      if (this.versionBefore('0.9.1')) {
        // Druid < 0.9.1 behaves badly on null https://github.com/druid-io/druid/issues/2706 (does not have nullHandling: 'returnNull')
        extractionFns.push({
          type: "javascript",
          'function': Expression.concat(pattern).getJSFn('d'),
          injective: true
        });
        return;
      }

      var format = pattern.map(ex => {
        if (ex instanceof LiteralExpression) {
          return ex.value.replace(/%/g, '\\%');
        }
        if (!ex.isOp('ref')) {
          this._expressionToExtractionFns(ex, extractionFns);
        }
        return '%s';
      }).join('');

      extractionFns.push({
        type: 'stringFormat',
        format,
        nullHandling: 'returnNull'
      })
    }

    public actionToJavaScriptExtractionFn(action: Action): Druid.ExtractionFn {
      return this.expressionToJavaScriptExtractionFn($('x').performAction(action));
    }

    public expressionToJavaScriptExtractionFn(ex: Expression): Druid.ExtractionFn {
      return {
        type: "javascript",
        'function': ex.getJSFn('d')
      };
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

    public expressionToDimensionInflater(expression: Expression, label: string): DimensionInflater {
      var freeReferences = expression.getFreeReferences();
      if (freeReferences.length !== 1) {
        throw new Error(`must have 1 reference (has ${freeReferences.length}): ${expression}`);
      }
      var referenceName = freeReferences[0];

      var attributeInfo = this.getAttributesInfo(referenceName);
      if (attributeInfo.unsplitable) {
        throw new Error(`can not convert ${expression} to split because it references an un-splitable metric '${referenceName}' which is most likely rolled up.`)
      }

      var extractionFn = this.expressionToExtractionFn(expression);

      var simpleInflater = External.getSimpleInflater(expression, label);

      var dimension: Druid.DimensionSpecFull = {
        type: "default",
        dimension: referenceName === this.timeAttribute ? '__time' : referenceName,
        outputName: label
      };
      if (extractionFn) {
        dimension.type = "extraction";
        dimension.extractionFn = extractionFn;
      }

      if (expression instanceof RefExpression) {

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

      if (expression instanceof ChainExpression) {
        var splitAction = expression.lastAction();

        if (splitAction instanceof TimeBucketAction) {
          return {
            dimension,
            inflater: External.timeRangeInflaterFactory(label, splitAction.duration, splitAction.getTimezone())
          };
        }

        if (splitAction instanceof TimePartAction) {
          return {
            dimension,
            inflater: simpleInflater
          };
        }

        if (splitAction instanceof NumberBucketAction) {
          if (attributeInfo.type === 'NUMBER') {
            //var floorExpression = continuousFloorExpression("d", "Math.floor", splitAction.size, splitAction.offset);
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

      var effectiveType = unwrapSetType(expression.type);
      if (simpleInflater || effectiveType === 'STRING') {
        return {
          dimension,
          inflater: simpleInflater
        };
      }

      throw new Error(`could not convert ${expression} to a Druid dimension`);
    }

    public splitToDruid(split: SplitAction): DruidSplit {
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

          var { dimension, inflater } = this.expressionToDimensionInflater(expression, name);
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
          postProcess: postProcessFactory(
            groupByNormalizerFactory(timestampLabel),
            inflaters,
            null
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
            [granularityInflater.inflater],
            null
          )
        };
      }

      var dimensionInflater = this.expressionToDimensionInflater(splitExpression, label);
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
          postProcess: postProcessFactory(topNNormalizer, inflaters, null)
        };
      }

      return {
        queryType: 'groupBy',
        dimensions: [dimensionInflater.dimension],
        granularity: 'all',
        postProcess: postProcessFactory(groupByNormalizerFactory(), inflaters, null)
      };
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
          var aggregationType = aggregation.type;
          if (aggregationType === 'filtered') aggregationType = aggregation.aggregator.type;
          return this.getAccessTypeForAggregation(aggregationType);
        }
      }
      return 'fieldAccess'; // If not found it must be a post-agg
    }

    public expressionToPostAggregation(ex: Expression, aggregations: Druid.Aggregation[], postAggregations: Druid.PostAggregation[]): Druid.PostAggregation {
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
        var lastAction = ex.lastAction();

        if (lastAction instanceof AbsoluteAction || lastAction instanceof PowerAction || lastAction instanceof FallbackAction) {
          var fieldNameRefs = ex.getFreeReferences();
          var fieldNames = fieldNameRefs.map(fieldNameRef => {
            var accessType = this.getAccessType(aggregations, fieldNameRef);
            if (accessType === 'fieldAccess') return fieldNameRef;
            var fieldNameRefTemp = '!F_' + fieldNameRef;
            postAggregations.push({
              name: fieldNameRefTemp,
              type: accessType,
              fieldName: fieldNameRef
            });
            return fieldNameRefTemp;
          });

          return {
            type: 'javascript',
            fieldNames: fieldNames,
            'function': `function(${fieldNameRefs.map(RefExpression.toSimpleName)}) { return ${ex.getJS(null)}; }`
          };
        }

        var pattern: Expression[];
        if (pattern = ex.getExpressionPattern('add')) {
          return {
            type: 'arithmetic',
            fn: '+',
            fields: pattern.map(e => this.expressionToPostAggregation(e, aggregations, postAggregations))
          };
        }
        if (pattern = ex.getExpressionPattern('subtract')) {
          return {
            type: 'arithmetic',
            fn: '-',
            fields: pattern.map(e => this.expressionToPostAggregation(e, aggregations, postAggregations))
          };
        }
        if (pattern = ex.getExpressionPattern('multiply')) {
          return {
            type: 'arithmetic',
            fn: '*',
            fields: pattern.map(e => this.expressionToPostAggregation(e, aggregations, postAggregations))
          };
        }
        if (pattern = ex.getExpressionPattern('divide')) {
          return {
            type: 'arithmetic',
            fn: '/',
            fields: pattern.map(e => this.expressionToPostAggregation(e, aggregations, postAggregations))
          };
        }
        throw new Error(`can not convert chain to post agg: ${ex}`);

      } else {
        throw new Error(`can not convert expression to post agg: ${ex}`);
      }
    }

    public applyToPostAggregation(action: ApplyAction, aggregations: Druid.Aggregation[], postAggregations: Druid.PostAggregation[]): void {
      var postAgg = this.expressionToPostAggregation(action.expression, aggregations, postAggregations);
      postAgg.name = action.name;
      postAggregations.push(postAgg);
    }

    public makeNativeAggregateFilter(filterExpression: Expression, aggregator: Druid.Aggregation): Druid.Aggregation {
      return {
        type: "filtered",
        name: aggregator.name,
        filter: this.timelessFilterToDruid(filterExpression, true),
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
          var refName = aggregateExpression.name;
          var attributeInfo = this.getAttributesInfo(refName);
          if (attributeInfo.unsplitable) {
            aggregation.fieldName = refName;
          } else {
            return this.makeJavaScriptAggregation(name, aggregateAction);
          }
        } else {
          return this.makeJavaScriptAggregation(name, aggregateAction);
        }
      }

      return aggregation;
    }

    public makeCountDistinctAggregation(name: string, action: CountDistinctAction, postAggregations: Druid.PostAggregation[]): Druid.Aggregation {
      if (this.exactResultsOnly) {
        throw new Error("approximate query not allowed");
      }

      var attribute = action.expression;
      if (attribute instanceof RefExpression) {
        var attributeName = attribute.name;
      } else {
        throw new Error(`can not compute countDistinct on derived attribute: ${attribute}`);
      }

      var attributeInfo = this.getAttributesInfo(attributeName);
      if (attributeInfo instanceof UniqueAttributeInfo) {
        return {
          name: name,
          type: "hyperUnique",
          fieldName: attributeName
        };

      } else if (attributeInfo instanceof ThetaAttributeInfo) {
        var tempName = '!Theta_' + name;
        postAggregations.push({
          type: "thetaSketchEstimate",
          name: name,
          field: { type: 'fieldAccess', fieldName: tempName }
        });

        return {
          name: tempName,
          type: "thetaSketch",
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

    public makeQuantileAggregation(name: string, action: QuantileAction, postAggregations: Druid.PostAggregation[]): Druid.Aggregation {
      if (this.exactResultsOnly) {
        throw new Error("approximate query not allowed");
      }

      var attribute = action.expression;
      if (attribute instanceof RefExpression) {
        var attributeName = attribute.name;
      } else {
        throw new Error(`can not compute countDistinct on derived attribute: ${attribute}`);
      }

      var histogramAggregationName = "!H_" + name;
      var aggregation: Druid.Aggregation = {
        name: histogramAggregationName,
        type: "approxHistogramFold",
        fieldName: attributeName
      };

      postAggregations.push({
        name,
        type: "quantile",
        fieldName: histogramAggregationName,
        probability: action.quantile
      });

      return aggregation;
    }

    public makeJavaScriptAggregation(name: string, aggregateAction: Action): Druid.Aggregation {
      var aggregateActionType = aggregateAction.action;
      var aggregateExpression = aggregateAction.expression;

      var aggregateFunction = AGGREGATE_TO_FUNCTION[aggregateActionType];
      if (!aggregateFunction) throw new Error(`Can not convert ${aggregateActionType} to JS`);
      var zero = AGGREGATE_TO_ZERO[aggregateActionType];
      var fieldNames = aggregateExpression.getFreeReferences();
      return {
        name,
        type: "javascript",
        fieldNames: fieldNames,
        fnAggregate: `function(_c,${fieldNames}) { return ${aggregateFunction('_c', aggregateExpression.getJS(null))}; }`,
        fnCombine: `function(a,b) { return ${aggregateFunction('a', 'b')}; }`,
        fnReset: `function() { return ${zero}; }`
      }
    }

    public applyToAggregation(action: ApplyAction, aggregations: Druid.Aggregation[], postAggregations: Druid.PostAggregation[]): void {
      var applyExpression = <ChainExpression>action.expression;
      if (applyExpression.op !== 'chain') throw new Error(`can not convert apply: ${applyExpression}`);

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
          throw new Error(`first action not a filter in: ${applyExpression}`);
        }
        aggregateAction = actions[1];
      } else {
        throw new Error(`can not convert strange apply: ${applyExpression}`);
      }

      var aggregation: Druid.Aggregation;
      switch (aggregateAction.action) {
        case "count":
        case "sum":
        case "min":
        case "max":
          aggregation = this.makeStandardAggregation(action.name, aggregateAction);
          break;

        case "countDistinct":
          aggregation = this.makeCountDistinctAggregation(action.name, <CountDistinctAction>aggregateAction, postAggregations);
          break;

        case "quantile":
          aggregation = this.makeQuantileAggregation(action.name, <QuantileAction>aggregateAction, postAggregations);
          break;

        case "custom":
          aggregation = this.makeCustomAggregation(action.name, <CustomAction>aggregateAction);
          break;

        default:
          throw new Error(`unsupported aggregate action ${aggregateAction.action}`);
      }

      if (filterExpression) {
        aggregation = this.makeNativeAggregateFilter(filterExpression, aggregation);
      }
      aggregations.push(aggregation);
    }

    public getAggregationsAndPostAggregations(applies: ApplyAction[]): AggregationsAndPostAggregations {
      var { aggregateApplies, postAggregateApplies } = External.segregationAggregateApplies(
        applies.map(apply => {
          var expression = apply.expression;
          expression = this.switchToRollupCount(this.inlineDerivedAttributesInAggregate(expression).decomposeAverage()).distribute();
          return <ApplyAction>apply.changeExpression(expression);
        })
      );

      var aggregations: Druid.Aggregation[] = [];
      var postAggregations: Druid.PostAggregation[] = [];

      for (let aggregateApply of aggregateApplies) {
        this.applyToAggregation(aggregateApply, aggregations, postAggregations);
      }

      for (let postAggregateApply of postAggregateApplies) {
        this.applyToPostAggregation(postAggregateApply, aggregations, postAggregations);
      }

      return {
        aggregations,
        postAggregations
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
          throw new Error(`unknown op: ${op}`);
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
      return havingSpecs.length === 1 ? havingSpecs[0] : { type: 'and', havingSpecs };
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
        if (actions.length !== 1) throw new Error(`can not convert ${filter} to Druid interval`);
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
            throw new Error(`can not convert ${filter} to Druid filter`);
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
              throw new Error("can not compute having filter on time");

            } else {
              throw new Error("not supported " + rhsType);
            }
          } else {
            throw new Error(`can not convert ${filter} to Druid having filter`);
          }

        }

      }

      throw new Error(`could not convert filter ${filter} to Druid filter`);
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
      const { applies, context } = this;
      var druidQuery: Druid.Query = {
        queryType: "timeBoundary",
        dataSource: this.getDruidDataSource()
      };

      if (context) {
        druidQuery.context = context;
      }

      if (applies.length === 1) {
        var loneApplyExpression = <ChainExpression>applies[0].expression;
        // Max time only
        druidQuery.bound = loneApplyExpression.actions[0].action + "Time";
        // druidQuery.queryType = "dataSourceMetadata";
      }

      return {
        query: druidQuery,
        postProcess: timeBoundaryPostProcessFactory(applies)
      };
    }

    public getQueryAndPostProcess(): QueryAndPostProcess<Druid.Query> {
      const { mode, applies, sort, limit, context } = this;
      if (applies && applies.length && applies.every(this.isMinMaxTimeApply, this)) {
        return this.getTimeBoundaryQueryAndPostProcess();
      }

      var druidQuery: Druid.Query = {
        queryType: 'timeseries',
        dataSource: this.getDruidDataSource(),
        intervals: null,
        granularity: 'all'
      };

      if (context) {
        druidQuery.context = helper.shallowCopy(context);
      }

      // Filter
      var filterAndIntervals = this.filterToDruid(this.getQueryFilter());
      druidQuery.intervals = filterAndIntervals.intervals;
      if (filterAndIntervals.filter) {
        druidQuery.filter = filterAndIntervals.filter;
      }

      switch (mode) {
        case 'raw':
          if (!this.allowSelectQueries) {
            throw new Error("to issues 'select' queries allowSelectQueries flag must be set");
          }

          var selectDimensions: Druid.DimensionSpec[] = [];
          var selectMetrics: string[] = [];
          var inflaters: Inflater[] = [];

          var timeAttribute = this.timeAttribute;
          var derivedAttributes = this.derivedAttributes;
          var selectedTimeAttribute: string = null;
          var selectedAttributes = this.getSelectedAttributes();
          selectedAttributes.forEach(attribute => {
            var { name, type, unsplitable } = attribute;

            if (name === timeAttribute) {
              selectedTimeAttribute = name;
            } else {
              if (unsplitable) {
                selectMetrics.push(name);
              } else {
                var derivedAttribute = derivedAttributes[name];
                if (derivedAttribute) {
                  if (this.versionBefore('0.9.1') && !/^0\.9\.0-iap/.test(this.version)) {
                    throw new Error(`can not have derived attributes in Druid select in ${this.version}, upgrade to 0.9.1 or 0.9.0-iap`);
                  }
                  var dimensionInflater = this.expressionToDimensionInflater(derivedAttribute, name);
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
          if (!selectDimensions.length) selectDimensions.push(DUMMY_NAME);
          if (!selectMetrics.length) selectMetrics.push(DUMMY_NAME);

          var resultLimit = limit ? limit.limit : Infinity;
          druidQuery.queryType = 'select';
          druidQuery.dimensions = selectDimensions;
          druidQuery.metrics = selectMetrics;
          druidQuery.pagingSpec = {
            "pagingIdentifiers": {},
            "threshold": Math.min(resultLimit, DruidExternal.SELECT_INIT_LIMIT)
          };

          var descending = sort && sort.direction === 'descending';
          if (descending) {
            druidQuery.descending = true;
          }

          return {
            query: druidQuery,
            postProcess: postProcessFactory(selectNormalizerFactory(selectedTimeAttribute), inflaters, selectedAttributes),
            next: selectNextFactory(resultLimit, descending)
          };

        case 'value':
          var aggregationsAndPostAggregations = this.getAggregationsAndPostAggregations([this.toValueApply()]);
          if (aggregationsAndPostAggregations.aggregations.length) {
            druidQuery.aggregations = aggregationsAndPostAggregations.aggregations;
          }
          if (aggregationsAndPostAggregations.postAggregations.length) {
            druidQuery.postAggregations = aggregationsAndPostAggregations.postAggregations;
          }

          return {
            query: druidQuery,
            postProcess: valuePostProcess
          };

        case 'total':
          var aggregationsAndPostAggregations = this.getAggregationsAndPostAggregations(this.applies);
          if (aggregationsAndPostAggregations.aggregations.length) {
            druidQuery.aggregations = aggregationsAndPostAggregations.aggregations;
          }
          if (aggregationsAndPostAggregations.postAggregations.length) {
            druidQuery.postAggregations = aggregationsAndPostAggregations.postAggregations;
          }

          return {
            query: druidQuery,
            postProcess: totalPostProcessFactory(applies)
          };

        case 'split':
          // Split
          var split = this.getQuerySplit();
          var splitSpec = this.splitToDruid(split);
          druidQuery.queryType = splitSpec.queryType;
          druidQuery.granularity = splitSpec.granularity;
          if (splitSpec.dimension) druidQuery.dimension = splitSpec.dimension;
          if (splitSpec.dimensions) druidQuery.dimensions = splitSpec.dimensions;
          var postProcess = splitSpec.postProcess;

          // Apply
          var aggregationsAndPostAggregations = this.getAggregationsAndPostAggregations(applies);
          if (aggregationsAndPostAggregations.aggregations.length) {
            druidQuery.aggregations = aggregationsAndPostAggregations.aggregations;
          } else {
            // Druid hates not having aggregates so add a dummy count
            druidQuery.aggregations = [{ name: DUMMY_NAME, type: "count" }];
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
              if (!druidQuery.context || !hasOwnProperty(druidQuery.context, 'skipEmptyBuckets')) {
                druidQuery.context = druidQuery.context || {};
                druidQuery.context.skipEmptyBuckets = "true"; // This needs to be the string "true" to work with older Druid versions
              }
              break;

            case 'topN':
              var metric: Druid.TopNMetricSpec;
              if (sort) {
                var inverted: boolean;
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
              druidQuery.threshold = limit ? limit.limit : 1000;
              break;

            case 'groupBy':
              var orderByColumn: Druid.OrderByColumnSpecFull = null;
              if (sort) {
                var col = sort.refName();
                orderByColumn = {
                  dimension: col,
                  direction: sort.direction
                };
                if (this.sortOnLabel()) {
                  if (expressionNeedsAlphaNumericSort(split.splits[col])) {
                    orderByColumn.dimensionOrder = 'alphaNumeric';
                  }
                }
              } else { // Going to sortOnLabel implicitly
                // Find the first non primary time key
                var { timestampLabel } = splitSpec;
                var splitKeys = split.keys.filter(k => k !== timestampLabel);
                if (!splitKeys.length) throw new Error('could not find order by column for group by');
                var splitKey = splitKeys[0];
                var keyExpression = split.splits[splitKey];
                orderByColumn = {
                  dimension: splitKey,
                };
                if (expressionNeedsAlphaNumericSort(keyExpression)) {
                  orderByColumn.dimensionOrder = 'alphaNumeric';
                }
              }

              druidQuery.limitSpec = {
                type: "default",
                columns: [orderByColumn || split.firstSplitName()]
              };
              if (limit) {
                druidQuery.limitSpec.limit = limit.limit;
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

    public getIntrospectAttributesWithSegmentMetadata(withAggregators: boolean): Q.Promise<Attributes> {
      var { requester, timeAttribute } = this;

      var query: Druid.Query = {
        queryType: 'segmentMetadata',
        dataSource: this.getDruidDataSource(),
        merge: true,
        analysisTypes: []
      };
      if (withAggregators) {
        query.analysisTypes.push("aggregators");
        query.lenientAggregatorMerge = true;
      }

      var resultPromise = requester({ query }).then(segmentMetadataPostProcessFactory(timeAttribute));

      if (withAggregators) {
        resultPromise = resultPromise.catch((err: Error) => {
          if (err.message.indexOf('Can not construct instance of io.druid.query.metadata.metadata.SegmentMetadataQuery$AnalysisType') === -1) {
            throw err;
          }
          return this.getIntrospectAttributesWithSegmentMetadata(false);
        });
      }

      return resultPromise;
    }

    public getIntrospectAttributesWithGet(): Q.Promise<Attributes> {
      var { requester, timeAttribute } = this;

      return requester({
        query: {
          queryType: 'introspect',
          dataSource: this.getDruidDataSource()
        }
      })
        .then(introspectPostProcessFactory(timeAttribute))
    }

    public getIntrospectAttributes(): Q.Promise<IntrospectResult> {
      var versionPromise = this.version ? Q(this.version) : DruidExternal.getVersion(this.requester);

      return versionPromise.then((version) => {
        var withAggregators = version && !External.versionLessThan(version, '0.9.0');
        var attributePromise: Q.Promise<Attributes>;
        switch (this.introspectionStrategy) {
          case 'segment-metadata-fallback':
            attributePromise = this.getIntrospectAttributesWithSegmentMetadata(withAggregators)
              .catch((err: Error) => {
                if (err.message.indexOf("querySegmentSpec can't be null") === -1) throw err;
                return this.getIntrospectAttributesWithGet();
              });
            break;

          case 'segment-metadata-only':
            attributePromise = this.getIntrospectAttributesWithSegmentMetadata(withAggregators);
            break;

          case 'datasource-get':
            attributePromise = this.getIntrospectAttributesWithGet();
            break;

          default:
            throw new Error('invalid introspectionStrategy');
        }

        return attributePromise.then(attributes => {
          return {
            version,
            attributes
          }
        });
      });
    }
  }
  External.register(DruidExternal);
}
