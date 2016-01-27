// Type definitions for druid.io (version 0.8.2)
// Project: http://druid.io/
// Definitions by: Vadim Ogievetsky <https://github.com/vogievetsky/>
// Definitions: https://github.com/implyio/druid.d.ts

declare module Druid {
    /* ----------------------- *\
    |           Query           |
    \* ----------------------- */

    // http://druid.io/docs/latest/Querying.html#query-context
    interface Context {
        timeout?: number;
        priority?: number;
        queryId?: number;
        useCache?: boolean;
        populateCache?: boolean;
        bySegment?: boolean;
        finalize?: boolean;
        chunkPeriod?: number;

        // Undocumented:
        doAggregateTopNMetricFirst?: boolean;

        // Or whatever...
        [key: string]: any;
    }

    // http://druid.io/docs/latest/GeographicQueries.html
    interface SpatialBound {
        type: string;

        // Specific to type: "rectangular"
        minCoords?: number[];
        maxCoords?: number[];

        // Specific to type: "radius"
        coords?: number[];
        radius?: number;
    }

    // http://druid.io/docs/latest/TimeseriesQuery.html
    type Intervals = Array<string>;

    // http://druid.io/docs/latest/Filters.html
    interface Filter {
        type: string;
        dimension?: string;

        // Specific to type: "selector"
        value?: string;

        // Specific to type: "in"
        values?: string[];

        // Specific to type: "between"
        lower?: number | string;
        upper?: number | string;
        numerically?: boolean;

        // Specific to type: "regex"
        pattern?: string;

        // Specific to type: "search"
        query?: SearchQuerySpec;

        // Specific to type: "javascript"
        "function"?: string;

        // Specific to type: "spatial"
        bound?: SpatialBound;

        // Specific to type: "extraction"
        extractionFn?: ExtractionFn;

        // Specific to type: "not"
        field?: Filter;

        // Specific to type: "and" | "or"
        fields?: Filter[];
    }

    // http://druid.io/docs/latest/Aggregations.html
    interface Aggregation {
        type: string;
        name?: string;
        fieldName?: string;

        // Specific to type: "javascript" and "cardinality"
        fieldNames?: string[];

        // Specific to type: "javascript"
        fnAggregate?: string;
        fnCombine?: string;
        fnReset?: string;

        // Specific to type: "filtered"
        filter?: Filter;
        aggregator?: Aggregation;

        // Specific to type: "cardinality"
        byRow?: boolean;

        // Specific to type: "approxHistogramFold"
        resolution?: number;
        numBuckets?: number;
        lowerLimit?: number;
        upperLimit?: number;

        // Also arbitrary keys are supported
        [key: string]: any;
    }

    // http://druid.io/docs/latest/Post-aggregations.html
    interface PostAggregation {
        type: string;
        name?: string;
        fn?: string;
        fields?: PostAggregation[];

        // Specific to type: "fieldAccess"
        fieldName?: string;

        // Specific to type: "constant"
        value?: number;

        // Specific to type: "javascript"
        fieldNames?: string[];
        "function"?: string;

        // Specific to type: "equalBuckets"
        numBuckets?: number;

        // Specific to type: "buckets"
        bucketSize?: number;
        offset?: number;

        // Specific to type: "quantile"
        probability?: number;

        // Specific to type: "quantiles"
        probabilities?: number[];
    }

    // http://druid.io/docs/latest/Granularities.html
    interface Granularity {
        type: string;
        duration?: number; // or string?

        period?: string;
        timeZone?: string;
        origin?: string;
    }

    // http://druid.io/docs/latest/LimitSpec.html
    interface OrderByColumnSpec {
        dimension: string;
        direction: string;
    }
    interface LimitSpec {
        type: string;
        limit: number;
        columns: Array<string|OrderByColumnSpec>;
    }

    // http://druid.io/docs/latest/Having.html
    interface Having {
        type: string;
        aggregation?: string;
        value?: number;

        // Specific to type: "not"
        havingSpec?: Having;

        // Specific to type: "and" | "or"
        havingSpecs?: Having[];
    }

    // http://druid.io/docs/latest/SearchQuerySpec.html
    interface SearchQuerySpec {
        type: string;

        // Specific to type: "insensitive_contains"
        value?: string;

        // Specific to type: "fragment"
        values?: string[];
    }

    // http://druid.io/docs/latest/SegmentMetadataQuery.html
    interface ToInclude {
        type: string;

        // Specific to type: "list"
        columns?: string[];
    }

    // http://druid.io/docs/latest/DimensionSpecs.html
    interface ExtractionLookup {
        type: string;

        // Specific to type: "map"
        map?: { [key: string]: string };

        // Specific to type: "namespace"
        namespace?: string;
    }

    // http://druid.io/docs/latest/DimensionSpecs.html
    interface ExtractionFn {
        type: string;

        // Specific to type: "regex"
        expr?: string;
        replaceMissingValue?: boolean;

        // Specific to type: "searchQuery"
        query?: string;

        // Specific to type: "time"
        timeFormat?: string;
        resultFormat?: string;

        // Specific to type: "javascript"
        "function"?: string;
        injective?: boolean;

        // Specific to type: "substring"
        index?: number;
        length?: number;

        // Specific to type: "timeFormat"
        format?: string;
        timeZone?: string;
        locale?: string;

        // Specific to type: "lookup",
        lookup?: ExtractionLookup;
        retainMissingValue?: boolean;
        replaceMissingValueWith?: string;
    }
    interface DimensionSpec {
        type: string;
        dimension?: string;
        outputName?: string;

        // Specific to type: "extraction"
        extractionFn?: ExtractionFn;
        dimExtractionFn?: ExtractionFn; // This will be deprecated soon
    }

    // http://druid.io/docs/latest/TopNMetricSpec.html
    interface TopNMetricSpec {
        type: string;

        // Specific to type: "numeric" | "inverted"
        metric?: string|TopNMetricSpec;

        // Specific to type: "lexicographic" | "alphaNumeric"
        previousStop?: any;
    }

    // http://druid.io/docs/latest/SelectQuery.html
    interface PagingSpec {
        pagingIdentifiers: any; // ToDo: find better docs for this / ask FJ
        threshold: number
    }

    // http://druid.io/docs/latest/DataSource.html
    interface DataSource {
        type: string;

        // Specific to type: "table"
        name?: string;

        // Specific to type: "union"
        dataSources?: string[];

        // Specific to type: "query"
        query?: Query;
    }

    // http://druid.io/docs/latest/Querying.html
    interface Query {
        queryType: string;
        dataSource: string|DataSource;
        context?: Context;
        intervals?: Intervals;
        filter?: Filter;
        aggregations?: Aggregation[];
        postAggregations?: PostAggregation[];
        granularity?: string|Granularity;

        // Used by queryType: "groupBy" and "select";
        dimensions?: Array<string|DimensionSpec>;

        // Specific to queryType: "groupBy"
        // http://druid.io/docs/latest/GroupByQuery.html
        limitSpec?: LimitSpec;
        having?: Having;

        // Specific to queryType: "search"
        // http://druid.io/docs/latest/SearchQuery.html
        searchDimensions?: string[];
        query?: SearchQuerySpec;
        sort?: string; // ToDo: revisit after clarification

        // Specific to queryType: "segmentMetadata"
        // http://druid.io/docs/latest/SegmentMetadataQuery.html
        toInclude?: ToInclude;
        merge?: boolean;
        analysisTypes?: string[];

        // Specific to queryType: "timeBoundary"
        // http://druid.io/docs/latest/TimeBoundaryQuery.html
        bound?: string;

        // Specific to queryType: "timeseries"
        // http://druid.io/docs/latest/TimeseriesQuery.html
        // <nothing>

        // Specific to queryType: "topN"
        // http://druid.io/docs/latest/TopNQuery.html
        dimension?: string|DimensionSpec;
        threshold?: number;
        metric?: string|TopNMetricSpec;

        // Specific to queryType: "select"
        // http://druid.io/docs/latest/SelectQuery.html
        metrics?: string[];
        pagingSpec?: PagingSpec;
    }

    /* ----------------------- *\
    |          Results          |
    \* ----------------------- */

    // The result of calling http://$host:$port/druid/v2/datasources
    type OverallIntrospectResult = Array<string>;

    // The result of calling http://$host:$port/druid/v2/datasources/$datasource
    interface DatasourceIntrospectResult {
        dimensions: string[];
        metrics: string[];
    }

    // http://druid.io/docs/latest/TimeBoundaryQuery.html
    interface TimeBoundaryDatum {
        timestamp: string;
        result: string | Result; // string in case of useDataSourceMetadata
    }

    type TimeBoundaryResults = Array<TimeBoundaryDatum>;

    // http://druid.io/docs/latest/TopNQuery.html
    // http://druid.io/docs/latest/SearchQuery.html
    interface Result {
        [field: string]: string|number;
    }

    interface DruidDatum {
        timestamp: string;
        result: Array<Result>;
    }

    type DruidResults = Array<DruidDatum>;

    // http://druid.io/docs/latest/TimeseriesQuery.html
    interface TimeseriesDatum {
        timestamp: string;
        result: Result;
    }

    type TimeseriesResults = Array<TimeseriesDatum>;

    // http://druid.io/docs/latest/GroupByQuery.html
    interface GroupByDatum {
        version: string;
        timestamp: string;
        event: Result;
    }

    type GroupByResults = Array<GroupByDatum>;

    // http://druid.io/docs/latest/SegmentMetadataQuery.html
    interface ColumnMetadata {
        type: string;
        size?: number;
        cardinality?: number;
        hasMultipleValues?: boolean;
        errorMessage?: string;
    }

    interface SegmentMetadataDatum {
        id: string;
        intervals: Intervals;
        size: number;
        numRows?: number;
        columns: { [columnName: string]: ColumnMetadata };
        aggregators?: { [columnName: string]: Aggregation };
    }

    type SegmentMetadataResults = Array<SegmentMetadataDatum>;

    // http://druid.io/docs/latest/SelectQuery.html
    interface Event {
        segmentId: string;
        offset: number;
        event: Result;
    }

    interface PagingIdentifiers {
        [segment: string]: number;
    }

    interface SelectResult {
        pagingIdentifiers: PagingIdentifiers;
        events: Event[];
    }

    interface SelectDatum {
        timestamp: string;
        result: SelectResult;
    }

    type SelectResults = Array<SelectDatum>;
}
