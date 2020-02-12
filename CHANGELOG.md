# Change Log

## 0.21.3

- Better handle `undefined` in druid groupBys

## 0.21.2

- Add `rebaseOnStart` to `Range`

## 0.21.1

- Make time joiner more robust

## 0.21.0

- Bumping dependencies

## 0.20.13

- Adjusted more types, to make plywood friendlier with strict nulls

## 0.20.12

- Adjusted types

## 0.20.11

- Move force finalize into expression options

## 0.20.10

- Add get and set options

## 0.20.9

- Allow options on expressions
- Fix version checks on `null` in Druid expressions

## 0.20.8

- Use expressions over JS aggs when possible
- Support more datasketches

## 0.20.7

- Handle large overrides better, fix `O(n^2)` issue 

## 0.20.6

- More robust `toDate` does not crash if bogus date is introspected in DruidExternal

## 0.20.5

- Added `forceFinalize` to fix DAU calculation

## 0.20.4

- Fix clipping of badly rounded doubles

## 0.20.3

- Fix bug when planning complex fallback
- Allow multiple aggregations in custom agg

## 0.20.2

- Better sql escaping

## 0.20.1

- Fix Druid filtering on alternative time columns represented as LONG

## 0.20.0

- Updated dependencies


## 0.19.15

- Fix escaping in Druid `like`

## 0.19.14

- Send less javascript to Druid

## 0.19.13

- Fix typo in `Dataset#fullJoin`

## 0.19.12

(publish error)

## 0.19.11

- Allow re-splitting complex expressions

## 0.19.10

- Fix totals calculation for resplit measures

## 0.19.9

- Support Druid timeseries sorted descending
- Fixed Druid planning bug where contains case insensitivity preference would be swallowed up when LHS is a concatenation over a single dimension
- Fixed Druid planning bug with concatenating a dimension to itself

## 0.19.8

- Fix `in` filtering on fancy lookup expressions

## 0.19.7

- Fix previous NPE for real

## 0.19.6

- Fix NPE

## 0.19.5

- Support contains on Druid expressions

## 0.19.4

- Changed how concat is calculated in Druid to me more efficient

## 0.19.3

- Shortcut to decompose certain compare type queries

## 0.19.2

- Handle concat with different expressions

## 0.19.1

- Handle empty string sources

## 0.19.0

- Update dependencies

## 0.18.11

- Use expressions in fallback when extraction functions fail

## 0.18.10

- Correct time bucket inflation in case of string dates (as returned by topN queries)

## 0.18.9

- Move some Dataset methods into statics

## 0.18.8

- Nothing. Npm is not replicating so it is a crap shoot to install 0.18.7 hence bumping version and publishing again to hopefully kickstart it into action.

## 0.18.7

- avoid topN for filtered on time aggregate sort

## 0.18.6

- allow multiple resplit measures

## 0.18.5

- basic resplit ability

## 0.18.4

- use `usePrefixForLastTerm`

## 0.18.3

- Remove aggressive fallback simplification

## 0.18.2

- Added `termsDelegate` and fullText search prototype
- fix error where virtual columns can refer to themselves
- added number bucket expression support

## 0.18.1

- Control query concurrency

## 0.18.0

- Removing support for Druid < 0.10.0, cleaning up code

## 0.17.44

- Support for MatchExpression in Druid expression builder

## 0.17.43

- Better error messages for unsupported Druid expressions in older (< 0.11.0) Druid
- Fixed legacy cross functional test

## 0.17.42

- Use expressions to handle cases where an extraction function would have been used instead

## 0.17.41

- Changed Druid expression division to work how the post aggregators used to work (return 0 when dividing by 0)

## 0.17.40

- Add `.ln()` to expression parser
- Fix Druid concat to null out on empty strings

## 0.17.39

- Use expressions in post aggs
- Added `LogExpression` (`.log()`)

## 0.17.38

- Fix bug where CSV/TSV headers were not being escaped
- Added `attributeTitle` to TabulatorOptions
- Added `attributeFilter` to TabulatorOptions

## 0.17.37

- Support complex boolean math expressions for Druid

## 0.17.36

- Correctly gate `scan` query on 0.11.0

## 0.17.35

- More forgiving deserialization of sets

## 0.17.34

- Use `scan` query instead of `select`

## 0.17.33

- Adding more tests
- Fix null handling in IN filters for SQL drivers

## 0.17.32

- Correctly handle IN filters on virtualColumns

## 0.17.31

- Also working in filter

## 0.17.30

- Started virtualColumn implementation

## 0.17.29

- `Expression#defineEnvironment` works recursively

## 0.17.28

- Better stringification of strings in expressions

## 0.17.27

- Change the External#introspect method to take a `depth: "deep" | "default" | "shallow"` parameter instead of `deep: boolean` (`deep: true` => `depth: deep`)
- Druid introspect route will query time range on `depth: "default"` and not query it on `depth: "shallow"`

## 0.17.26

- Fix `Set#isAtomicType` to work correctly for `NULL`

## 0.17.25

- Respect filtered aggregations for quantile and custom aggregates

## 0.17.24

- Allow `maxRows` and support dataset trimming

## 0.17.23

- Now setting `outputType` in Druid dimension specs

## 0.17.22

- Updated tests for Druid 0.11.0
- Added and enabled previously skipped tests
- Fixed bug where `Set(null)` and `Set("null")` would be subsets of each other

## 0.17.21

- Added `round: true` to cardinality

## 0.17.20

- Fixed issue with milti-value list filter

## 0.17.19

- Fixed planning for nested JS postagg
- Fixed tests

## 0.17.18

- Updated dependencies
- `TimeRangeExpression.getQualifiedDurationDescription` now accepts a capitalization flag
- Testing with newer Druid
- Support Druid `DOUBLE` types

## 0.17.17

- Allow deep introspection
- Move away from any-promise towards native promises

## 0.17.16

- Fix bug in several PlyQL functions where string inputs were interpreted as plywood expressions leading to errors on values with spaces

## 0.17.15

- Fix bug in `LOCATE`

## 0.17.14

- Fix bug in `.cast("NUMBER")`
- Change tests to work with node8
- Updated examples

## 0.17.13

- Add engine property to `rawQueries`

## 0.17.12

- Allow `rawQueries` collection for externals

## 0.17.11

- Fix bug in formula parsing related to tuning
- Added tuning support to PlyQL

## 0.17.10

- Add support for a `tuning` config for Druid approximate histograms
- Add support for a finalizing postAgg for a custom aggregation
- Replaced TS `Lookup<T>` type with the TS2.2 native `Record<string, T>`

## 0.17.9

- Fix typos
- Add `AttributeInfo#changeType`
- Fix to DSV in dataset for NULL type columns

## 0.17.8

- Changed the meaning of IN to be a subset only operation with back compat provisions.
- Better build process for people with older npm
- `keys` not emitted if empty for `Dataset#toJS`
- `Set#simplify` => `Set#simplifyCover`
- `Set#generalUnion` => `Set#unionCover`
- `Set#generalIntersect` => `Set#intersectCover`
- Fixed SQL externals for filtered `Min` and `Max` aggregations
- Fixed an instance of unhanded error in pipe
- Simplification for case independent contains
- Allow `table_alias.*` in PlyQL
- Support new features added on Druid 0.10.0
- Allow approxHistogram calculation on numeric columns
- verboseRequesterFactory parameter `preQuery` renamed to `onQuery`
- verboseRequesterFactory callback parameters now get a single argument with lots of info
- verboseRequesterFactory will now name queries


## 0.16.5

- PlyQL: support for `IF()` `CASE WHEN THEN END`, and `NULLIF()`

## 0.16.4

- `DruidExternal` fixed some problems with JS code generation
- `DruidExternal` support for planning `.then().fallback()`

## 0.16.3

- `DruidExternal` GroupBys will not correctly use 'numeric' order
- `DruidExternal` fixed GroupBy output name

## 0.16.2

- Stable release

## 0.16.1

**Major breaking changes in this release**

- Druid: Allow count distinct on cross product (e.g. `$data.countDistinct($a ++ $b)`)
- Transition to TS 2.1
- Better division by 0 handling
- Extracted Requester type definition into `plywood-base-api`
- Switched from `Q` to `any-promise`
- Use has-own-prop library
- Changed `Dataset#toJS` to return an object rather than just the `data` array
- Datasets now actively maintain their `keys` array
- `Dataset#flatten` now returns a new (flat) `Dataset` and thus the `parentName` option is no longer supported
- In `Dataset#flatten`, `Dataset#ToCSV`, e.t.c `orderedColumns` option is no longer supported use `.select()` instead
- Added `columnOrdering` option to `Dataset#flatten` with values `'as-seen'` and  `'keys-first'`
- `Dataset#getColumns` is now just `return this.flatten(options).attributes`
- `Dataset#getNestedColumns` was removed
- Added `ThenExpression`
- Expressions have all been standardized to apply to Sets as well as atomics
- Fix `NULL` types handling everywhere
- Remove `AttributeInfo#serialize`
- Removed `AttributeInfo#special`, `UniqueAttributeInfo`, `ThetaAttributeInfo`, and `HistogramAttributeInfo`
- Added `AttributeInfo.nativeType` that stores the original database type of the attribute
- `DruidExernal` will now be able to plan using `longSum`
- `DruidExernal` added ability to split on constant
- Removed deprecation warnings and deleted crutches from the 0.15 release
- `MinExpression` and `MaxExpression` will now correctly output their type as `TIME` if the argument is `TIME`
- Experimental support for DruidSQL
- `DruidExternal` correctly defining numeric ordering in topN metricsSpecs
- `DruidExternal` will now explicitly set `fromNext: false` when paginating select


## 0.15.13

- Fixed bug where bucketing in a multi-value dimension was off for non UTC timezones

## 0.15.12

- Version check now works with all parts of the version

## 0.15.11

- Fix error in version check that had trouble with versions like `0.10.0`

## 0.15.10

- PlyQL: Support for `SHOW CHARACTER SET`
- PlyQL: Support for `SHOW COLLATION`
- PlyQL: Tolerance for encoding defined strings `N'Hello World'` and `_utf8'Hello World'`

## 0.15.7

- Added timezone based formatting ability

## 0.15.6

- Better publish script

## 0.15.5

- Fixing JS code gen for number casting `+x !== parseFloat(x)` when `x == " "`
- Implement `bucket` extractionFn for DruidExternal

## 0.15.4

- Added `Range#isFinite`
- Fixed logic around heatmap query

## 0.15.3

- Added more CAST options

## 0.15.2

- Fixed special attributes not reporting `unsplitable` correctly
- Fixed numberInflater sometimes returning `NaN`

## 0.15.1

- All Dataset computation and aggregation functions now take expressions instead of functions.
  Each has a corresponding function that takes a function like the old API.
  So `Dataset#apply(name, x)` now expects `x` to be and expression but `Dataset#applyFn(name, x)` has the old API
- `AttributeInfo#type` defaults to `STRING`
- Removed PlyQL parser from plywood-lite


## 0.14.12

- Druid countDistinct planing no longer does `byRow: true` to be more inline with how plywood aggregates work in general
- Fix countDistinct to be able to accept all expression types
- Default step in timeShift and timeRange maker functions

## 0.14.11

- Added a plywood-light version with no externals or requester utils

## 0.14.10

- Run tests against Druid groupBy v2 + fix tests

## 0.14.9

- Fixed time resolution in `derivedAttributes`

## 0.14.8

- Better handling for derivedAttributes by type checking them in the external constructor

## 0.14.7

- Fixed `rawAttributes` when empty
- made `Expression#distribute` work for nested expression

## 0.14.6

- removed deprecated uses of `isInstanceOf`

## 0.14.5

- 'action' expressions no longer return `{ op: "ref", name: "_" }` in their toJS further improving API back compat.

## 0.14.4

- export `CustomDruidAggregations` and `CustomDruidTransforms` types

## 0.14.3

- better API backwards compatibility

## 0.14.2

- Typo

## 0.14.1

- Totally reworked internal representation all BlahAction classes are now BlahExpression
- removed `.custom` use `.customAggregate`
- changed `lookup` to `lookupFn`
- changed `LimitAction#limit` to `LimitExpression#value`
- changed `QuantileAction#quantile` to `QuantileExpression#value`
- Fixed NOT() in Druid having clause
- removed `Expression#actionize`
- removed `Expression#firstAction`
- removed `Expression#lastAction`
- removed `Expression#headActions`
- removed `Expression#popAction` use `.operand` instead


## 0.13.7

- Sets do not auto-unify elements
- Limit action works with Infinity

## 0.13.6

- Update deps

## 0.13.5

- Collect works for all types

## 0.13.4

- Fixed public / private typings

## 0.13.3

- Smart query limiting
- Support for `show status`
- Fixed BETWEEN bounds

## 0.13.2

- Added collect action
- No more context folding

## 0.13.1

- More efficient query parallelisation
- Misc cleanup

## 0.12.12

- Better type injection with TDI

## 0.12.11

- Quarter supported

## 0.12.10

- Allow bug fixes in deps

## 0.12.9

- Transition to better build system

## 0.12.8

- Support Druid `regexFiltered` and `listFiltered` dim specs

## 0.12.7

- Respect PlyQL column ordering by generating extra select statements

## 0.12.6

- SegmentMetadata passes context also
- Upgraded to latest PEG.js
- Fixed bug in OR action
- Support case insensitive RefExpression
- PlyQL `SHOW` query rewriting is case insensitive
- Moved methods out of `plywood` and into `immutable-class`:
  * `plywood.find` to `SimpleArray.find`
  * `plywood.findIndex` to `SimpleArray.findIndex`
  * `plywood.findByName` to `NamedArray.findByName`
  * `plywood.findIndexByName` to `NamedArray.findIndexByName`
  * `plywood.overrideByName` to `NamedArray.overrideByName`
  * `plywood.overridesByName` to `NamedArray.overridesByName`

## 0.12.5

- Fixes in `AttributeInfo#valueOf`

## 0.12.4

- Added `AttributeInfo#change`

## 0.12.3

- Transitioned to newer immutable tester

## 0.12.2

- Renamed `customExtractionFns` to `customTransforms`
- Using lowercase `alphanumeric` in `dimensionOrder`
- Renamed `customAction` to `customAggregationAction`

## 0.12.1

- Changed to build to use external modules
- Moved methods out of `plywood.helper`, specifically:
  * `helper.parseJSON` to `Dataset.parseJSON`
  * `helper.expressionLookupFromJS` to `Expression.expressionLookupFromJS`
  * `helper.expressionLookupToJS` to `Expression.expressionLookupToJS`
  * `helper.find` to `plywood.find`
  * `helper.findIndex` to `plywood.findIndex`
  * `helper.findByName` to `plywood.findByName`
  * `helper.findIndexByName` to `plywood.findIndexByName`
  * `helper.overrideByName` to `plywood.overrideByName`
  * `helper.overridesByName` to `plywood.overridesByName`
  * `helper.shallowCopy` to `plywood.shallowCopy`
  * `helper.deduplicateSort` to `plywood.deduplicateSort`
  * `helper.mapLookup` to `plywood.mapLookup`
  * `helper.emptyLookup` to `plywood.emptyLookup`
  * `helper.nonEmptyLookup` to `plywood.nonEmptyLookup`
  * `helper.verboseRequesterFactory` to `plywood.verboseRequesterFactory`
  * `helper.retryRequesterFactory` to `plywood.retryRequesterFactory`
  * `helper.concurrentLimitRequesterFactory` to `plywood.concurrentLimitRequesterFactory`
  * `helper.promiseWhile` to `plywood.promiseWhile`
- Moved `simpleLocator` out
- Removed `retryRequester` which was deprecated

## 0.11.9

- Expressions can parse pure JSON also

## 0.11.8

- Updated file notice headers
- Updated dependencies

## 0.11.7

- Added `STRING_RANGE` type
- Core: Support for `.indexOf()`, `.transformCase()` action
- PlyQL: Support for `LOCATE`, `UCASE`, `LCASE`

## 0.11.6

- No JS globals in JS functions

## 0.11.5

- Core: Support for `.cast()` action
- PlyQL: Support for `CAST`, `FROM_UNIXTIME`, `UNIX_TIMESTAMP` actions
- DruidExternal: fix for `.cast('number').numberBucket()`

## 0.11.4

- Cleaned up npm package and updated dependencies

## 0.11.3

- Externals take account of `requester` for comparisons
- Misc cleanup

## 0.11.2

- Made `Range` and abstract class
- PlyQL: Fixed `AS` for tables, also `"AS"` keyword is now optional (like in MySQL)
- Added `.cardinality()` action
- Added `CARDINALITY` to PlyQL

## 0.11.1

- Takes advantage of most features in Druid 0.9.1
- Removed untested RangeAttributeInfo with helpful error message to use numbers instead
- DruidExternal: added `-legacy-lookups` version flag
- Trying to secondary filter on `__time` in Druid <= 0.9.1 now throws an error (at planning stage) as it is impossible
- Added `.length()` action
- Added `LENGTH`, `LEN`, and `CHAR_LENGTH` to PlyQL

## 0.10.33

- DruidExternal: Segment metadata works on union dataSource
- Using abstract classes (forces dependency on TypeScript >= 1.6)

## 0.10.32

- Rename `dataSource` and `table` to `source`
- Added `External.getExternalFor`

## 0.10.31

- Fix bug where JS reserved words could not be used in aggregates

## 0.10.30

- Compute expression HEAD also

## 0.10.29

- PlyQL: From(sub query) works
- Native quantiles also work

## 0.10.28

- Added compare to ranges so they sort correctly

## 0.10.27

- DruidExternal: Fix bug where `$blah / 10` did not work in split
- DruidExternal: Fix bug where `1 + $blah` did not cast to number
- DruidExternal: Pure `.fallback()` now works
- Removed some dead code

## 0.10.26

- DruidExternal: Ability to split on a secondary time dimension by `PT2H` e.t.c.
- PlyQL: `SHOW FULL TABLES` rewrites correctly

## 0.10.23

- Druid Select query no longer bound to 10000 max limit and will use iterative strategy
- Numeric Dimensions can be aggregated

## 0.10.22

- Better simplification of `.is(TimeRange)` and `.is(NumberRange)`
- Removed some dead code

## 0.10.21

- Druid: Default to JavaScript filter for negative bounds in bound filter
- Druid Fixed number range behaviour in nested splits
- Add tests around negative numbers

## 0.10.20

- Added PostgresExternal
- Refactored common SQL External functionality into SQLExternal base class
- Fixed timezone bucketing in MySQL External
- Support countDistinct on Theta sketches
- Druid GroupBy will no longer receive arbitrary limit

## 0.10.19

- Made filters on non primary time dimensions work in DruidExternal
- Added basic tests for legacy Druid versions

## 0.10.18

- `timeFloor` and `timeBucket` actions make sure duration is floorable

## 0.10.17

- `helper.overrideByName` maintains original order
- DruidExternal explicitly sends UTC timestamps

## 0.10.16

- Ability to define delegates in Externals
- selectAction now type checks and only allows defined attributes to be selected
- `DruidExternal.getSourceList` now returns the sources in a sorted order
- Adding range extent

## 0.10.15

- New chronoshift fixes bug with flooring `PT12H`

## 0.10.14

- Fixed parsing of `NULL` in set expressions
- Added information function to PlyQL

## 0.10.13

- Allow PlyQL to support `SHOW SESSION VARIABLES WHERE ...` rewriting

## 0.10.12

- Fix distribution of `SUM(1) => COUNT()`
- Added `DruidExternal.getVersion`
- Fixed post computation tasks
- Added support for `USE` on PlyQL
- PlyQL allows `NULL` in set literal e.g. `{'a', 'b', NULL}`

## 0.10.11

- PlyQL describe query now rewrites

## 0.10.10

- Fixed problem with DruidExternal having filter generating an OR instead of an AND for time ranges

## 0.10.9

- Fixed problem with filtering on multi-dimensional dimensions with a lookup
- Added `Dataset#findDatumByAttribute`

## 0.10.8

- Fixed bug where flatten would fail on an empty nested dataset.

## 0.10.7

- Accidentally published again.

## 0.10.6

- Added `finalizer` option to `Dataset#toTabular`
- Fixed quoting when exporting to CSV or TSV

## 0.10.5

- Fixed DruidExternal sometimes generating an invalid Druid query when doing a multi-dimensional split that includes time
- Unsuppress datasource when it is limited or selected
- Better show query support

## 0.10.4

- Made Dataset#apply and Dataset#select truly immutable
- Fix Druid introspection bug on JS ingestion aggregators
- Allow data-less PlyQL queries like `SELECT 1+1`
- Default PlyQL `AS` text now matches SQL implementation.

## 0.10.3

- Removed duplicate entry that killed in strict mode.

## 0.10.2

- Added ability to do ISO8601 in PlyQL time literals
- Ability to sort in select queries
- Fix `timeFloor` now work with limit

## 0.10.1

- Fixed bug where select queries including multi value dimensions would sometimes error out in DruidExternal
- Changed `helper.overrideByName` to `helper.overridesByName`, `helper.overrideByName` now does a single override
- Added `finalLineBreak` option to `Dataset#toTabular`, `Dataset#toCSV`, `Dataset#toTSV` (default to 'include' for tabular and 'suppress' for toCSV and toTSV)
- `Dataset` now always has introspected dimensions
- Support for zero Intervals in PlyQL
- Support for `DATE_FORMAT` in PlyQL as used in MySQL for `TIME_FLOOR`


## 0.9.25

- Dramatically reduced PlyQL parser size
- Added `YEAR` as a possible `timePart` value
- Added `CURDATE`, `PI`, `YEAR`, `MONTH`, `WEEK_OF_YEAR`, `DAY_OF_YEAR`, `DAY_OF_MONTH`,
  `DAY_OF_WEEK`, `HOUR`, `MINUTE`, `SECOND`, `DATE`, `CURRENT_TIMESTAMP`, `LOCALTIME`,
  `LOCALTIMESTAMP`, `UTC_TIMESTAMP`, `SYSDATE`, `CURRENT_DATE`, `UTC_DATE`, `DAY_OF_YEAR`,
  `DOY`, `DOW`, `DAYOFMONTH`, `DAY`, `WEEKOFYEAR`, and `WEEK` to PlyQL
- Fixed bug where `DAY_OF_YEAR`, `DAY_OF_MONTH`, and `DAY_OF_WEEK` was zero indexed
- Added support for `timeFloor` split in Druid external

## 0.9.24

- Added SET expressions to PlyQL `{'A', 'B', 'C'}`
- PlyQL can now parse IN with arbitrary expression on right hand side

## 0.9.23

- Fix .match() not working on SET/STRING

## 0.9.22

- Moved date parsing to Chronoshift and updated to latest Chronoshift
- Parser can now configure a timezone within which it will parse 'local' date strings
- Optimized post processing by not calculating the filter for final splits

## 0.9.21

- Added ability to define the timezone through the environment
- Timezone always defaults to UTC
- Date parsing no longer done by PlyQL parser
- Added tests for legacy Druid versions

## 0.9.20

- Added convenient filtered aggregators to PlyQL like: `SUM(added WHERE cityName = 'San Francisco')`
- Fixed PlyQL incorrectly parsing `SELECT COUNT(page)`

## 0.9.19

- Fixed null handling in extractionFns for Druid 0.8.3

## 0.9.18

- Fixed overlap on `[null]`
- Enabled all Druid 0.9.0 tests (testing using Druid 0.9.0-iap1)

## 0.9.17

- DruidExternal now supports lookups on SET/STRING dimensions

## 0.9.16

- Fixed bug where DruidExternal did not work with filtered cardinality aggregator

## 0.9.15

- New immutable class methods

## 0.9.14

- Allow derivedAttributes to be specified in ExternalJS
- derivedAttributes can now be used in filters and splits
- Fixed bugs in type inference
- Fixed `.substr()` getJS to work correctly with nulls

## 0.9.13

- Added (experimental) rollup mode to all Externals, only implemented in DruidExternal
- Derived attributes now work for DruidExternal

## 0.9.12

- DruidExternal now supports boolean ref filters like: `$wiki.filter($isAnonymous)`

## 0.9.11

- added `getSourceList` to externals
- Fixed bug in expression simplification where `$s.contains('A') and $x.is('B') => false`
- Respect unsplitable measures in DruidExternal

## 0.9.10

- DruidExternal timeAttribute now defaults to `__time`
- Moved find* methods into helper
- Externals can re-introspect

## 0.9.9

- Fixed Druid 'select' failing on time interval outside of Druid cluster

## 0.9.8

- Using Imply Analytics Platform 1.1.1 docker image for Druid tests
- Derived columns in Druid select will now trigger if version is `/^0.9.0-iap/`

## 0.9.7

- `version` is now a parameter on all External
- In Druid external `druidVersion` was renamed to `version`
- Fixed MySQL translation of .is() action to use null safe equals (<=>)
- Fixed MySQL translation of .contains()
- Added `<=>` to PlyQL
- Druid external now introspects version and does not make broken query

## 0.9.6

- Support for new features in Druid 0.9.0
- Fix for Druid "bound" / "between" filter
- Much faster PlyQL parsing

## 0.9.5

- The FROM clause in PlyQL now accepts relaxed table names allowing for: `SELECT * FROM my-table*is:the/best_table`

## 0.9.4

- Set `skipEmptyBuckets: "true"` in timeseries queries to get the Plywood expected behaviour

## 0.9.3

- Added `.select('attr1', 'attr2', 'attr3')` action allowing `SELECT attr1, attr2, attr3 FROM ...` to be expressed in PlyQL
- Added support for `.quantile(p)` aggregator in DruidExternal (using [approximateHistograms](http://druid.io/docs/latest/development/approximate-histograms.html))
- Better support for filtered aggregates in DruidExternal

## 0.9.2

- Custom type guards for all the modal classes

## 0.9.1

- New 'value' mode in externals. `$wikipedia.sum($added)` is now computable.
- More relaxed date literal parsing in PlyQL


## 0.8.21

- Fix native `overlap` calculation

## 0.8.20

- Support Date and Timestamp literal as described here: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html

## 0.8.19

- Upgraded to TypeScript 1.8

## 0.8.18

- SELECT queries in Druid are now mindful of the attributes and inflate values correctly.

## 0.8.17

- All cross functional tests now running with rollup.
- Temporary columns are no longer being returned by Druid.

## 0.8.16

- Fixed issue with `.in(...).not()` in the DruidExternal

## 0.8.15

- `overlap` now allows STRING arguments, simplifies to `in`

## 0.8.14

- DruidExternal will use topNs for defined small splits even when there is no limit set (like `BOOLEAN`, `timeBucket(HOUR_OF_DAY)`)
- Fixed bug for MySQLExternal not dealing well with BOOLEAN splits

## 0.8.13

- Added `overlap` action
- Added multi-database query test / example

## 0.8.12

- Fixed sorting direction when sorting on a split in Druid
- Ability to do filtered aggregations on SQL drivers
- `== NULL` works correctly with SQL drivers

## 0.8.11

- MySQL functional tests can now run in docker
- Better checks for expression types in aggregates

## 0.8.10

- Better handling for filtered attributes now allows filtered attributes on `countDistinct` and `custom`
- Fixed sorting for numeric columns in DruidExternal

## 0.8.9

- Add `absolute`, `power`, `sqrt`, `fallback` action
- Corresponding `ABS`, `POW/POWER`, `EXP`, `SQRT`, and `IFNULL/FALLBACK` to PlyQL
- Fixed type checking in `InAction`

## 0.8.8

- Fixed `match` on nulls to return `null`

## 0.8.7

- Support `SELECT DISTINCT` syntax in PlyQL

## 0.8.6

- Add `timeRange` action (`TIME_RANGE` in PlyQL) - not supported by DruidExternal yet

## 0.8.5

- Add `timeFloor` action (`TIME_FLOOR` in PlyQL) - not supported by DruidExternal yet
- Rename unused `offsetTime` action to `timeShift` (`TIME_SHIFT` in PlyQL) and add `step` parameter - not supported by DruidExternal yet
- Fixed how `concat` works on nulls to return null
- Added `.isnt` and `.negate` to expression parsing grammar (they were mistakenly excluded)
- Added `MATCH` function to PlyQL
- Timezone parameter is now optional in all the `time*` functions in Plywood and PlyQL
- Added `NOW()` to PlyQL
- Better DruidExternal column introspection error detection

## 0.8.4

- Allow parsing of `DESCRIBE` verb for PlyQL

## 0.8.3

- Changed `avoidSegmentMetadata` to `introspectionStrategy: 'datasource-get'` defaults to `'segment-metadata-fallback'`

## 0.8.2

- Brand new introspection code for `druidExternal` no more `useSegmentMetadata` flag instead there is a `avoidSegmentMetadata` flag
- Fix `.lookup()` stringification
- PlyQL comments are treated like they are in MySQL: `--` needs to be followed by a space, `#` starts a comment,
  and `/* ... */` inline and multiline comments are supported

## 0.8.1

- Removing d3 dependency and minimizing code size


## 0.7.30

- Fixed interval conversion to add a millisecond and not a second
- Allow for filtering on exact date in DruidExternal
- Fixed missing error message when invalid interval is given

## 0.7.29

- In DruidExternal a time attribute will no longer collide with an existing attribute

## 0.7.28

- Added ability to write `$('time').in('2015-03-03Z', '2015-10-10Z')`
- Added ability to parse `$time.in('2015-03-03Z', '2015-10-10Z')`
- Fixed bug in `$number.in(1, 2)` syntax

## 0.7.27

- Native `timePart` now works

## 0.7.26

- Updated Druid External to use `doubleMin` and `doubleMax`.

## 0.7.25

- Fixed bug in sorting with `null` in the list
- Updated to latest typescript (1.7.5)

## 0.7.24

- Added the `.lookup('my_lookup')` expression that translates to a query time lookup in Druid
- Added the corresponding `LOOKUP` function to PlyQL
- Fixed split simplification

## 0.7.23

- In PlyQL `COUNT(blah)` returns the count where blah is not `null` in accordance with SQL standard.
- Added query IDs to `verboseRequester`
- Flashed out PlyQL docs
- Fixed bug in parameter parsing with leading numbers

## 0.7.22

- Added `.extract()` function
- Added `EXTRACT` function to PlyQL
- Fixed bug in PlyQL where a space before the trailing ) would cause a parse error
- More docs :-)

## 0.7.21

- DruidExternal supports extractionFn filters such as `.filter($x.substr(0, 1) === 'D')`
- added `druidVersion` to DruidExternal to support feature differentiation
- added native Druid substring filter support or druidVersion >= 0.9.0

## 0.7.20

- Added some docs
- Created [plywood-proxy](https://github.com/implydata/plywood-proxy)
- PlyQL `LIKE` now works to SQL spec (added `ESCAPE` clause)
- Added `CONTAINS` clause to PlyQL
- Added `REGEXP` clause to PlyQL

## 0.7.19

- Added `firstAction` and `lastAction` to all expressions
- `popAction` no longer takes an optional action type (warning added)

## 0.7.18

- DruidExternal now works with upper/lower time bound only

## 0.7.17

- Fixed string handling in parser arguments
- Added `contains` to expression parsing grammar

## 0.7.16

- Added `timePart` to MySQL dialect
- Added time part option for `MONTH_OF_YEAR`

## 0.7.15

- Fixed bug to ensure `Dataset#flatten` works with empty datasets

## 0.7.14

- Added `Set#toggle`

## 0.7.13

- Added simplification rule for `.sort(X).filter(Y) => .filter(Y).sort(X)`
- Added simplification rule for `.sort(X).sort(X) => .sort(X)`

## 0.7.12

- Added simplification rule for `.not().not() => nothing`

## 0.7.11

- Added `External#updateAttribute`

## 0.7.10

- Fixed `Dataset#average`
- Latest Chronoshift allows for PT15M bucketing, e.t.c.

## 0.7.9

- Better apply, filter, limit sorting
- Multi-dim group dy uses granularity when it can.

## 0.7.8

- Fixed bug in `Set#add`

## 0.7.7

- added tonic example: https://tonicdev.com/npm/plywood

## 0.7.4

- Refactored DruidExternal
- DruidExternal now supports multi-dim splits
- Timezone (2nd argument) in `timeBucket` is now optional, defaults to UTC
- Fixed native `dataset.max`

## 0.7.3

- Better docs
- Context is now added passed along in a DruidExternal

## 0.7.2

- Added `Expression.concat`
- Added `CONCAT` to the PlyQL parser

## 0.7.1

- Paving the road for multi-dimensional splits `.split({ Page: '$page', User: '$user' })`
- Fixed problems with SELECT queries
- Allow for SQL parsing of `SELECT *`
- Fix support for `sort` and `limit` in SELECT queries
- Better escaping in MySQL driver


## 0.6.3

- Changed JS fallbacks in DruidExternal to use native code generators and removed some null bugs.

## 0.6.2

- Added more rules to the SQL parser: `IS`, `LIKE`, and awareness of `UPDATE`, `SET`, e.t.c

## 0.6.1

- Changed the return type of `Expression.parseSQL` to return and object with the keys `verb`, `expression`, and `table`
  `Expression.parseSQL(blah)` ==> `Expression.parseSQL(blah).expression`
- In PlyQL changed the meaning of `GROUP BY <number>` to be a reference to a column (just like in MySQL and Postgres)


## 0.5.2

- Added support for case sensitive (`normal`) / case insensitive (`ignoreCase`) contains.
- Improved concat action support in Druid


## 0.5

- Chain expression `.toJS()` now returns `action` instead of `actions` array if there is only one action.
  Both are still valid to parse.
- Misc bug fixes
- Added SortAction DESCENDING, ASCENDING, toggleDirection
- DruidExternal guards against duplicate aggs and postAggs


## 0.4

- Changed attribute definitions to be an array instead of an object and added deprecation message.
