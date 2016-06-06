# Change Log

For updates follow [@implydata](https://twitter.com/implydata) on Twitter.

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
