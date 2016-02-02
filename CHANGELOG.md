# Change log

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
