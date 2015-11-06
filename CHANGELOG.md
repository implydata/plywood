# 0.7.9

- Multi-dim group dy uses granularity when it can. 

# 0.7.8

- fixed bug in `Set#add` 

# 0.7.7

- added tonic example: https://tonicdev.com/npm/plywood

# 0.7.4

- refactored DruidExternal
- DruidExternal now supports multi-dim splits
- timezone (2nd argument) in `timeBucket` is now optional, defaults to UTC
- fixed native `dataset.max`

# 0.7.3

- better docs
- context is now added passed along in a DruidExternal

# 0.7.2

- added `Expression.concat`
- added `CONCAT` to the PlyQL parser

# 0.7.1

- Paving the road for multi-dimensional splits `.split({ Page: '$page', User: '$user' })`
- Fixed problems with SELECT queries
- Allow for SQL parsing of `SELECT *`
- Fix support for `sort` and `limit` in SELECT queries
- Better escaping in MySQL driver

# 0.6.3

- Changed JS fallbacks in DruidExternal to use native code generators and removed some null bugs. 

# 0.6.2

- Added more rules to the SQL parser: `IS`, `LIKE`, and awareness of `UPDATE`, `SET`, e.t.c
 
# 0.6.1

- Changed the return type of `Expression.parseSQL` to return and object with the keys `verb`, `expression`, and `table`
  `Expression.parseSQL(blah)` ==> `Expression.parseSQL(blah).expression`
- In PlyQL changed the meaning of `GROUP BY <number>` to be a reference to a column (just like in MySQL and Postgres)

# 0.5.2

- Added support for case sensitive (`normal`) / case insensitive (`ignoreCase`) contains.
- Improved concat action support in Druid

# 0.5

- Chain expression `.toJS()` now returns `action` instead of `actions` array if there is only one action.
  Both are still valid to parse.
- Misc bug fixes
- Added SortAction DESCENDING, ASCENDING, toggleDirection
- DruidExternal guards against duplicate aggs and postAggs

# 0.4

- Changed attribute definitions to be an array instead of an object and added deprecation message. 
