# 0.4

- Changed attribute definitions to be an array instead of an object and added deprecation message. 

# 0.5

- Chain expression `.toJS()` now returns `action` instead of `actions` array if there is only one action.
  Both are still valid to parse.
- Misc bug fixes
- Added SortAction DESCENDING, ASCENDING, toggleDirection
- DruidExternal guards against duplicate aggs and postAggs

# 0.5.2

- Added support for case sensitive (`normal`) / case insensitive (`ignoreCase`) contains.
- Improved concat action support in Druid
