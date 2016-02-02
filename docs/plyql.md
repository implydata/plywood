# PlyQL

PlyQL is a SQL-like language that can be parsed into a plywood expression and then executed.

PlyQL only supports `SELECT` queries as plywood's only focus is getting data out of the data store.

## Example

Here is an example of a simple PlyQL query:

```sql
SELECT
  repository AS "Repository",
  COUNT() AS "Commits",
  SUM(lines_added) AS "TotalLinesAdded",
  SUM(lines_added) / COUNT(DISTINCT user) AS "LinesPerUser"  
FROM github_events
WHERE '2015-04-15T00:00:00' <= __time AND __time < '2015-04-16T00:00:00' AND event_type = "Commit"
GROUP BY repository
ORDER BY Commits DESC
LIMIT 5;
```

In this query `github_events` is the datasource of public events that happened on [GitHub](https://developer.github.com/v3/).
This query computes the number of *Commits*, *TotalLinesAdded*, and *LinesPerUser* for every repository and then returns the five repositories with the most commits.
 
A notable feature of PlyQL is that it treats a dataset (aka table) as just another datatype that can be nested within another table.
Consider this following query:
 
```sql
SELECT
  repository AS "Repository",
  COUNT() AS "Commits",
  SUM(lines_added) AS "TotalLinesAdded",
  SUM(lines_added) / COUNT(DISTINCT user) AS "LinesPerUser",
  (
    SELECT
      TIME_BUCKET(__time, PT1H, 'Etc/UTC') AS 'Hour',
      COUNT() AS "Commits"
    GROUP BY 1, -- this is a shortcut to group on the first SELECT clause
    ORDER BY Hour ASC  
  ) AS 'TimeSeries'
FROM github_events
WHERE '2015-04-15T00:00:00' <= __time AND __time < '2015-04-16T00:00:00' AND event_type = "Commit"
GROUP BY repository
ORDER BY Commits DESC
LIMIT 5;
``` 

This query is like the first example except that for every repository we are also querying an hourly time series of commits.  

## Operators

Name                    | Description
------------------------|-------------------------------------
+                       | Addition operator
-                       | Minus operator / Unary negation
*                       | Multiplication operator
/                       | Division operator
=, IS                   | Equal operator
!=, <>, IS NOT          | Not equal operator
>=                      | Greater than or equal operator
>                       | Greater than operator
<=                      | Less than or equal operator
<                       | Less than operator
BETWEEN ... AND ...     | Check whether a value is within a range of values
NOT BETWEEN ... AND ... | Check whether a value is not within a range of values
LIKE, CONTAINS          | Simple pattern matching
NOT LIKE, CONTAINS      | Negation of simple pattern matching
REGEXP                  | Pattern matching using regular expressions
NOT REGEXP              | Negation of REGEXP
NOT, !                  | Negates value
AND                     | Logical AND
OR                      | Logical OR


## Functions

**NUMBER_BUCKET**(operand, size, offset)

Bucket the numeric dimension into a buckets of size `size` with the given offset.

Example: `NUMBER_BUCKET($revenue, 5, 1)`

This will bucket the `$revenue` into buckets like: [1, 6), [6, 11), [11, 16), e.t.c.
Note that the buckets are open closed (start <= x < end).


**TIME_BUCKET**(operand, duration, timezone)

Bucket the time into a bucket of size `duration` in the given `timezone`.

Example: `TIME_BUCKET(time, 'P1D', 'America/Los_Angeles')`

This will bucket the `time` variable into day chunks where days are defined in the `America/Los_Angeles` timezone.


**TIME_PART**(operand, part, timezone)

Part the time into the given repeating buckets.

Example: `TIME_PART(time, 'DAY_OF_YEAR', 'America/Los_Angeles')`

This will part the `time` variable into the (integer) number that represents what day of the year it is.

The possible part values are:

* `SECOND_OF_MINUTE`, `SECOND_OF_HOUR`, `SECOND_OF_DAY`, `SECOND_OF_WEEK`, `SECOND_OF_MONTH`, `SECOND_OF_YEAR`
* `MINUTE_OF_HOUR`, `MINUTE_OF_DAY`, `MINUTE_OF_WEEK`, `MINUTE_OF_MONTH`, `MINUTE_OF_YEAR`
* `HOUR_OF_DAY`, `HOUR_OF_WEEK`, `HOUR_OF_MONTH`, `HOUR_OF_YEAR`
* `DAY_OF_WEEK`, `DAY_OF_MONTH`, `DAY_OF_YEAR`
* `WEEK_OF_MONTH`, `WEEK_OF_YEAR`
* `MONTH_OF_YEAR`


**TIME_FLOOR**(operand, duration, timezone)

Floor the time to the nearest `duration` in the given `timezone`.

Example: `TIME_FLOOR(time, 'P1D', 'America/Los_Angeles')`

This will floor the `time` variable to the start of day, where days are defined in the `America/Los_Angeles` timezone.


**TIME_SHIFT**(operand, duration, step, timezone)

Shift the time forwards by `duration` * `step` in the given `timezone`.
`step` may be negative.

Example: `TIME_SHIFT(time, 'P1D', -2, 'America/Los_Angeles')`

This will shift the `time` variable two days back in time, where days are defined in the `America/Los_Angeles` timezone.


**TIME_RANGE**(operand, duration, step, timezone)

Create a range form `time` and the point that is `duration` * `step` away from `time` in the given `timezone`.
`step` may be negative.

Example: `TIME_RANGE(time, 'P1D', -2, 'America/Los_Angeles')`

This will shift the `time` variable two days back in time, where days are defined in the `America/Los_Angeles` timezone and create a range of time-2*P1D -> time


**SUBSTR**(*str*, *pos*, *len*)

Returns a substring *len* characters long from string *str*, starting at position *pos*.

**CONCAT**(*str1*, *str2*, ...)

Returns the string that results from concatenating the arguments. May have one or more arguments. 

**EXTRACT**(*str*, *regexp*)

Returns the first matching group that results form matching *regexp* to *str*.

**LOOKUP**(*str*, *lookup-namespace*)

Returns the value for the key *str* withing the *lookup-namespace*.

## Aggregations

**COUNT**(*expr?*)

If used without an expression (or as `COUNT(*)`) returns the count of the number of rows.
When an expression is provided returns a count of the rows where *expr* is not null.
 
**COUNT**(**DISTINCT** *expr*), **COUNT_DISTINCT**(*expr*) 

Returns the count of the number of rows with different *expr* values.

**SUM**(*expr*) 

Returns the sum of all *expr* values.

**MIN**(*expr*) 

Returns the min of all *expr* values.

**MAX**(*expr*) 

Returns the max of all *expr* values.

**AVG**(*expr*)

Returns the average of all *expr* values.

**QUANTILE**(*expr*, *quantile*)

Returns the upper *quantile* of all *expr* values.

**CUSTOM**(*custom_name*)

Returns the user defined aggregation named *custom_name*.
