# PlyQL

PlyQL is a SQL-like language that can be parsed into a plywood expression and then executed.

PlyQL only supports `SELECT`, `DESCRIBE`, and `SHOW TABLES` queries as plywood's main focus is getting information out of the data store.

## Examples

For these examples we are querying a Druid broker node hosted on a docker machine with ip `192.168.60.100` at port `8082`.
We pass this address (`192.168.60.100:8082`) to the `--host` (`-h`) option.
A full list of CLI options is documented on the [Github page] (https://github.com/implydata/plyql)
To start, we can issue a `SHOW TABLES` query for the list of data sources, which we pass in to the  `--query` (`-q`) option.

```sql
plyql -h 192.168.60.100:8082 -q 'SHOW TABLES'
```

Returns:

```json
[
  "wikipedia"
]
```


PlyQL allows you to interface with druid data sources as if they were SQL tables, so we can issue a `describe` query on it.

```sql
plyql -h 192.168.60.100:8082 -q 'DESCRIBE wikipedia'
```

Returns the column definitions of the data source:

```json
[
  {
    "name": "__time",
    "type": "TIME"
  },
  {
    "name": "added",
    "type": "NUMBER",
    "unsplitable": true
  },
  {
    "name": "channel",
    "type": "STRING"
  },
  {
    "name": "cityName",
    "type": "STRING"
  },
  {
    "name": "comment",
    "type": "STRING"
  },
  {
    "name": "commentLength",
    "type": "STRING"
  },
  {
    "name": "count",
    "type": "NUMBER",
    "unsplitable": true
  },
  {
    "name": "countryIsoCode",
    "type": "STRING"
  },
  {
    "name": "countryName",
    "type": "STRING"
  },
  {
    "name": "deleted",
    "type": "NUMBER",
    "unsplitable": true
  },
  {
    "name": "delta",
    "type": "NUMBER",
    "unsplitable": true
  },
  {
    "name": "deltaByTen",
    "type": "NUMBER",
    "unsplitable": true
  },
  {
    "name": "delta_hist",
    "type": "NUMBER",
    "special": "histogram"
  },
  "... results omitted ..."
]
```

Here is a simple query that gets the maximum of the `__time` column. This query displays the time of the latest event in the database.

```sql
plyql -h 192.168.60.100:8082 -q 'SELECT MAX(__time) AS maxTime FROM wikipedia'
```

Returns:

```json
[
  {
    "maxTime": {
      "type": "TIME",
      "value": "2015-09-12T23:59:00.000Z"
    }
  }
]
```

Ok now you might want to examine the different hashtags that are trending.

You might do a GROUP BY on the `page` column like this:

```sql
plyql -h 192.168.60.100:8082 -q '
SELECT page as pg, 
COUNT() as cnt 
FROM wikipedia 
GROUP BY page 
ORDER BY cnt DESC 
LIMIT 5;
'
```

This will throw an error because there is no time filter specified and the plyql guards against this.

This behaviour can be disabled using the `--allow eternity` flag but it is generally bad idea to do it when working with
large amounts of data as it can issue computationally prohibitive queries.
  
Try it again, with a time filter:
  
```sql
plyql -h 192.168.60.100:8082 -q '
SELECT page as pg, 
COUNT() as cnt 
FROM wikipedia 
WHERE "2015-09-12T00:00:00" <= __time AND __time < "2015-09-13T00:00:00"
GROUP BY page 
ORDER BY cnt DESC 
LIMIT 5;
'
```

Results:
  
```json
[
  {
    "cnt": 314,
    "pg": "Jeremy Corbyn"
  },
  {
    "cnt": 255,
    "pg": "User:Cyde/List of candidates for speedy deletion/Subpage"
  },
  {
    "cnt": 228,
    "pg": "Wikipedia:Administrators' noticeboard/Incidents"
  },
  {
    "cnt": 186,
    "pg": "Wikipedia:Vandalismusmeldung"
  },
  {
    "cnt": 160,
    "pg": "Total Drama Presents: The Ridonculous Race"
  }
]
```
  
Plyql has an option `--interval` (`-i`) that automatically filters time on the last `interval` worth of time.
It is useful if you do not want to type out a time filter.

```sql
plyql -h 192.168.60.100:8082 -i P1Y -q '
SELECT page as pg, 
COUNT() as cnt 
FROM wikipedia 
GROUP BY page 
ORDER BY cnt DESC 
LIMIT 5;
'
```

To get a breakdown by time the `TIME_BUCKET` function can be used:

```sql
plyql -h 192.168.60.100:8082 -i P1Y -q '
SELECT SUM(added) as TotalAdded 
FROM wikipedia 
GROUP BY TIME_BUCKET(__time, PT6H, "Etc/UTC");
'
```

Returns:

```json
[
  {
    "TotalAdded": 15426936,
    "split0": {
      "start": "2015-09-12T00:00:00.000Z",
      "end": "2015-09-12T06:00:00.000Z",
      "type": "TIME_RANGE"
    }
  },
  {
    "TotalAdded": 25996165,
    "split0": {
      "start": "2015-09-12T06:00:00.000Z",
      "end": "2015-09-12T12:00:00.000Z",
      "type": "TIME_RANGE"
    }
  },
  "... results omitted ..."
]
```

Note that the grouping column was not selected but was still returned as if `TIME_BUCKET(__time, PT1H, 'Etc/UTC') as 'split'`
was one of the select clauses.

Time parting is also supported, here is an example:

```sql
plyql -h 192.168.60.100:8082 -i P1Y -q '
SELECT TIME_PART(__time, HOUR_OF_DAY, "Etc/UTC") as HourOfDay, 
SUM(added) as TotalAdded 
FROM wikipedia 
GROUP BY 1 
ORDER BY TotalAdded DESC LIMIT 3;
'
```

Notice that this `GROUP BY` is referring to the first column in the select.

This returns:

```json
[
  {
    "TotalAdded": 8077302,
    "HourOfDay": 10
  },
  {
    "TotalAdded": 5998730,
    "HourOfDay": 17
  },
  {
    "TotalAdded": 5210222,
    "HourOfDay": 18
  }
]
```

Quantiles on histograms are supported. 
Suppose you wanted to use histograms to calculate the 0.95 quantile of delta filtered on city is San Francisco.
```sql
plyql -h 192.168.60.100:8082 -i P1Y -q '
SELECT 
QUANTILE(delta_hist WHERE cityName = "San Francisco", 0.95) as P95 
FROM wikipedia;
'
```

It is also possible to do multi dimensional GROUP BYs

```sql
plyql -h 192.168.60.100:8082 -i P1Y -q '
SELECT TIME_BUCKET(__time, PT1H, "Etc/UTC") as Hour, 
page as PageName, 
SUM(added) as TotalAdded 
FROM wikipedia 
GROUP BY 1, 2 
ORDER BY TotalAdded DESC 
LIMIT 3;
'
```

Returns:

```json
[
  {
    "TotalAdded": 242211,
    "PageName": "Wikipedia‐ノート:即時削除の方針/過去ログ16",
    "Hour": {
      "start": "2015-09-12T15:00:00.000Z",
      "end": "2015-09-12T16:00:00.000Z",
      "type": "TIME_RANGE"
    }
  },
  {
    "TotalAdded": 232941,
    "PageName": "Користувач:SuomynonA666/Заготовка",
    "Hour": {
      "start": "2015-09-12T14:00:00.000Z",
      "end": "2015-09-12T15:00:00.000Z",
      "type": "TIME_RANGE"
    }
  },
  {
    "TotalAdded": 214017,
    "PageName": "User talk:Estela.rs",
    "Hour": {
      "start": "2015-09-12T12:00:00.000Z",
      "end": "2015-09-12T13:00:00.000Z",
      "type": "TIME_RANGE"
    }
  }
]
```

Here is an advanced example that gets the top 5 pages edited by time. 
A notable feature of PlyQL is that it treats a dataset (aka table) as just another datatype that can be nested within another table.
This allows us to nest queries as aggregates like so:

```sql
plyql -h 192.168.60.100:8082 -i P1Y -q '
SELECT page as Page, 
COUNT() as cnt, 
(
  SELECT 
  SUM(added) as TotalAdded 
  GROUP BY TIME_BUCKET(__time, PT1H, "Etc/UTC") 
  LIMIT 3 -- only get the first 3 hours to keep this example output small
) as "ByTime" 
FROM wikipedia 
GROUP BY page 
ORDER BY cnt DESC 
LIMIT 5;
'
```

Returns:

```json
[
  {
    "cnt": 314,
    "Page": "Jeremy Corbyn",
    "ByTime": [
      {
        "TotalAdded": 1075,
        "split0": {
          "start": "2015-09-12T01:00:00.000Z",
          "end": "2015-09-12T02:00:00.000Z",
          "type": "TIME_RANGE"
        }
      },
      {
        "TotalAdded": 0,
        "split0": {
          "start": "2015-09-12T07:00:00.000Z",
          "end": "2015-09-12T08:00:00.000Z",
          "type": "TIME_RANGE"
        }
      },
      {
        "TotalAdded": 10553,
        "split0": {
          "start": "2015-09-12T08:00:00.000Z",
          "end": "2015-09-12T09:00:00.000Z",
          "type": "TIME_RANGE"
        }
      }
    ]
  },
  {
    "cnt": 255,
    "Page": "User:Cyde/List of candidates for speedy deletion/Subpage",
    "ByTime": [
      {
        "TotalAdded": 73,
        "split0": {
          "start": "2015-09-12T00:00:00.000Z",
          "end": "2015-09-12T01:00:00.000Z",
          "type": "TIME_RANGE"
        }
      },
      {
        "TotalAdded": 3363,
        "split0": {
          "start": "2015-09-12T01:00:00.000Z",
          "end": "2015-09-12T02:00:00.000Z",
          "type": "TIME_RANGE"
        }
      },
      {
        "TotalAdded": 336,
        "split0": {
          "start": "2015-09-12T02:00:00.000Z",
          "end": "2015-09-12T03:00:00.000Z",
          "type": "TIME_RANGE"
        }
      }
    ]
  },
  "... results omitted ..."
]
```

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
* `YEAR`


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

This will shift the `time` variable two days back in time, where days are defined in the `America/Los_Angeles` timezone and create a range of time-2*P1D -> time.


**SUBSTR**(*str*, *pos*, *len*)

Returns a substring *len* characters long from string *str*, starting at position *pos*

**CONCAT**(*str1*, *str2*, ...)

Returns the string that results from concatenating the arguments. May have one or more arguments

**EXTRACT**(*str*, *regexp*)

Returns the first matching group that results form matching *regexp* to *str*

**LOOKUP**(*str*, *lookup-namespace*)

Returns the value for the key *str* withing the *lookup-namespace*

**IFNULL**(*expr1*, *expr2*)

Returns the *expr* if it's not null, otherwise returns *expr2* 

**FALLBACK**(*expr1*, *expr2*)

This is a synonym for **IFNULL**(*expr1*, *expr2*)

**OVERLAP**(*expr1*, *expr2*)

Checks whether *expr1* and *expr2* overlap

### Mathematical Functions

**ABS**(*expr*)

  Returns the absolute value of *expr* value.  

**POW**(*expr1*, *expr2*)

  Returns *expr1* raised to the power of *expr2*.  

**POWER**(*expr1*, *expr2*)  

This is a synonym for **POW**(*expr1*, *expr2*)  

**SQRT**(*expr*)

  Returns the square root of *expr*

  **EXP**(*expr*)

Returns the value of e (base of natural logs) raised to the power of *expr*.


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
