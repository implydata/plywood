# plySQL

plySQL is the plywood flavored version of SQL that can be parsed into a plywood expression and then executed.

plySQL only supports `SELECT` queries as plywood's only focus is getting data out of the data store.

## Custom functions

### NUMBER_BUCKET(operand: Expression, size: Number, offset: Number)

Bucket the numeric dimension into a buckets of size `size` with the given offset.

Example: `NUMBER_BUCKET($revenue, 5, 1)`

This will bucket the `$revenue` into buckets like: [1, 6), [6, 11), [11, 16), e.t.c.
Note that the buckets are open closed (start <= x < end).


### TIME_BUCKET(operand: Expression, duration: String, timezone: String)

Bucket the time into a bucket of size `duration` in the given `timezone`.

Example: `TIME_BUCKET($time, 'P1D', 'America/Los_Angeles')`

This will bucket the `$time` variable into day chunks where days are defined in the `America/Los_Angeles` timezone.


### TIME_PART(operand: Expression, part: String, timezone: String)

Part the time into the given repeating buckets.

Example: `TIME_PART($time, 'DAY_OF_YEAR', 'America/Los_Angeles')`

This will part the `$time` variable into the (integer) number that represents what day of the year it is.

The possible part values are:

* SECOND_OF_MINUTE
* SECOND_OF_HOUR
* SECOND_OF_DAY
* SECOND_OF_WEEK
* SECOND_OF_MONTH
* SECOND_OF_YEAR
* MINUTE_OF_HOUR
* MINUTE_OF_DAY
* MINUTE_OF_WEEK
* MINUTE_OF_MONTH
* MINUTE_OF_YEAR
* HOUR_OF_DAY
* HOUR_OF_WEEK
* HOUR_OF_MONTH
* HOUR_OF_YEAR
* DAY_OF_WEEK
* DAY_OF_MONTH
* DAY_OF_YEAR
* WEEK_OF_MONTH
* WEEK_OF_YEAR


### SUBSTR(operand: Expression, position: Number, length: Number)

Returns the specified number of characters from a particular position of a given string expresion.


### coming soon: TIME_OFFSET(operand: Expression, duration: String, timezone: String)

### coming soon: CONCAT(op1: Expression, op2: Expression, ...)
