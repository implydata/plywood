# Expressions

Expressions are the backbone of Plywood. The basic flow is to make and expression and then hand it to Plywood to evaluate it.
Expressions can be expressed as JSON or composed via the provided operands.

## Creating expressions

There are many ways of creating expressions to account for all the different ways in which Plywood can be used.


**$**(*name*)

Will create a reference expression that refers to the given name.
Writing `$('x')` is the same as parsing `$x`

```javascript
var ex = $('x');
ex.compute({ x: 10 }).then(console.log); // => 10
```


**r**(*value*)

Will create a literal value expression out of the given value.

```javascript
var ex = r('$x');
ex.compute().then(console.log); // => '$x'
```

There are a number of basic literal expressions that are provided as static members on the `Expression` class.

```javascript
Expression.NULL.equals(r(null));

Expression.ZERO.equals(r(0));

Expression.ONE.equals(r(1));

Expression.FALSE.equals(r(false));

Expression.TRUE.equals(r(true));

Expression.EMPTY_STRING.equals(r(''));
```


## Composing expressions

Expressions are designed to be composed by method chaining.
All of the functions below operate on an expression and produce an expression in turn.


*operand*.**performAction**(action: Action, markSimple?: boolean)

Perform a specific action on an expression.
This method is useful if you are creating actions as action objects.
All the methods below create the appropriate action and then call this method.


### Basic arithmetic


*operand*.**add**(...exs: any[])

Adds the arguments to the operand.
Writing `$('x').add(1)` is the same as parsing `$x + 1`

```javascript
var ex = $('x').add('$y', 1);
ex.compute({ x: 10, y: 2 }).then(console.log); // => 13
```


*operand*.**subtract**(...exs: any[])

Subtracts the arguments from the operand.
Writing `$('x').subtract(1)` is the same as parsing `$x - 1`

```javascript
var ex = $('x').subtract('$y', 1);
ex.compute({ x: 10, y: 2 }).then(console.log); // => 7
```


*operand*.**negate**()

Negates the operand.
Writing `$('x').negate()` is the same as parsing `-$x`

```javascript
var ex = $('x').negate();
ex.compute({ x: 10 }).then(console.log); // => -10
```


*operand*.**multiply**(...exs: any[])

Multiplies the operator by the arguments.
Writing `$('x').multiply(3)` is the same as parsing `$x * 3`

```javascript
var ex = $('x').multiply('$y', 3);
ex.compute({ x: 10, y: 2 }).then(console.log); // => 60
```


*operand*.**divide**(...exs: any[])

Divides the operator by the arguments.
Writing `$('x').divide(3)` is the same as parsing `$x / 3`

```javascript
var ex = $('x').divide('$y');
ex.compute({ x: 10, y: 2 }).then(console.log); // => 5
```


*operand*.**reciprocate**()

Reciprocates the operand.
Writing `$('x').reciprocate()` is the same as parsing `1 / $x`

```javascript
var ex = $('x').reciprocate();
ex.compute({ x: 10 }).then(console.log); // => 0.1
```


### Boolean predicates


*operand*.**is**(ex: any)

Checks that the operand and the given expression are equal.
Writing `$('x').is(5)` is the same as parsing `$x == 5`

```javascript
var ex = $('x').is(5);
ex.compute({ x: 10 }).then(console.log); // => false
```


*operand*.**isnt**(ex: any)

Checks that the operand and the given expression are not equal.
Writing `$('x').isnt(5)` is the same as parsing `$x != 5`

```javascript
var ex = $('x').isnt(5);
ex.compute({ x: 10 }).then(console.log); // => true
```


*operand*.**lessThan**(ex: any)

Checks that the operand is less than the given expression.
Writing `$('x').lessThan(5)` is the same as parsing `$x < 5`

```javascript
var ex = $('x').lessThan(5);
ex.compute({ x: 10 }).then(console.log); // => false
```


*operand*.**lessThanOrEqual**(ex: any)

Checks that the operand is less than or equal the given expression.
Writing `$('x').lessThanOrEqual(5)` is the same as parsing `$x <= 5`

```javascript
var ex = $('x').lessThanOrEqual(5);
ex.compute({ x: 10 }).then(console.log); // => false
```


*operand*.**greaterThan**(ex: any)

Checks that the operand is greater than the given expression.
Writing `$('x').greaterThan(5)` is the same as parsing `$x > 5`

```javascript
var ex = $('x').greaterThan(5);
ex.compute({ x: 10 }).then(console.log); // => true
```


*operand*.**greaterThanOrEqual**(ex: any)

Checks that the operand is greater than the given expression.
Writing `$('x').greaterThanOrEqual(5)` is the same as parsing `$x >= 5`

```javascript
var ex = $('x').greaterThanOrEqual(5);
ex.compute({ x: 10 }).then(console.log); // => true
```


*operand*.**contains**(ex: any, compare?: string)

Checks whether the operand contains the given expression.
An optional argument specifying weather the check should be case sensitive is provided.

```javascript
var ex = $('str').contains('ello');
ex.compute({ str: 'Hello World' }).then(console.log); // => true
```


*operand*.**match**(re: string)

Checks whether the operand matches the given RegExp that is provided as a string.

```javascript
var ex = $('str').match('^Hell.*d$');
ex.compute({ str: 'Hello World' }).then(console.log); // => true
```


*operand*.**in**(ex: any)

Checks whether the operand is in the provided set.

```javascript
var ex = $('x').in([5, 8]);
ex.compute({ x: 10 }).then(console.log); // => false

var ex = $('str').in(['hello', 'world']);
ex.compute({ str: 'hello' }).then(console.log); // => true
```


*operand*.**not**()

Inverts the truth value of the operand.
Writing `$('x').not()` is the same as parsing `not $x`

```javascript
var ex = $('x').not();
ex.compute({ x: true }).then(console.log); // => false
```


*operand*.**and**(...exs: any[])

Performs a boolean AND operation on the operand and the given expressions.
Writing `$('x').and($('y'))` is the same as parsing `$x and $y`

```javascript
var ex = $('x').and($('y'));
ex.compute({ x: true, y: false }).then(console.log); // => false
```


*operand*.**or**(...exs: any[])

Performs a boolean OR operation on the operand and the given expressions.
Writing `$('x').or($('y'))` is the same as parsing `$x or $y`

```javascript
var ex = $('x').or($('y'));
ex.compute({ x: true, y: false }).then(console.log); // => true
```


### String manipulation


*operand*.**substr**(position: number, length: number)

Extracts a substring from the operand.

```javascript
var ex = $('str').substr(1, 5);
ex.compute({ str: 'Hello World' }).then(console.log); // => 'ello '
```


*operand*.**extract**(re: string)
  
Extracts a substring from the operand.

```javascript
var ex = $('str').extract("([0-9]+\\.[0-9]+\\.[0-9]+)");
ex.compute({ str: 'kafka-0.7.2' }).then(console.log); // => '0.7.2'
ex.compute({ str: 'Web 2.0' }).then(console.log); // => null
```


*operand*.**concat**(...exs: any[])

Performs a string concatenation operation on the operand and the given expressions.
Writing `$('str').concat(r('hello'))` is the same as parsing `$str ++ 'hello'`

```javascript
var ex = r('[').concat($('str'), r(']'));
ex.compute({ str: 'Hello World' }).then(console.log); // => '[Hello World]'
```


*operand*.**lookup**(lookup: string)

Performs a lookup within the specified namespace.


### Number manipulation


*operand*.**numberBucket**(size: number, offset: number = 0)

Buckets the numeric operand to buckets defined by the `size` and `offset`.

```javascript
var ex = $('x').numberBucket(5);
ex.compute({ x: 7 }).then(console.log); // => [5, 10)
```


### Time manipulation


*operand*.**timeBucket**(duration: any, timezone?: string)

Buckets the operand time to the nearest `duration` within the given `timezone`.
Creates a TimeRange of size `duration`. 

```javascript
var ex = $('time').timeBucket('P1D');
```


*operand*.**timeFloor**(duration: any, timezone?: string)

Floors the operand time to the nearest `duration` within the given `timezone`.

```javascript
var ex = $('time').timeFloor('P1D');
```


*operand*.**timeShift**(duration: any, step: number, timezone?: string)

Shifts the operand time by `duration` * `step` within the given `timezone`.

```javascript
var ex = $('time').timeShift('P1D', -2);
```


*operand*.**timeRange**(duration: any, step: number, timezone?: string)

Constructs a range with the operand time as one end point and the other endpoint determined by shifting the provided time by `duration` * `step` within the given `timezone`.
Creates a TimeRange of size `duration` * `step`.

```javascript
var ex = $('time').timeRange('P1D', -2);
```


*operand*.**timePart**(part: string, timezone?: string)

This will 'part' the operand into the (integer) number that represents what day of the year it is within the given `timezone`. 

The possible part values are:

* `SECOND_OF_MINUTE`, `SECOND_OF_HOUR`, `SECOND_OF_DAY`, `SECOND_OF_WEEK`, `SECOND_OF_MONTH`, `SECOND_OF_YEAR`
* `MINUTE_OF_HOUR`, `MINUTE_OF_DAY`, `MINUTE_OF_WEEK`, `MINUTE_OF_MONTH`, `MINUTE_OF_YEAR`
* `HOUR_OF_DAY`, `HOUR_OF_WEEK`, `HOUR_OF_MONTH`, `HOUR_OF_YEAR`
* `DAY_OF_WEEK`, `DAY_OF_MONTH`, `DAY_OF_YEAR`
* `WEEK_OF_MONTH`, `WEEK_OF_YEAR`
* `MONTH_OF_YEAR`

```javascript
var ex = $('time').timePart('DAY_OF_WEEK');
```


### Split Apply Combine based transformations

Let's pretend we have this simple dataset:

```javascript
var someDataset = Dataset.fromJS([
  { cut: 'Good',  price: 400, time: new Date('2015-10-01T10:20:30Z') },
  { cut: 'Good',  price: 300, time: new Date('2015-10-02T10:20:30Z') },
  { cut: 'Great', price: 124, time: null },
  { cut: 'Wow',   price: 160, time: new Date('2015-10-04T10:20:30Z') },
  { cut: 'Wow',   price: 100, time: new Date('2015-10-05T10:20:30Z') }
]);
```


*operand*.**filter**(ex: any)

Filter the given dataset using the given boolean expression leave only the items for which the expression returned `true`.

```javascript
var ex = $('data').filter('$cut == "Good"');
ex.compute({ data: someDataset }).then(console.log);
// =>
Dataset.fromJS([
  { cut: 'Good',  price: 400, time: new Date('2015-10-01T10:20:30Z') },
  { cut: 'Good',  price: 300, time: new Date('2015-10-02T10:20:30Z') }
])
```


*operand*.**split**(splits: any, name?: string, dataName?: string)

Split the data based on the given expression

```javascript
var ex = $('data').split('$cut', 'Cut');
ex.compute({ data: someDataset }).then(console.log);
// =>
Dataset.fromJS([
  {
    Cut: 'Good',
    data: [
      { cut: 'Good',  price: 400, time: new Date('2015-10-01T10:20:30Z') },
      { cut: 'Good',  price: 300, time: new Date('2015-10-02T10:20:30Z') }
    ]
  },
  {
    Cut: 'Great',
    data: [
      { cut: 'Great', price: 124, time: null }
    ]
  },
  {
    Cut: 'Wow',
    data: [
      { cut: 'Wow',   price: 160, time: new Date('2015-10-04T10:20:30Z') },
      { cut: 'Wow',   price: 100, time: new Date('2015-10-05T10:20:30Z') }
    ]
  }
])
```


*operand*.**apply**(name: string, ex: any)

Apply the given expression to every datum in the dataset saving the result as `name`.

```javascript
var ex = $('data').apply('DoublePrice', '$price * 2');
ex.compute({ data: someDataset }).then(console.log);
// =>
Dataset.fromJS([
  { cut: 'Good',  price: 400, DoublePrice: 800, time: new Date('2015-10-01T10:20:30Z') },
  { cut: 'Good',  price: 300, DoublePrice: 600, time: new Date('2015-10-02T10:20:30Z') },
  { cut: 'Great', price: 124, DoublePrice: 248, time: null },
  { cut: 'Wow',   price: 160, DoublePrice: 320, time: new Date('2015-10-04T10:20:30Z') },
  { cut: 'Wow',   price: 100, DoublePrice: 200, time: new Date('2015-10-05T10:20:30Z') }
])
```


*operand*.**sort**(ex: any, direction: string)

Sort the operand dataset according to the given expression.

```javascript
var ex = $('data').sort('$price', 'ascending');
ex.compute({ data: someDataset }).then(console.log);
// =>
Dataset.fromJS([
  { cut: 'Wow',   price: 100, time: new Date('2015-10-05T10:20:30Z') },
  { cut: 'Great', price: 124, time: null },
  { cut: 'Wow',   price: 160, time: new Date('2015-10-04T10:20:30Z') },
  { cut: 'Good',  price: 300, time: new Date('2015-10-02T10:20:30Z') },
  { cut: 'Good',  price: 400, time: new Date('2015-10-01T10:20:30Z') }
])
```


*operand*.**limit**(limit: int)

Limit the operand dataset to the given positive integer.

```javascript
var ex = $('data').limit(3);
ex.compute({ data: someDataset }).then(console.log);
// =>
Dataset.fromJS([
  { cut: 'Good',  price: 400, DoublePrice: 800, time: new Date('2015-10-01T10:20:30Z') },
  { cut: 'Good',  price: 300, DoublePrice: 600, time: new Date('2015-10-02T10:20:30Z') },
  { cut: 'Great', price: 124, DoublePrice: 248, time: null }
])
```

### Aggregate expressions

Let's pretend we have this simple dataset:

```javascript
var someDataset = Dataset.fromJS([
  { cut: 'Good',  price: 400, time: new Date('2015-10-01T10:20:30Z') }
  { cut: 'Good',  price: 300, time: new Date('2015-10-02T10:20:30Z') }
  { cut: 'Great', price: 124, time: null }
  { cut: 'Wow',   price: 160, time: new Date('2015-10-04T10:20:30Z') }
  { cut: 'Wow',   price: 100, time: new Date('2015-10-05T10:20:30Z') }
]);
```


*operand*.**count**()

Computes the count of the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // => 5
```


*operand*.**sum**(ex: any)

Computes the sum of the given expression in the operand dataset

```javascript
var ex = $('data').sum($('price'));
ex.compute({ data: someDataset }).then(console.log); // => 1084
```


*operand*.**min**(ex: any)

Computes the min of the given expression in the operand dataset

```javascript
var ex = $('data').min($('price'));
ex.compute({ data: someDataset }).then(console.log); // => 100
```


*operand*.**max**(ex: any)

Computes the max of the given expression in the operand dataset

```javascript
var ex = $('data').max($('price'));
ex.compute({ data: someDataset }).then(console.log); // => 400
```


*operand*.**average**(ex: any)

Computes the average of the given expression in the operand dataset

```javascript
var ex = $('data').average($('price'));
ex.compute({ data: someDataset }).then(console.log); // => 216.8
```


*operand*.**countDistinct**(ex: any)

Computes the count of the distinct items of the given expression in the operand dataset

```javascript
var ex = $('data').countDistinct($('cut'));
ex.compute({ data: someDataset }).then(console.log); // => 3
```


*operand*.**quantile**(ex: any, quantile: number)

Computes the quantile of the given expression in the operand dataset

```javascript
var ex = $('data').quantile($('price'), 0.95);
```


*operand*.**custom**(custom: string)

Computes the custom of the given expression in the operand dataset

```javascript
var ex = $('data').custom('customAggregator');
```
