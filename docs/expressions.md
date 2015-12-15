# Expressions

Expressions are the backbone of Plywood. The basic flow is to make and expression and then hand it to Plywood to evaluate it.
Expressions can be expressed as JSON or composed via the provided operands.

## Creating expressions

There are many ways of creating expressions to account for all the different ways in which Plywood can be used.

- Calling the basic functions provided by Plywood.
- 

**$**(*name*)

Will create a reference expression to name

## Composing expressions

Expressions are designed to be composed by method chaining.
All of the functions below operate on an expression and produce an expression in turn.

**performAction**(action: Action, markSimple?: boolean)

Perform a specific action on an expression

 
### Basic arithmetic 

**add**(...exs: any[])

Adds the arguments to the operand.
Writing `$('x').add(1)` is the same as parsing `$x + 1`

```javascript
var ex = $('x').add('$y', 1);
ex.compute({ x: 10, y: 2 }).then(console.log); // => 13
```

**subtract**(...exs: any[])

Subtracts the arguments from the operand.
Writing `$('x').subtract(1)` is the same as parsing `$x - 1`
    
```javascript
var ex = $('x').subtract('$y', 1);
ex.compute({ x: 10, y: 2 }).then(console.log); // => 7
```

**negate**()
    
Negates the operand.
Writing `$('x').negate()` is the same as parsing `-$x`
    
```javascript
var ex = $('x').negate();
ex.compute({ x: 10 }).then(console.log); // => -10
```

**multiply**(...exs: any[])
    
Blah 
    
**divide**(...exs: any[])

Blah

**reciprocate**()

Blah


## Boolean predicates

**is**(ex: any)

Blah

```javascript
var ex = $('x').is(5);
```

**isnt**(ex: any)

Blah

```javascript
var ex = $('x').is(5);
```

**lessThan**(ex: any)

Blah

```javascript
var ex = $('x').is(5);
```

**lessThanOrEqual**(ex: any)

Blah

```javascript
var ex = $('x').is(5);
```

**greaterThan**(ex: any)

Blah

```javascript
var ex = $('x').is(5);
```

**greaterThanOrEqual**(ex: any)

Blah

```javascript
var ex = $('x').is(5);
```

**contains**(ex: any, compare?: string)

Blah

```javascript
var ex = $('x').is(5);
```

**match**(re: string)

Blah

```javascript
var ex = $('x').is(5);
```

**in**(ex: any)

Blah

```javascript
var ex = $('x').is(5);
```

**not**()

Blah

```javascript
var ex = $('x').is(5);
```

**and**(...exs: any[])

Blah

```javascript
var ex = $('x').is(5);
```

**or**(...exs: any[])

Blah

```javascript
var ex = $('x').is(5);
```


## Split Apply Combine based transformations 

**filter**(ex: any)

Filter the given dataset using the given boolean expression leave only the items for which the expression returned `true`.


**split**(splits: any, name?: string, dataName?: string)

Split the data based on the given expression


**apply**(name: string, ex: any)

Apply the given expression to every datum in the dataset saving the result as `name`.


**sort**(ex: any, direction: string)

Sort the operand dataset according to the given expression.


**limit**(limit: int)

Limit the operand dataset to the given positive integer.


## Aggregate expressions

**count**()

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```

**sum**(ex: any)

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```

**min**(ex: any)

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```

**max**(ex: any)

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```

**average**(ex: any)

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```

**countDistinct**(ex: any)

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```

**quantile**(ex: any, quantile: number)

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```

**custom**(custom: string)

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```
