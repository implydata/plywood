# Expressions

Expressions are the backbone of Plywood. The basic flow is to make and expression and then hand it to Plywood to evaluate it.
Expressions can be expressed as JSON or composed via the provided operands.

## Creating expressions

There are many ways of creating expressions to account for all the different ways in which Plywood can be used.
 
**$**(*name*)

Will create a reference expression that refers to the given name. 

```javascript
var ex = $('x');
ex.compute({ x: 10 }).then(console.log); // => 10
```

**r**(*value*)

Will create a literal value expression out of the givven value.

```javascript
var ex = r('$x');
ex.compute().then(console.log); // => '$x'
```


## Composing expressions

Expressions are designed to be composed by method chaining.
All of the functions below operate on an expression and produce an expression in turn.

*operand*.**performAction**(action: Action, markSimple?: boolean)

Perform a specific action on an expression

 
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
    
Blah 
    
*operand*.**divide**(...exs: any[])

Blah

*operand*.**reciprocate**()

Blah


## Boolean predicates

*operand*.**is**(ex: any)

Blah

```javascript
var ex = $('x').is(5);
```

*operand*.**isnt**(ex: any)

Blah

```javascript
var ex = $('x').is(5);
```

*operand*.**lessThan**(ex: any)

Blah

```javascript
var ex = $('x').is(5);
```

*operand*.**lessThanOrEqual**(ex: any)

Blah

```javascript
var ex = $('x').is(5);
```

*operand*.**greaterThan**(ex: any)

Blah

```javascript
var ex = $('x').is(5);
```

*operand*.**greaterThanOrEqual**(ex: any)

Blah

```javascript
var ex = $('x').is(5);
```

*operand*.**contains**(ex: any, compare?: string)

Blah

```javascript
var ex = $('x').is(5);
```

*operand*.**match**(re: string)

Blah

```javascript
var ex = $('x').is(5);
```

*operand*.**in**(ex: any)

Blah

```javascript
var ex = $('x').is(5);
```

*operand*.**not**()

Blah

```javascript
var ex = $('x').is(5);
```

*operand*.**and**(...exs: any[])

Blah

```javascript
var ex = $('x').is(5);
```

*operand*.**or**(...exs: any[])

Blah

```javascript
var ex = $('x').is(5);
```


## Split Apply Combine based transformations 

*operand*.**filter**(ex: any)

Filter the given dataset using the given boolean expression leave only the items for which the expression returned `true`.


*operand*.**split**(splits: any, name?: string, dataName?: string)

Split the data based on the given expression


*operand*.**apply**(name: string, ex: any)

Apply the given expression to every datum in the dataset saving the result as `name`.


*operand*.**sort**(ex: any, direction: string)

Sort the operand dataset according to the given expression.


*operand*.**limit**(limit: int)

Limit the operand dataset to the given positive integer.


## Aggregate expressions

*operand*.**count**()

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```

*operand*.**sum**(ex: any)

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```

*operand*.**min**(ex: any)

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```

*operand*.**max**(ex: any)

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```

*operand*.**average**(ex: any)

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```

*operand*.**countDistinct**(ex: any)

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```

*operand*.**quantile**(ex: any, quantile: number)

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```

*operand*.**custom**(custom: string)

Counts the datums in the operand dataset

```javascript
var ex = $('data').count();
ex.compute({ data: someDataset }).then(console.log); // 5
```
