# New Expression Checklist

So you have decided (or been asked to) add a new expression to Plywood. That is great!
Here are the general steps to do it. Written down to hopefully add some structure to your work.

Let's assume that you are adding an expression called `foo bar`
 
1. Make sure your idea is sensible. Think about it and maybe run it by other people who have added more expressions than you.

1. Create the file `src/expressions/fooBarExpression.ts`. It might help to consider which existing expressions are most similar
   to foo bar and to copy their file instead of staring from scratch.
   
1. Add the newly created file `src/expressions/fooBarExpression.ts` to [compile-tsc](/compile-tsc).

1. Edit `src/expressions/fooBarExpression.ts` to implement the expression as best you can.
  * Ensure you implement a `class FooBarExpression` that has the *op* type of `"fooBar"`  
  * Ensure that any special properties are added to `ExpressionValue` and `ExpressionJS` interfaces in [baseExpression.ts](src/expressions/baseExpression.ts)

1. Add a immutable class test of to [expression.js](test/expressions/expression.mocha.js).
   Something that looks like `{ op: 'fooBar', ... }`.
   Ensure it works.

1. Add a `public fooBar(...): FooBarExpression` to [baseExpression.ts](src/expressions/baseExpression.ts) that allows for
   the creation of this expression via the JS API.
   Be mindful of the section that you put this function in. Ideally it should live with its own kind.
   
1. Add an entry for `*operand*.**fooBar**(...)` in the [expression docs](docs/expressions.md).
   Ensure that it is in the same relative order as the function in the previous step.
   Describe what *foo bar* does and add an example or two.
   
1. Add a test that ensures the native computation of this function is adequate.
   You probably want to add that test to [compute.mocha.js](test/overall/compute.mocha.js).
    
1. Add a test that ensures the correct parsing of this function by the expression parser.
   You probably want to add that test to [expressionParser.mocha.js](test/overall/expressionParser.mocha.js).

1. Think about any special conditions under which this expression can be simplified and add the adequate simplification
   rules to the `specialSimplify` function in `fooBarExpression.ts` and tests to [simplify.mocha.js](test/overall/simplify.mocha.js).
   Some ideas of what to consider:
  * Is this expression idempotent (`$x.fooBar(blah).fooBar(blah)` => `$x.fooBar(blah)`) or does it generally combine well with itself?
  * Is there a special 'zero' literal that, when acted upon, renders this expression is irrelevant (e.g. `X.multiply(0)` => `r(0)`)?     
  * Is there a special 'one' condition that, when in the expression, renders the expression useless (e.g. `X.multiply(1)` => `X`)?
  * Does it interact with other expressions in a specific way?    
   
1. All expressions should (preferably) be supported by all Externals. Add the functionality and tests accordingly. 
  * MySQL External: [code](src/external/mySqlExternal.ts), [test](test/external/mySqlExternal.mocha.js)
  * Druid External: [code](src/external/druidExternal.ts), [test](test/external/druidExternal.mocha.js)

1. Lastly update the [CHANGELOG](CHANGELOG.md) and make a Pull Request!
