# New Action Checklist

So you have decided (or been asked to) add a new action to Plywood. That is great!
Here are the general steps to do it. Written down to hopefully add some structure to your work.

Let's assume that you are adding an action called `foo bar`
 
1. Make sure your idea is sensible. Think about it and maybe run it by other people who have added more actions than you.

1. Create the file `src/actions/fooBarAction.ts`. It might help to consider which existing actions are most similar
   to foo bar and to copy their file instead of staring from scratch.
   
1. Add the newly created file `src/actions/fooBarAction.ts` to [compile-tsc](/compile-tsc).

1. Edit `src/actions/fooBarAction.ts` to implement the action as best you can.
  * Ensure you implement a `class FooBarAction` that has the *action* type of `"fooBar"`  
  * Ensure that any special properties are added to `ActionValue` and `ActionJS` interfaces in [baseAction.ts](src/actions/baseAction.ts)

1. Add a immutable class test of to [actions.ts](test/overall/actions.ts). Something that looks like `{ action: 'fooBar', ... }`. Ensure it works.

1. Add a `public fooBar(...): ChainExpression` to [baseExpression.ts](src/expressions/baseExpression.ts) that allows for
   the creation of this action via the JS API.
   Be mindful of the section that you put this function in. Ideally it should live with its own kind.
   
1. Add an entry for `*operand*.**fooBar**(...)` in the [expression docs](docs/expressions.md).
   Ensure that it is in the same relative order as the function in the previous step.
   Describe what *foo bar* does and add an example or two.
   
1. Add a test that ensures the native computation of this function is adequate.
   You probably want to add that test to [compute.mocha.coffee](test/overall/compute.mocha.coffee).
    
1. Add a test that ensures the correct parsing of this function by the expression parser.    
   You probably want to add that test to [expressionParser.mocha.coffee](test/overall/expressionParser.mocha.coffee).

1. Think about any special conditions under which this action can be simplified and add the adequate simplification
   rules to `fooBarAction.ts` and tests to [simplify.mocha.coffee](test/overall/simplify.mocha.coffee).
   Some ideas of what to consider:
  * Is this action idempotent (`$x.fooBar(blah).fooBar(blah)` => `$x.fooBar(blah)`) or does it generally combine well with itself?
    Implement `_foldWithPrevAction`.
  * Is there a special 'zero' literal that, when acted upon, this action is irrelevant (e.g. `r(0).multiply($x)` => `r(0)`)?
    Implement `_performOnLiteral`.  
  * Is there a special 'zero' literal that, when in the action, does something special (e.g. `$x.multiply(0)` => `r(0)`)?
    Implement `_removeAction` and/or `_nukeExpression`.
      
1. Add this function to the PlyQL grammar in [plyql.pegjs](src/expressions/plyql.pegjs).
  * Figure out what names you will give this function.
    You should allow both `'FOO_BAR'` and whatever is standard to call it in general SQL dialects.
    In general PlyQL tries to be MySQL compliant so have a look if a similar function is implemented by MySQL.
    It is also possible that by adding this function you will facilitate some other function to be implemented
    (for example when `MatchAction` was added it allowed for `LIKE` and `REGEXP` to be implemented as well as `MATCH`).
  * Add all the names to the `reservedWords` list.  
  * Create tokens for these names.

1. Add tests for `FOO_BAR` and all the other functions in the PlyQL parser.
   The tests go in [plyqlParser.mocha.coffee](test/overall/plyqlParser.mocha.coffee).
   
1. All actions should (preferably) be supported by all Externals. Add the functionality and tests accordingly. 
  * MySQL External: [code](src/external/mySqlExternal.ts), [test](test/external/mySqlExternal.mocha.coffee)
  * Druid External: [code](src/external/druidExternal.ts), [test](test/external/druidExternal.mocha.coffee)

1. Lastly update the [CHANGELOG](CHANGELOG.md) and make a Pull Request!
