# Design Overview
Plywood consists of three logical parts: an expression language for describing data queries, a set of *externals* for
connecting to databases, and a collection of useful helper functions.
 
## Expression Language

At its core Plywood contains an expression language ([DSL](https://en.wikipedia.org/wiki/Domain-specific_language)) that is used to describe data queries.

Here is an example:

```javascript
var ex = ply()
  .apply('Count', $('wiki').count())
  .apply('TotalAdded', '$wiki.sum($added)')
  .apply('Pages',
    $('wiki').split('$page', 'Page')
      .apply('Count', $('wiki').count())
      .sort('$Count', 'descending')
      .limit(6)
  );
```

Plywood's language (called *plywood*, with a lowercase "P") is heavily inspired by
Hadley Wickham's [split-apply-combine](http://vita.had.co.nz/papers/plyr.html) principle and the [D3](http://d3js.org/) API.

Plywood expressions were designed with the following ideas in mind:

- High level - certain key data operations can be expressed with ease.
- Serializable - an expression can be converted to and from plain JSON to be saved in a file or transferred over the network.
- Immutable - inspired by [immutable.js](https://facebook.github.io/immutable-js/), this immutability makes expressions very easy to work with and reason about.
- Parsable - the plywood expression DSL is implemented in JavaScript and as a parser so: `Expression.parse('$wiki.sum($added)').equals($('wiki').sum($('added')))`  
- Smart - expressions can perform complex internal rewriting to facilitate [query simplification](test/overall/simplify.mocha.coffee).  

For more information about expressions check out the [API reference](expressions.md).

## Externals

While Plywood can crunch numbers internally using native JavaScript (this is useful for unit tests) its true utility is
in being able to pass queries to databases.
As of this writing only [Druid](http://druid.io/) and [MySQL](https://www.mysql.com/) externals exist but more will be added.

The externals act as query planners and schedulers for their respective databases.
In the case of the Druid External it also acts as a Polyfill, filling in key missing functionality in the native API. 

Here is an example of a Druid external:

```javascript
External.fromJS({
  engine: 'druid',         
  dataSource: 'wikipedia',  // The datasource name in Druid
  timeAttribute: 'time',    // Druid's anonymous time attribute will be called 'time'
  
  requester: druidRequester // a thing that knows how to make Druid requests
})
```

## Helpers

A varied collection of helper functions are included with Plywood with the idea of making the task of building a query
layer as simple as possible.

One notable example of a helper is the SQL parser which parses [PlyQL](https://github.com/implydata/plyql), 
a SQL-like language, into plywood expressions allowing those to be executed via the Plywood externals. 
This is how Plywood can provide a SQL-like interface to Druid.
