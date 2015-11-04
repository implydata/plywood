# Plywood

An ORM-like framework for OLAP.


## Introduction

Plywood tries to simplify the task of building powerful, data driven interfaces and visualizations around OLAP databases.


## Installation

To use Plywood from npm simply run: `npm install plywood`.

Plywood can be also used by the browser like [so](TODO:example).

## Overview

Plywood consists of three logical parts: an expression language for describing data queries, a set of *externals* for
connecting to databases, and a collection of useful helper functions.
 
### Expression Language

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
- Smart - expressions can perform complex internal rewriting to facilitate [query simplification](https://github.com/implydata/plywood/blob/master/test/overall/simplify.mocha.coffee).  

For more information about expressions check out the API reference.

### Externals

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

### Helpers

A varied collection of helper functions are included with Plywood with the idea of making the task of building a query
layer as simple as possible.

One notable example of a helper is the SQL parser which parses [PlyQL](https://github.com/implydata/plyql), 
a SQL-like language, into plywood expressions allowing those to be executed via the Plywood externals. 
This is how Plywood can provide a SQL-like interface to Druid.


## Learn by Example

### Example 0

Here is an example of a simple plywood query that illustrates the different ways by which expressions can be created:

```javascript
var ex0 = ply() // Create an empty singleton dataset literal [{}]
  // 1 is converted into a literal
  .apply("one", 1)

  // The string "$one + 1" is parsed into an expression
  .apply("two", "$one + 1")

  // The method chaining approach is used to make an expression
  .apply("four", $("two").multiply(2))
```

Calling ```ex0.compute()``` will return a [Q](https://github.com/kriskowal/q) promise that will resolve to:

```javascript
[
  {
    one: 1
    two: 2
    four: 4
  }
]
```

This example employs three functions:

* `ply()` creates a dataset with one empty datum inside of it. This is the base of many plywood operations.

* `apply(name, expression)` evaluates the given `expression` for every element of the dataset and saves the result as `name`.


### Example 1

First of all plywood and its component parts need to be imported into the project.
This example will use Druid as the data store:

```javascript
// Get the druid requester (which is a node specific module)
var druidRequesterFactory = require('plywoodjs-druid-requester').druidRequesterFactory;

var plywood = require('plywood');
var Dataset = plywood.Dataset;
```

Next, the druid connection needs to be configured:

```javascript
var druidRequester = druidRequesterFactory({
  host: '10.153.211.100' // Where ever your Druid may be
});

var wikiDataset = Dataset.fromJS({
  source: 'druid',
  dataSource: 'wikipedia',  // The datasource name in Druid
  timeAttribute: 'time',  // Druid's anonymous time attribute will be called 'time'
  requester: druidRequester
});
```

Once that is up and running a simple query can be issued:

```javascript
var context = {
  wiki: wikiDataset
};

var ex = ply()
  // Define the dataset in context with a filter on time and language
  .apply("wiki",
    $('wiki').filter($("time").in({
      start: new Date("2015-08-26T00:00:00Z"),
      end: new Date("2015-08-27T00:00:00Z")
    }).and($('language').is('en')))
  )

  // Calculate count
  .apply('Count', $('wiki').count())

  // Calculate the sum of the `added` attribute
  .apply('TotalAdded', '$wiki.sum($added)');

ex.compute(context).then(function(data) {
  // Log the data while converting it to a readable standard
  console.log(JSON.stringify(data.toJS(), null, 2));
}).done();
```

This will output:

```javascript
[
  {
    "Count": 308675,
    "TotalAdded": 41412583
  }
]
```

A dataset with a single datum in it.
The attribute of this datum will be the `.apply` calls that we asked Druid to calculate.

This might not look mind blowing but we can build on this concept.

### Example 2

Using the same setup as before we can issue a more interesting query:

```javascript
var context = {
  wiki: wikiDataset
};

var ex = ply()
  .apply("wiki",
    $('wiki').filter($("time").in({
      start: new Date("2015-08-26T00:00:00Z"),
      end: new Date("2015-08-27T00:00:00Z")
    }))
  )
  .apply('Count', $('wiki').count())
  .apply('TotalAdded', '$wiki.sum($added)')
  .apply('Pages',
    $('wiki').split('$page', 'Page')
      .apply('Count', $('wiki').count())
      .sort('$Count', 'descending')
      .limit(6)
  );

ex.compute(context).then(function(data) {
  // Log the data while converting it to a readable standard
  console.log(JSON.stringify(data.toJS(), null, 2));
}).done();
```

Here a sub split is added. The `Pages` attribute will actually be a dataset that represents the data in `wiki`
split on the `page` attribute (labeled as `'Page'`) and then the top 6 pages will be taken by applying a sort
and limit.

The output will look like so:

```javascript
[
  {
    "Count": 573775,
    "TotalAdded": 124184252,
    "Page": [
      {
        "Page": "Wikipedia:Vandalismusmeldung",
        "Count": 177
      },
      {
        "Page": "Wikipedia:Administrator_intervention_against_vandalism",
        "Count": 124
      },
      {
        "Page": "Wikipedia:Auskunft",
        "Count": 124
      },
      {
        "Page": "Wikipedia:Löschkandidaten/26._Februar_2013",
        "Count": 88
      },
      {
        "Page": "Wikipedia:Reference_desk/Science",
        "Count": 88
      },
      {
        "Page": "Wikipedia:Administrators'_noticeboard",
        "Count": 87
      }
    ]
  }
]
```

## Videos

Vadim Ogievetsky talks about the [inspiration behind Plywood](https://www.youtube.com/watch?v=JNMbLxqzGFA)

## Questions & Support

Please direct all questions to our [user groups](https://groups.google.com/forum/#!forum/imply-user-group).

You might want to have a look at [Pivot](https://github.com/implydata/pivot), a GUI for big data exploration being built on top of Plywood. 
