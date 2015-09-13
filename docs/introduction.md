# plywood

## Introduction

plywood is a framework for expressing data queries optimized for visualizations.
plywood places the end user first by facilitating a rich query language that gets translated to an underlying database.

## Philosophy

plywood was built with these goals in mind:

### Higher-level language

A high-level *domain specific language* is employed to describe the plywood API.
This language is inspired by Hadley Wickham's [split-apply-combine](http://www.jstatsoft.org/v40/i01/paper) principle,
and by [jq](https://stedolan.github.io/jq/).

### Higher-level objects

A number of core datatypes are provided to make life easy.

### Serializability

plywood queries and visualizations can be serialized to and from JSON.

## Querying

Making a query in plywood consists of creating an expression and then evaluating it.

There are a number of ways to create expressions:

- by using the ```ply()``` helper method
- by parsing an expression string using the built-in parser
- by composing them manually using the Expression sub-class objects
- by constructing the appropriate JSON and then deserializing it into an Expression

Expressions are joined together to create powerful queries.
These queries, which can be computed on any supported database, are executed by calling ```.compute()```.

Learn more about [expressions here](./expressions.md).

## Datasets

The backbone datatype in plywood is the dataset.

A dataset is a potentially ordered collection of datums.

A datum is a collection of named attributes where the name is a string and the value any one of a number of things.

Most notably a dataset can be the value of a datum in another dataset.

In the SQL world, a *dataset* can be though as a **table** and an *attribute* as a **column**. SQL can use a *foreign-key relation* to express a table as a value
in another table, but it can not easily return the data in that format.

In the Druid world, a *dataset* is a **datasource** and an *attribute* is a **field**.

Lear more about [data types here](./datatypes.md).

## Learn by example

### Example 0

Here is an example of a simple plywood query that illustrates the different ways by which expressions can be created:

```javascript
var ex0 = ply() // Create an empty singleton dataset literal [{}]
  // 1 is converted into a literal
  .apply("one", 1)

  // 2 is converted into a literal via the ply() function
  .apply("two", $(2))

  // The string "$one + $two" is parsed into an expression
  .apply("three", "$one + $two")

  // The method chaining approach is used to make an expression
  .apply("four", $("three").add(1))

  // Simple JSON of an expression is used to define an expression
  .apply("five", {
    op: 'add'
    operands: [
      { op: 'ref', name: 'four' }
      { op: 'literal', value: 1 }
    ]
  })

  // Same as before except individual expression sub-components are parsed
  .apply("six", { op: 'add', operands: ['$five', 1] })
```

This query shows off the different ways of creating an expression.

Calling ```ex0.compute()``` will return a Q promise that will resolve to:

```javascript
[
  {
    three: 3
    four: 4
    five: 5
    six: 6
  }
]
```

This example employees three functions:

* `ply()` creates a dataset with one empty datum inside of it. This is the base of most plywood operations.

* `apply(name, expression)` evaluates the given `expression` for every element of the dataset and saves the result as `name`.

* `def(name, expression)` is essentially the same as `apply` except that the result will not show up in the output.
This can be used for temporary computations.


### Example 1

First of all plywood and its component parts need to be imported into the project.
This example will use Druid as the data store.

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

  // Calculate the total of the `added` attribute
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

This is a simple manifestation of a 'group by' like query.

### Example 3

This concept can be nested to produce more and more advanced analysis.

ToDo: fill in ASAP (Feb 25, 2015)
