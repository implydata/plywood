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

