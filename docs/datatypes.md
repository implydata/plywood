# Datatypes

There are several core datatypes that exist within plywood.

The datatypes are represented as both the return values of expressions and as native objects.

## NULL

Represents the special type of null. It is a subtype of every type.

Native example: `null`

## BOOLEAN

Represents the true / false states often used for filtering.

Native example: `true`

## NUMBER

Represents numeric attributes.

Native example: `5`

## NUMBER_RANGE

Represents an interval of numbers, often the result of a bucketing.

Native example: `NumberRange.fromJS({ start: 4, end: 7.5 })`

## TIME

Represents time based attributes

Native example: `new Date('2015-02-24T18:00:00')`

## TIME_RANGE

Represents an interval of time, often the result of a bucketing.

Native example: `TimeRange.fromJS({ start: new Date('2015-02-24T18:00:00'), end: new Date('2015-02-24T19:00:00') })`

## STRING

Represents a categorical attribute.

Native example: `'USA'`

## SET/*

Represents a set of distinct elements. The elements within a set must all have the same type.

The type of a set of a certain type is represented as `SET/X` where X is the type of the elements.
For example `SET/NUMBER`

Native example: `Set.fromJS(['USA', 'UK', 'Japan'])`

## DATASET

Represents a (potentially ordered) collection of datums.
Each datum being a collection of attributes, each having one of the above types.

A dataset is the abstract representation of a table in a SQL database or a dataSource in Druid.

Native example:

```javascript
Dataset.fromJS([
  { x: 1, y: "USA" }
  { x: 2, y: "UK" }
])
```
