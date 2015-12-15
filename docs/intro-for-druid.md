# Plywood <3 Druid

This section is devoted to explaining Plywood with the assumption you are coming from the Druid world.
A familiarity with the Druid query language is expected here.

There are many Druid [libraries](http://druid.io/docs/0.6.171/Libraries.html) out there but they are 1-1 wrappers of
the Druid API. They save you the hustle of writing JSON but you are still fundamentally constrained to the
expressiveness of the Druid query language.


## Basic Druid queries expressed in plywood

Here are plywood queries the would translate directly to single Druid queries.

### TimeBoundary query

*ToDo: add description*

```javascript
ply()
  .apply('maxTime', '$wiki.max($timestamp)')
  .apply('minTime', '$wiki.min($timestamp)')
```

### Timeseries query

*ToDo: add description*

```javascript
ply()
  .apply('TimeByHour'
    $('wiki').split($('timestamp').numberBucket('PT1H', 'Etc/UTC'), "Time")
      .apply('Count', '$wiki.count()')
      .apply('Added', '$wiki.sum($added)')
      .sort('Time', 'ascending')
  )
```

### TopN query

*ToDo: add description*

```javascript
ply()
  .apply('Pages'
    $('wiki').split('$page', "Page")
      .apply('Count', '$wiki.count()')
      .apply('Added', '$wiki.sum($added)')
      .sort('Count', 'descending')
      .limit(10)
  )
```
