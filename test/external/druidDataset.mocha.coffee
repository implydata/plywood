{ expect } = require("chai")
Q = require('q')

{ WallTime } = require('chronology')
if not WallTime.rules
  tzData = require("chronology/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

plywood = require('../../build/plywood')
{ Expression, Dataset, TimeRange, $ } = plywood

timeFilter = $('time').in(TimeRange.fromJS({
  start: new Date("2013-02-26T00:00:00Z")
  end: new Date("2013-02-27T00:00:00Z")
}))

context = {
  wiki: Dataset.fromJS({
    engine: 'druid',
    dataSource: 'wikipedia_editstream',
    timeAttribute: 'time',
    attributes: {
      time: { type: 'TIME' }
      language: { type: 'STRING' }
      page: { type: 'STRING' }
      added: { type: 'NUMBER' }
      deleted: { type: 'NUMBER' }
    }
    filter: timeFilter
  })
}

contextNoApprox = {
  wiki: Dataset.fromJS({
    engine: 'druid',
    dataSource: 'wikipedia_editstream',
    timeAttribute: 'time',
    exactResultsOnly: true,
    attributes: {
      time: { type: 'TIME' }
      language: { type: 'STRING' }
      page: { type: 'STRING' }
      added: { type: 'NUMBER' }
    }
    filter: timeFilter
  })
}

describe "DruidDataset", ->
  describe "processApply", ->
    it "breaks up correctly in simple case", ->
      ex = $()
        .apply('wiki', '$wiki') # for now
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$wiki.max($added) - $wiki.min($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('literal')
      druidDataset = ex.value

      expect(druidDataset.defs.join('\n')).to.equal("""
        .apply('_sd_0', $wiki:DATASET.max($added:NUMBER))
        .apply('_sd_1', $wiki:DATASET.min($deleted:NUMBER))
        """)

      expect(druidDataset.applies.join('\n')).to.equal("""
        .apply(Count, $wiki:DATASET.count())
        .apply(Added, $wiki:DATASET.sum($added:NUMBER))
        .apply(Volatile, ($_sd_0:NUMBER + $_sd_1:NUMBER.negate()))
        """)

    it "breaks up correctly in case of duplicate name", ->
      ex = $()
        .apply('wiki', '$wiki') # for now
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$wiki.sum($added) - $wiki.sum($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('literal')
      druidDataset = ex.value

      expect(druidDataset.defs.join('\n')).to.equal("""
        .apply('_sd_0', $wiki:DATASET.sum($deleted:NUMBER))
        """)

      expect(druidDataset.applies.join('\n')).to.equal("""
        .apply(Count, $wiki:DATASET.count())
        .apply(Added, $wiki:DATASET.sum($added:NUMBER))
        .apply(Volatile, ($Added:NUMBER + $_sd_0:NUMBER.negate()))
        """)

    it "breaks up correctly in case of variable reference", ->
      ex = $()
        .apply('wiki', '$wiki') # for now
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$Added - $wiki.sum($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('literal')
      druidDataset = ex.value

      expect(druidDataset.defs.join('\n')).to.equal("""
        .apply('_sd_0', $wiki:DATASET.sum($deleted:NUMBER))
        """)

      expect(druidDataset.applies.join('\n')).to.equal("""
        .apply(Count, $wiki:DATASET.count())
        .apply(Added, $wiki:DATASET.sum($added:NUMBER))
        .apply(Volatile, ($Added:NUMBER + $_sd_0:NUMBER.negate()))
        """)

    it "breaks up correctly in case of duplicate apply", ->
      ex = $()
        .apply('wiki', '$wiki') # for now
        .apply('Added', '$wiki.sum($added)')
        .apply('Added2', '$wiki.sum($added)')
        .apply('Volatile', '$Added - $wiki.sum($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('literal')
      druidDataset = ex.value

      expect(druidDataset.defs.join('\n')).to.equal("""
        .apply('_sd_0', $wiki:DATASET.sum($deleted:NUMBER))
        """)

      expect(druidDataset.applies.join('\n')).to.equal("""
        .apply(Added, $wiki:DATASET.sum($added:NUMBER))
        .apply(Added2, $Added:NUMBER)
        .apply(Volatile, ($Added:NUMBER + $_sd_0:NUMBER.negate()))
        """)

    it "breaks up correctly in case of duplicate apply (same name)", ->
      ex = $()
        .apply('wiki', '$wiki') # for now
        .apply('Added', '$wiki.sum($added)')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$Added - $wiki.sum($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('literal')
      druidDataset = ex.value

      expect(druidDataset.defs.join('\n')).to.equal("""
        .apply('_sd_0', $wiki:DATASET.sum($deleted:NUMBER))
        """)

      expect(druidDataset.applies.join('\n')).to.equal("""
        .apply(Added, $wiki:DATASET.sum($added:NUMBER))
        .apply(Volatile, ($Added:NUMBER + $_sd_0:NUMBER.negate()))
        """)


  describe "simplifies / digests", ->
    it "a (timeBoundary) total", ->
      ex = $()
        .apply('maximumTime', '$wiki.max($time)')
        .apply('minimumTime', '$wiki.min($time)')

      ex = ex.referenceCheck(context).resolve(context).simplify()
      expect(ex.op).to.equal('literal')
      remoteDataset = ex.value
      expect(remoteDataset.getQueryAndPostProcess().query).to.deep.equal({
        "dataSource": "wikipedia_editstream"
        "queryType": "timeBoundary"
      })

    it "a total", ->
      ex = $()
        .apply("wiki",
          $('^wiki')
            .apply('addedTwice', '$added * 2')
            .filter($("language").is('en'))
        )
        .apply('Count', '$wiki.count()')
        .apply('TotalAdded', '$wiki.sum($added)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('literal')
      remoteDataset = ex.value
      expect(remoteDataset.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "Count"
            "type": "count"
          }
          {
            "fieldName": "added"
            "name": "TotalAdded"
            "type": "doubleSum"
          }
        ]
        "dataSource": "wikipedia_editstream"
        "filter": {
          "dimension": "language"
          "type": "selector"
          "value": "en"
        }
        "granularity": "all"
        "intervals": [
          "2013-02-26/2013-02-27"
        ]
        "queryType": "timeseries"
      })

    it "inlines a total with a def", ->
      ex = $()
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply('TotalAddedX2', '$TotalAdded * 2')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('literal')
      remoteDataset = ex.value
      queryAndPostProcess = remoteDataset.getQueryAndPostProcess()
      expect(queryAndPostProcess.query).to.deep.equal({
        "aggregations": [
          {
            "fieldName": "added"
            "name": "TotalAdded"
            "type": "doubleSum"
          }
        ]
        "dataSource": "wikipedia_editstream"
        "granularity": "all"
        "intervals": [
          "2013-02-26/2013-02-27"
        ]
        "postAggregations": [
          {
            "fields": [
              {
                "fieldName": "TotalAdded"
                "type": "fieldAccess"
              }
              {
                "type": "constant"
                "value": 2
              }
            ]
            "fn": "*"
            "name": "TotalAddedX2"
            "type": "arithmetic"
          }
        ]
        "queryType": "timeseries"
      })

      expect(queryAndPostProcess.postProcess([
        {
          result: {
            TotalAdded: 5
            TotalAddedX2: 10
          }
        }
      ]).toJS()).to.deep.equal([
        {
          TotalAddedX2: 10
        }
      ])

    it "a split", ->
      ex = $('wiki').split("$page", 'Page')
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5)

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('literal')
      remoteDataset = ex.value
      expect(remoteDataset.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "Count"
            "type": "count"
          }
          {
            "fieldName": "added"
            "name": "Added"
            "type": "doubleSum"
          }
        ]
        "dataSource": "wikipedia_editstream"
        "dimension": {
          "dimension": "page"
          "outputName": "Page"
          "type": "default"
        }
        "granularity": "all"
        "intervals": [
          "2013-02-26/2013-02-27"
        ]
        "metric": "Count"
        "queryType": "topN"
        "threshold": 5
      })

    it "a split (no approximate)", ->
      ex = $('wiki').split("$page", 'Page')
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5)

      ex = ex.referenceCheck(contextNoApprox).resolve(contextNoApprox).simplify()

      expect(ex.op).to.equal('literal')
      remoteDataset = ex.value
      expect(remoteDataset.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "Count"
            "type": "count"
          }
          {
            "fieldName": "added"
            "name": "Added"
            "type": "doubleSum"
          }
        ]
        "dataSource": "wikipedia_editstream"
        "dimensions": [
          {
            "dimension": "page"
            "outputName": "Page"
            "type": "default"
          }
        ]
        "granularity": "all"
        "intervals": [
          "2013-02-26/2013-02-27"
        ]
        "limitSpec": {
          "columns": [
            {
              "dimension": "Count"
              "direction": "descending"
            }
          ]
          "limit": 5
          "type": "default"
        }
        "queryType": "groupBy"
      })

    it "filters", ->
      ex = $()
        .apply("wiki",
          $('^wiki')
            .filter($("language").contains('en'))
        )
        .apply('Count', '$wiki.count()')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('literal')
      remoteDataset = ex.value
      expect(remoteDataset.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "Count"
            "type": "count"
          }
        ]
        "dataSource": "wikipedia_editstream"
        "filter": {
          "dimension": "language"
          "query": {
            "type": "fragment"
            "values": [
              "en"
            ]
          }
          "type": "search"
        }
        "granularity": "all"
        "intervals": [
          "2013-02-26/2013-02-27"
        ]
        "queryType": "timeseries"
      })

  describe "introspects", ->
    requester = ({query}) ->
      expect(query).to.deep.equal({
        "dataSource": "wikipedia_editstream"
        "queryType": "introspect"
      })
      return Q({
        dimensions: ['page', 'language']
        metrics: ['added', 'deleted', 'uniques']
      })

    it "does a simple introspect", (testComplete) ->
      wikiDataset = Dataset.fromJS({
        engine: 'druid',
        dataSource: 'wikipedia_editstream',
        timeAttribute: 'time',
        requester
        filter: timeFilter
      })

      wikiDataset.introspect().then((introspectedDataset) ->
        expect(introspectedDataset.toJS().attributes).to.deep.equal({
          "added": {
            "filterable": false
            "splitable": false
            "type": "NUMBER"
          }
          "deleted": {
            "filterable": false
            "splitable": false
            "type": "NUMBER"
          }
          "language": {
            "type": "STRING"
          }
          "page": {
            "type": "STRING"
          }
          "time": {
            "type": "TIME"
          }
          "uniques": {
            "filterable": false
            "splitable": false
            "type": "NUMBER"
          }
        })
        testComplete()
      ).done()

    it "does an introspect with overrides", (testComplete) ->
      wikiDataset = Dataset.fromJS({
        engine: 'druid',
        dataSource: 'wikipedia_editstream',
        timeAttribute: 'time',
        requester
        filter: timeFilter
        attributeOverrides: {
          uniques: { special: 'unique' }
          histo: { special: 'histogram' }
        }
      })

      wikiDataset.introspect().then((introspectedDataset) ->
        expect(introspectedDataset.toJS().attributes).to.deep.equal({
          "added": {
            "filterable": false
            "splitable": false
            "type": "NUMBER"
          }
          "deleted": {
            "filterable": false
            "splitable": false
            "type": "NUMBER"
          }
          "histo": {
            "special": "histogram"
            "type": "NUMBER"
          }
          "language": {
            "type": "STRING"
          }
          "page": {
            "type": "STRING"
          }
          "time": {
            "type": "TIME"
          }
          "uniques": {
            "special": "unique"
            "type": "STRING"
          }
        })
        testComplete()
      ).done()
