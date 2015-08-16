{ expect } = require("chai")
Q = require('q')

{ WallTime } = require('chronology')
if not WallTime.rules
  tzData = require("chronology/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

plywood = require('../../build/plywood')
{ Expression, External, TimeRange, $ } = plywood

timeFilter = $('time').in(TimeRange.fromJS({
  start: new Date("2013-02-26T00:00:00Z")
  end: new Date("2013-02-27T00:00:00Z")
}))

context = {
  wiki: External.fromJS({
    engine: 'druid',
    dataSource: 'wikipedia',
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
  wiki: External.fromJS({
    engine: 'druid',
    dataSource: 'wikipedia',
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

describe "DruidExternal", ->
  describe "processApply", ->
    it "breaks up correctly in simple case", ->
      ex = $()
        .apply('wiki', '$wiki') # for now
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$wiki.max($added) - $wiki.min($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external

      expect(druidExternal.applies.join('\n')).to.equal("""
        apply(Count, $wiki:DATASET.count())
        apply(Added, $wiki:DATASET.sum($added:NUMBER))
        apply(_sd_0, $wiki:DATASET.max($added:NUMBER))
        apply(_sd_1, $wiki:DATASET.min($deleted:NUMBER))
        apply(Volatile, $_sd_0:NUMBER.subtract($_sd_1:NUMBER))
        """)

    it "breaks up correctly in case of duplicate name", ->
      ex = $()
        .apply('wiki', '$wiki') # for now
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$wiki.sum($added) - $wiki.sum($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external

      expect(druidExternal.applies.join('\n')).to.equal("""
        apply(Count, $wiki:DATASET.count())
        apply(Added, $wiki:DATASET.sum($added:NUMBER))
        apply(_sd_0, $wiki:DATASET.sum($deleted:NUMBER))
        apply(Volatile, $Added:NUMBER.subtract($_sd_0:NUMBER))
        """)

    it "breaks up correctly in case of variable reference", ->
      ex = $()
        .apply('wiki', '$wiki') # for now
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$Added - $wiki.sum($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external

      expect(druidExternal.applies.join('\n')).to.equal("""
        apply(Count, $wiki:DATASET.count())
        apply(Added, $wiki:DATASET.sum($added:NUMBER))
        apply(_sd_0, $wiki:DATASET.sum($deleted:NUMBER))
        apply(Volatile, $Added:NUMBER.subtract($_sd_0:NUMBER))
        """)

    it.skip "breaks up correctly in case of duplicate apply", ->
      ex = $()
        .apply('wiki', '$wiki') # for now
        .apply('Added', '$wiki.sum($added)')
        .apply('Added2', '$wiki.sum($added)')
        .apply('Volatile', '$Added - $wiki.sum($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external

      expect(druidExternal.applies.join('\n')).to.equal("""
        apply(Added, $wiki:DATASET.sum($added:NUMBER))
        apply(Added2, $Added:NUMBER)
        apply(_sd_0, $wiki:DATASET.sum($deleted:NUMBER))
        apply(Volatile, $Added:NUMBER.subtract($_sd_0:NUMBER))
        """)

    it.skip "breaks up correctly in case of duplicate apply (same name)", ->
      ex = $()
        .apply('wiki', '$wiki') # for now
        .apply('Added', '$wiki.sum($added)')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$Added - $wiki.sum($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external

      expect(druidExternal.defs.join('\n')).to.equal("""
        .apply('_sd_0', $wiki:DATASET.sum($deleted:NUMBER))
        """)

      expect(druidExternal.applies.join('\n')).to.equal("""
        .apply(Added, $wiki:DATASET.sum($added:NUMBER))
        .apply(Volatile, ($Added:NUMBER + $_sd_0:NUMBER.negate()))
        """)


  describe "simplifies / digests", ->
    it "a (timeBoundary) total", ->
      ex = $()
        .apply('maximumTime', '$wiki.max($time)')
        .apply('minimumTime', '$wiki.min($time)')

      ex = ex.referenceCheck(context).resolve(context).simplify()
      expect(ex.op).to.equal('external')
      druidExternal = ex.external
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "dataSource": "wikipedia"
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

      expect(ex.op).to.equal('external')
      druidExternal = ex.external
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
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
        "dataSource": "wikipedia"
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

    it "inlines a total with no explicit dataset apply", ->
      ex = $()
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply('TotalAddedX2', '$TotalAdded * 2')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external
      queryAndPostProcess = druidExternal.getQueryAndPostProcess()
      expect(queryAndPostProcess.query).to.deep.equal({
        "aggregations": [
          {
            "fieldName": "added"
            "name": "TotalAdded"
            "type": "doubleSum"
          }
        ]
        "dataSource": "wikipedia"
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
          TotalAdded: 5
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

      expect(ex.op).to.equal('external')
      druidExternal = ex.external
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
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
        "dataSource": "wikipedia"
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

      expect(ex.op).to.equal('external')
      druidExternal = ex.external
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
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
        "dataSource": "wikipedia"
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

    it "filters (in)", ->
      ex = $()
        .apply("wiki",
          $('^wiki')
            .filter($("language").in(['en']))
        )
        .apply('Count', '$wiki.count()')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "Count"
            "type": "count"
          }
        ]
        "dataSource": "wikipedia"
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

    it "filters (contains)", ->
      ex = $()
        .apply("wiki",
          $('^wiki')
            .filter($("language").contains('en'))
        )
        .apply('Count', '$wiki.count()')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "Count"
            "type": "count"
          }
        ]
        "dataSource": "wikipedia"
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
        "dataSource": "wikipedia"
        "queryType": "introspect"
      })
      return Q({
        dimensions: ['page', 'language']
        metrics: ['added', 'deleted', 'uniques']
      })

    it "does a simple introspect", (testComplete) ->
      wikiExternal = External.fromJS({
        engine: 'druid',
        dataSource: 'wikipedia',
        timeAttribute: 'time',
        requester
        filter: timeFilter
      })

      wikiExternal.introspect().then((introspectedExternal) ->
        expect(introspectedExternal.toJS().attributes).to.deep.equal({
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
      wikiExternal = External.fromJS({
        engine: 'druid',
        dataSource: 'wikipedia',
        timeAttribute: 'time',
        requester
        filter: timeFilter
        attributeOverrides: {
          uniques: { special: 'unique' }
          histo: { special: 'histogram' }
        }
      })

      wikiExternal.introspect().then((introspectedExternal) ->
        expect(introspectedExternal.toJS().attributes).to.deep.equal({
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
