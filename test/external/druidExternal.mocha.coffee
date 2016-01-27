{ expect } = require("chai")
Q = require('q')

{ WallTime } = require('chronoshift')
if not WallTime.rules
  tzData = require("chronoshift/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

plywood = require('../../build/plywood')
{ Expression, External, TimeRange, $, ply, r } = plywood

timeFilter = $('time').in(TimeRange.fromJS({
  start: new Date("2013-02-26T00:00:00Z")
  end: new Date("2013-02-27T00:00:00Z")
}))

context = {
  wiki: External.fromJS({
    engine: 'druid',
    dataSource: 'wikipedia',
    timeAttribute: 'time',
    attributes: [
      { name: 'time', type: 'TIME' }
      { name: 'language', type: 'STRING' }
      { name: 'page', type: 'STRING' }
      { name: 'added', type: 'NUMBER' }
      { name: 'deleted', type: 'NUMBER' }
      { name: 'inserted', type: 'NUMBER' }
    ]
    filter: timeFilter,
    customAggregations: {
      crazy: {
        accessType: 'getSomeCrazy'
        aggregation: {
          type: 'crazy'
          the: 'borg will rise again'
          activate: false
        }
      }
      stupid: {
        accessType: 'iAmWithStupid'
        aggregation: {
          type: 'stoopid'
          onePlusOne: 3
          globalWarming: 'hoax'
        }
      }
    }
  })
}

contextNoApprox = {
  wiki: External.fromJS({
    engine: 'druid',
    dataSource: 'wikipedia',
    timeAttribute: 'time',
    exactResultsOnly: true,
    attributes: [
      { name: 'time', type: 'TIME' }
      { name: 'language', type: 'STRING' }
      { name: 'page', type: 'STRING' }
      { name: 'added', type: 'NUMBER' }
      { name: 'deleted', type: 'NUMBER' }
      { name: 'inserted', type: 'NUMBER' }
    ]
    filter: timeFilter
  })
}

describe "DruidExternal", ->
  describe "processApply", ->
    it "breaks up correctly in simple case", ->
      ex = ply()
        .apply('wiki', '$wiki') # for now
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$wiki.max($added) - $wiki.min($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external

      expect(druidExternal.applies.join('\n')).to.equal("""
        apply(Count,$wiki:DATASET.count())
        apply(Added,$wiki:DATASET.sum($added:NUMBER))
        apply(_sd_0,$wiki:DATASET.max($added:NUMBER))
        apply(_sd_1,$wiki:DATASET.min($deleted:NUMBER))
        apply(Volatile,$_sd_0:NUMBER.subtract($_sd_1:NUMBER))
        """)

    it "breaks up correctly in case of duplicate name", ->
      ex = ply()
        .apply('wiki', '$wiki') # for now
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$wiki.sum($added) - $wiki.sum($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external

      expect(druidExternal.applies.join('\n')).to.equal("""
        apply(Count,$wiki:DATASET.count())
        apply(Added,$wiki:DATASET.sum($added:NUMBER))
        apply(_sd_0,$wiki:DATASET.sum($deleted:NUMBER))
        apply(Volatile,$Added:NUMBER.subtract($_sd_0:NUMBER))
        """)

    it "breaks up correctly in case of variable reference", ->
      ex = ply()
        .apply('wiki', '$wiki') # for now
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$Added - $wiki.sum($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external

      expect(druidExternal.applies.join('\n')).to.equal("""
        apply(Count,$wiki:DATASET.count())
        apply(Added,$wiki:DATASET.sum($added:NUMBER))
        apply(_sd_0,$wiki:DATASET.sum($deleted:NUMBER))
        apply(Volatile,$Added:NUMBER.subtract($_sd_0:NUMBER))
        """)

    it "breaks up correctly in complex case", ->
      ex = ply()
        .apply('wiki', '$wiki') # for now
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)')
        .apply('Deleted', '$wiki.sum($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external

      expect(druidExternal.applies.join('\n')).to.equal("""
        apply(Deleted,$wiki:DATASET.sum($deleted:NUMBER))
        apply(_sd_0,$wiki:DATASET.sum($added:NUMBER))
        apply(AddedByDeleted,$_sd_0:NUMBER.divide($Deleted:NUMBER))
        apply(_sd_1,$wiki:DATASET.sum($inserted:NUMBER))
        apply(DeletedByInserted,$Deleted:NUMBER.divide($_sd_1:NUMBER))
        """)

    it.skip "breaks up correctly in case of duplicate apply", ->
      ex = ply()
        .apply('wiki', '$wiki') # for now
        .apply('Added', '$wiki.sum($added)')
        .apply('Added2', '$wiki.sum($added)')
        .apply('Volatile', '$Added - $wiki.sum($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external

      expect(druidExternal.applies.join('\n')).to.equal("""
        apply(Added,$wiki:DATASET.sum($added:NUMBER))
        apply(Added2,$Added:NUMBER)
        apply(_sd_0,$wiki:DATASET.sum($deleted:NUMBER))
        apply(Volatile,$Added:NUMBER.subtract($_sd_0:NUMBER))
        """)

    it.skip "breaks up correctly in case of duplicate apply (same name)", ->
      ex = ply()
        .apply('wiki', '$wiki') # for now
        .apply('Added', '$wiki.sum($added)')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$Added - $wiki.sum($deleted)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external

      expect(druidExternal.defs.join('\n')).to.equal("""
        .apply('_sd_0',$wiki:DATASET.sum($deleted:NUMBER))
        """)

      expect(druidExternal.applies.join('\n')).to.equal("""
        .apply(Added,$wiki:DATASET.sum($added:NUMBER))
        .apply(Volatile,$Added:NUMBER.add($_sd_0:NUMBER.negate()))
        """)


  describe "simplifies / digests", ->
    it "a (timeBoundary) total", ->
      ex = ply()
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
      ex = ply()
        .apply("wiki",
          $('wiki', 1)
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
      ex = ply()
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

    it "a split with custom aggregations", ->
      ex = $('wiki').split("$page", 'Page')
        .apply('CrazyStupid', '$wiki.custom(crazy) * $wiki.custom(stupid)')
        .sort('$CrazyStupid', 'descending')
        .limit(5)

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('external')
      druidExternal = ex.external
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "activate": false
            "name": "_sd_0"
            "the": "borg will rise again"
            "type": "crazy"
          }
          {
            "globalWarming": "hoax"
            "name": "_sd_1"
            "onePlusOne": 3
            "type": "stoopid"
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
        "metric": "CrazyStupid"
        "postAggregations": [
          {
            "fields": [
              {
                "fieldName": "_sd_0"
                "type": "getSomeCrazy"
              }
              {
                "fieldName": "_sd_1"
                "type": "iAmWithStupid"
              }
            ]
            "fn": "*"
            "name": "CrazyStupid"
            "type": "arithmetic"
          }
        ]
        "queryType": "topN"
        "threshold": 5
      })

    it "filters (in)", ->
      ex = ply()
        .apply("wiki",
          $('wiki', 1)
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

    it "filters (in [null])", ->
      ex = ply()
        .apply("wiki",
          $('wiki', 1)
            .filter($("language").in([null]))
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
          "value": null
        }
        "granularity": "all"
        "intervals": [
          "2013-02-26/2013-02-27"
        ]
        "queryType": "timeseries"
      })

    it "filters (contains)", ->
      ex = ply()
        .apply("wiki",
          $('wiki', 1)
            .filter($("language").contains('en', 'ignoreCase'))
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

  describe "should work when getting back [] and [{result:[]}]", ->
    nullExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      requester: (query) -> Q([]),
      attributes: [
        { name: 'time', type: 'TIME' }
        { name: 'language', type: 'STRING' }
        { name: 'page', type: 'STRING' }
        { name: 'added', type: 'NUMBER' }
      ]
      filter: timeFilter
    })

    emptyExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      requester: (query) -> Q([{result:[]}]),
      attributes: [
        { name: 'time', type: 'TIME' }
        { name: 'language', type: 'STRING' }
        { name: 'page', type: 'STRING' }
        { name: 'added', type: 'NUMBER' }
      ]
      filter: timeFilter
    })

    describe "should return null correctly on a totals query", ->
      ex = ply()
        .apply('wiki', '$wiki') # for now
        .apply('Count', '$wiki.count()')

      it "should work with [] return", (testComplete) ->
        ex.compute({ wiki: nullExternal }).then((result) ->
          expect(result.toJS()).to.deep.equal([
            { Count: 0 }
          ])
          testComplete()
        ).done()

    describe "should return null correctly on a timeseries query", ->
      ex = $('wiki').split("$time.timeBucket(P1D, 'Etc/UTC')", 'Time')
        .apply('Count', '$wiki.count()')
        .sort('$Time', 'ascending')

      it "should work with [] return", (testComplete) ->
        ex.compute({ wiki: nullExternal }).then((result) ->
          expect(result.toJS()).to.deep.equal([])
          testComplete()
        ).done()

    describe "should return null correctly on a topN query", ->
      ex = $('wiki').split("$page", 'Page')
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5)

      it "should work with [] return", (testComplete) ->
        ex.compute({ wiki: nullExternal }).then((result) ->
          expect(result.toJS()).to.deep.equal([])
          testComplete()
        ).done()

      it "should work with [{result:[]}] return", (testComplete) ->
        ex.compute({ wiki: emptyExternal }).then((result) ->
          expect(result.toJS()).to.deep.equal([])
          testComplete()
        ).done()


  describe "should work when getting back crap data", ->
    crapExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      requester: (query) -> Q("[Does this look like data to you?")
      attributes: [
        { name: 'time', type: 'TIME' }
        { name: 'language', type: 'STRING' }
        { name: 'page', type: 'STRING' }
        { name: 'added', type: 'NUMBER' }
      ]
      filter: timeFilter
    })

    it "should work with all query", (testComplete) ->
      ex = ply()
        .apply('wiki', '$wiki') # for now
        .apply('Count', '$wiki.count()')

      ex.compute({ wiki: crapExternal })
        .then(-> throw new Error('DID_NOT_ERROR'))
        .fail((err) ->
          expect(err.message).to.equal('unexpected result from Druid (all)')
          testComplete()
        ).done()

    it "should work with timeseries query", (testComplete) ->
      ex = $('wiki').split("$time.timeBucket(P1D, 'Etc/UTC')", 'Time')
        .apply('Count', '$wiki.count()')
        .sort('$Time', 'ascending')

      ex.compute({ wiki: crapExternal })
        .then(-> throw new Error('DID_NOT_ERROR'))
        .fail((err) ->
          expect(err.message).to.equal('unexpected result from Druid (timeseries)')
          testComplete()
        ).done()
