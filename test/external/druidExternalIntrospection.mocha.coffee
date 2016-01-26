{ expect } = require("chai")
Q = require('q')

{ WallTime } = require('chronoshift')
if not WallTime.rules
  tzData = require("chronoshift/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

plywood = require('../../build/plywood')
{ Expression, External, TimeRange, $, ply, r } = plywood

describe "DruidExternal Introspection", ->

  requesterFail = ({query}) ->
    return Q.reject(new Error('Bad status code'))

  requesterDruid_0_9_0 = ({query}) ->
    expect(query.queryType).to.equal('segmentMetadata')
    expect(query.dataSource).to.equal('wikipedia')
    expect(query.merge).to.equal(true)
    expect(query.analysisTypes).to.be.an('array')
    expect(query.lenientAggregatorMerge).to.equal(true)

    merged = {
      "id": "merged",
      "intervals": null,
      "size": 0,
      "numRows": 654321,
      "columns": {
        "__time": { "type": "LONG", "hasMultipleValues": false, "size": 0, "cardinality": null, "errorMessage": null },
        "added": { "type": "FLOAT", "hasMultipleValues": false, "size": 0, "cardinality": null, "errorMessage": null },
        "anonymous": { "type": "STRING", "hasMultipleValues": false, "size": 0, "cardinality": 0, "errorMessage": null },
        "count": { "type": "LONG", "hasMultipleValues": false, "size": 0, "cardinality": null, "errorMessage": null },
        "delta_hist": { "type": "approximateHistogram", "hasMultipleValues": false, "size": 0, "cardinality": null, "errorMessage": null },
        "language": { "type": "STRING", "hasMultipleValues": false, "size": 0, "cardinality": 0, "errorMessage": null },
        "namespace": { "type": "STRING", "hasMultipleValues": true, "size": 0, "cardinality": 0, "errorMessage": null },
        "newPage": { "type": "STRING", "hasMultipleValues": false, "size": 0, "cardinality": 0, "errorMessage": "lol wtf" },
        "page": { "type": "STRING", "hasMultipleValues": false, "size": 0, "cardinality": 0, "errorMessage": null },
        "time": { "type": "STRING", "hasMultipleValues": false, "size": 0, "cardinality": 0, "errorMessage": null },
        "user_unique": { "type": "hyperUnique", "hasMultipleValues": false, "size": 0, "cardinality": null, "errorMessage": null }
      }
    }

    if query.analysisTypes.indexOf("aggregators") isnt -1
      merged.aggregators = {
        "added": { "type": "doubleSum", "name": "added", "fieldName": "added" },
        "count": { "type": "longSum", "name": "count", "fieldName": "count" },
        "delta": { "type": "doubleSum", "name": "delta", "fieldName": "delta" },
        "delta_hist": { "type": "approxHistogramFold", "name": "delta_hist", "fieldName": "delta_hist", "resolution": 50, "numBuckets": 7, "lowerLimit": "-Infinity", "upperLimit": "Infinity" },
        "user_unique": { "type": "hyperUnique", "name": "user_unique", "fieldName": "user_unique" }
      }

    return Q([merged])

  requesterDruid_0_8_3 = ({query}) ->
    expect(query.queryType).to.equal('segmentMetadata')
    expect(query.dataSource).to.equal('wikipedia')
    expect(query.merge).to.equal(true)
    expect(query.analysisTypes).to.be.an('array')
    expect(query.lenientAggregatorMerge).to.equal(true)

    merged = {
      "id": "merged",
      "intervals": null,
      "size": 0,
      "numRows": 654321,
      "columns": {
        "__time": { "type": "LONG", "hasMultipleValues": false, "size": 0, "cardinality": null, "errorMessage": null },
        "added": { "type": "FLOAT", "hasMultipleValues": false, "size": 0, "cardinality": null, "errorMessage": null },
        "anonymous": { "type": "STRING", "hasMultipleValues": false, "size": 0, "cardinality": 0, "errorMessage": null },
        "count": { "type": "LONG", "hasMultipleValues": false, "size": 0, "cardinality": null, "errorMessage": null },
        "delta_hist": { "type": "approximateHistogram", "hasMultipleValues": false, "size": 0, "cardinality": null, "errorMessage": null },
        "language": { "type": "STRING", "hasMultipleValues": false, "size": 0, "cardinality": 0, "errorMessage": null },
        "namespace": { "type": "STRING", "hasMultipleValues": true, "size": 0, "cardinality": 0, "errorMessage": null },
        "newPage": { "type": "STRING", "hasMultipleValues": false, "size": 0, "cardinality": 0, "errorMessage": "lol wtf" },
        "page": { "type": "STRING", "hasMultipleValues": false, "size": 0, "cardinality": 0, "errorMessage": null },
        "time": { "type": "STRING", "hasMultipleValues": false, "size": 0, "cardinality": 0, "errorMessage": null },
        "user_unique": { "type": "hyperUnique", "hasMultipleValues": false, "size": 0, "cardinality": null, "errorMessage": null }
      }
    }

    if query.analysisTypes.indexOf("aggregators") isnt -1
      return Q.reject(new Error('Can not construct instance of io.druid.query.metadata.metadata.SegmentMetadataQuery$AnalysisType'))

    return Q([merged])

  requesterDruid_0_8_0 = ({query}) ->
    if query.queryType is 'segmentMetadata'
      return Q.reject(new Error('segmentMetadata failed'))

    expect(query).to.deep.equal({
      "dataSource": "wikipedia"
      "queryType": "introspect"
    })
    return Q({
      dimensions: ['anonymous', 'language', 'namespace', 'newPage', 'page', 'time']
      metrics: ['added', 'count', 'delta_hist', 'user_unique']
    })


  it "does an introspect with general failure", (testComplete) ->
    wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      requester: requesterFail
    })

    wikiExternal.introspect()
      .then(-> throw new Error('DID_NOT_ERROR'))
      .fail((err) ->
        expect(err.message).to.equal('Bad status code')
        testComplete()
      ).done()

  it "does an introspect with segmentMetadata (with aggregators)", (testComplete) ->
    wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      requester: requesterDruid_0_9_0
    })

    wikiExternal.introspect().then((introspectedExternal) ->
      expect(introspectedExternal.toJS().attributes).to.deep.equal([
        {
          "name": "time"
          "type": "TIME"
        }
        {
          "makerAction": {
            "action": "sum"
            "expression": {
              "name": "added"
              "op": "ref"
            }
          }
          "name": "added"
          "type": "NUMBER"
          "unsplitable": true
        }
        {
          "name": "anonymous"
          "type": "STRING"
        }
        {
          "makerAction": {
            "action": "sum"
            "expression": {
              "name": "count"
              "op": "ref"
            }
          }
          "name": "count"
          "type": "NUMBER"
          "unsplitable": true
        }
        {
          "name": "delta_hist"
          "special": "histogram"
          "type": "NUMBER"
        }
        {
          "name": "language"
          "type": "STRING"
        }
        {
          "name": "namespace"
          "type": "SET/STRING"
        }
        {
          "name": "page"
          "type": "STRING"
        }
        {
          "name": "user_unique"
          "special": "unique"
          "type": "STRING"
        }
      ])
      testComplete()
    ).done()

  it "does an introspect with segmentMetadata (without aggregators)", (testComplete) ->
    wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      requester: requesterDruid_0_8_3
    })

    wikiExternal.introspect().then((introspectedExternal) ->
      expect(introspectedExternal.toJS().attributes).to.deep.equal([
        {
          "name": "time"
          "type": "TIME"
        }
        {
          "name": "added"
          "type": "NUMBER"
          "unsplitable": true
        }
        {
          "name": "anonymous"
          "type": "STRING"
        }
        {
          "name": "count"
          "type": "NUMBER"
          "unsplitable": true
        }
        {
          "name": "delta_hist"
          "special": "histogram"
          "type": "NUMBER"
        }
        {
          "name": "language"
          "type": "STRING"
        }
        {
          "name": "namespace"
          "type": "SET/STRING"
        }
        {
          "name": "page"
          "type": "STRING"
        }
        {
          "name": "user_unique"
          "special": "unique"
          "type": "STRING"
        }
      ])
      testComplete()
    ).done()

  it "does a simple introspect with GET", (testComplete) ->
    wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      requester: requesterDruid_0_8_0
    })

    wikiExternal.introspect().then((introspectedExternal) ->
      expect(introspectedExternal.toJS().attributes).to.deep.equal([
        {
          "name": "time"
          "type": "TIME"
        }
        {
          "name": "anonymous"
          "type": "STRING"
        }
        {
          "name": "language"
          "type": "STRING"
        }
        {
          "name": "namespace"
          "type": "STRING"
        }
        {
          "name": "newPage"
          "type": "STRING"
        }
        {
          "name": "page"
          "type": "STRING"
        }
        {
          "name": "added"
          "type": "NUMBER"
          "unsplitable": true
        }
        {
          "name": "count"
          "type": "NUMBER"
          "unsplitable": true
        }
        {
          "name": "delta_hist"
          "type": "NUMBER"
          "unsplitable": true
        }
        {
          "name": "user_unique"
          "type": "NUMBER"
          "unsplitable": true
        }
      ])
      testComplete()
    ).done()

  it "does an introspect with overrides", (testComplete) ->
    wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      requester: requesterDruid_0_8_0
      attributeOverrides: [
        { name: "user_unique", special: 'unique' }
        { name: "delta_hist", special: 'histogram' }
      ]
    })

    wikiExternal.introspect().then((introspectedExternal) ->
      expect(introspectedExternal.toJS().attributes).to.deep.equal([
        {
          "name": "time"
          "type": "TIME"
        }
        {
          "name": "anonymous"
          "type": "STRING"
        }
        {
          "name": "language"
          "type": "STRING"
        }
        {
          "name": "namespace"
          "type": "STRING"
        }
        {
          "name": "newPage"
          "type": "STRING"
        }
        {
          "name": "page"
          "type": "STRING"
        }
        {
          "name": "added"
          "type": "NUMBER"
          "unsplitable": true
        }
        {
          "name": "count"
          "type": "NUMBER"
          "unsplitable": true
        }
        {
          "name": "delta_hist"
          "special": "histogram"
          "type": "NUMBER"
        }
        {
          "name": "user_unique"
          "special": "unique"
          "type": "STRING"
        }
      ])
      testComplete()
    ).done()
