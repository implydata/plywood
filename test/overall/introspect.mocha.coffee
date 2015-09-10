{ expect } = require("chai")
Q = require("q")

{ WallTime } = require('chronoshift')
if not WallTime.rules
  tzData = require("chronoshift/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

plywood = require('../../build/plywood')
{ External, introspectDatum, $ } = plywood


describe "introspect", ->
  mockRequester = (request) ->
    return Q.fcall(->
      query = request.query
      expect(query.queryType).to.equal('segmentMetadata')
      if query.dataSource is 'diamonds'
        return [{
          "id": "some_diamonds_id",
          "intervals": ["0000-01-01T00:00:00.000Z/9999-12-31T00:00:00.000Z"],
          "columns": {
            "time": {"type": "LONG", "size": 407240380, "cardinality": null},
            "color": {"type": "STRING", "size": 100000, "cardinality": 1944},
            "cut": {"type": "STRING", "size": 100000, "cardinality": 1504},
            "tags": {"type": "STRING", "size": 100000, "cardinality": 1504},
            "carat": {"type": "STRING", "size": 100000, "cardinality": 1504},
            "price": {"type": "FLOAT", "size": 100000, "cardinality": null},
            "tax": {"type": "FLOAT", "size": 100000, "cardinality": null},
            "unique_view": {"type": "FLOAT", "size": 100000, "cardinality": null}
          },
          "size": 300000
        }]
      else if query.dataSource is 'wikipedia'
        return [{
          "id": "some_wikipedia_id",
          "intervals": ["0000-01-01T00:00:00.000Z/9999-12-31T00:00:00.000Z"],
          "columns": {
            "timestamp": {"type": "LONG", "size": 407240380, "cardinality": null},
            "page": {"type": "STRING", "size": 100000, "cardinality": 1944},
            "language": {"type": "STRING", "size": 100000, "cardinality": 1504},
            "is_robot": {"type": "STRING", "size": 100000, "cardinality": 1504},
            "added": {"type": "FLOAT", "size": 100000, "cardinality": null},
            "deleted": {"type": "FLOAT", "size": 100000, "cardinality": null}
          },
          "size": 300000
        }]
      else
        throw new Error("no such datasource: #{query.dataSource}");
    )

  context = {
    something: 'else'
    diamonds: External.fromJS({
      requester: mockRequester
      engine: 'druid',
      dataSource: 'diamonds',
      timeAttribute: 'time',
      context: null
    })
    wiki: External.fromJS({
      requester: mockRequester
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'timestamp',
      context: null
    })
  }

  it "introspects", (testComplete) ->
    introspectDatum(context).then((newContext) ->
      expect(newContext.diamonds.toJS().attributes).to.deep.equal({
        "carat": {
          "type": "STRING"
        }
        "color": {
          "type": "STRING"
        }
        "cut": {
          "type": "STRING"
        }
        "price": {
          "filterable": false
          "splitable": false
          "type": "NUMBER"
        }
        "tags": {
          "type": "STRING"
        }
        "tax": {
          "filterable": false
          "splitable": false
          "type": "NUMBER"
        }
        "time": {
          "type": "TIME"
        }
        "unique_view": {
          "filterable": false
          "splitable": false
          "type": "NUMBER"
        }
      })

      expect(newContext.wiki.toJS().attributes).to.deep.equal({
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
        "is_robot": {
          "type": "STRING"
        }
        "language": {
          "type": "STRING"
        }
        "page": {
          "type": "STRING"
        }
        "timestamp": {
          "type": "TIME"
        }
      })

      testComplete()
    ).done()
