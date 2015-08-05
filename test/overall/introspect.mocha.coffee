{ expect } = require("chai")
Q = require("q")

{ WallTime } = require('chronology')
if not WallTime.rules
  tzData = require("chronology/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

plywood = require('../../build/plywood')
{ External, introspectDatum, $ } = plywood


describe "introspect", ->
  mockRequester = (request) ->
    return Q.fcall(->
      query = request.query
      expect(query.queryType).to.equal('introspect')
      if query.dataSource is 'diamonds'
        return {
          dimensions: ['color', 'cut', 'tags', 'carat']
          metrics: ['price', 'tax', 'unique_view']
        }
      else if query.dataSource is 'wikipedia'
        return {
          dimensions: ['page', 'language', 'is_robot']
          metrics: ['added', 'deleted']
        }
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
