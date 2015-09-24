{ expect } = require("chai")
Q = require("q")

{ WallTime } = require('chronoshift')
if not WallTime.rules
  tzData = require("chronoshift/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

plywood = require('../../build/plywood')
{ External, introspectDatum, $, ply, r } = plywood


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
      expect(newContext.diamonds.toJS().attributes).to.deep.equal([
        {
          "name": "time"
          "type": "TIME"
        }
        {
          "name": "color"
          "type": "STRING"
        }
        {
          "name": "cut"
          "type": "STRING"
        }
        {
          "name": "tags"
          "type": "STRING"
        }
        {
          "name": "carat"
          "type": "STRING"
        }
        {
          "filterable": false
          "name": "price"
          "splitable": false
          "type": "NUMBER"
        }
        {
          "filterable": false
          "name": "tax"
          "splitable": false
          "type": "NUMBER"
        }
        {
          "name": "unique_view"
          "special": "unique"
          "type": "STRING"
        }
      ])

      expect(newContext.wiki.toJS().attributes).to.deep.equal([
        {
          "name": "timestamp"
          "type": "TIME"
        }
        {
          "name": "page"
          "type": "STRING"
        }
        {
          "name": "language"
          "type": "STRING"
        }
        {
          "name": "is_robot"
          "type": "STRING"
        }
        {
          "filterable": false
          "name": "added"
          "splitable": false
          "type": "NUMBER"
        }
        {
          "filterable": false
          "name": "deleted"
          "splitable": false
          "type": "NUMBER"
        }
      ])

      testComplete()
    ).done()
