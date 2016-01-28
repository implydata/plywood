{ expect } = require("chai")

{ WallTime } = require('chronoshift')
if not WallTime.rules
  tzData = require("chronoshift/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

{ mySqlRequesterFactory } = require('plywood-mysql-requester')

plywood = require('../../build/plywood')
{ Expression, External, TimeRange, $, ply, basicExecutorFactory } = plywood

info = require('../info')

mySqlRequester = mySqlRequesterFactory({
  host: info.mySqlHost
  database: info.mySqlDatabase
  user: info.mySqlUser
  password: info.mySqlPassword
})

describe "MySQLExternal", ->
  @timeout(10000);

  describe "defined attributes in datasource", ->
    basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'mysql'
          table: 'wiki_day_agg'
          attributes: [
            { name: 'time', type: 'TIME' }
            { name: 'language', type: 'STRING' }
            { name: 'page', type: 'STRING' }
            { name: 'added', type: 'NUMBER' }
            { name: 'count', type: 'NUMBER' }
          ]
          requester: mySqlRequester
        })
      }
    })

    it "works in advanced case", (testComplete) ->
      ex = ply()
        .apply("wiki", $('wiki').filter($("language").is('en')))
        .apply('Count', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply('Pages',
          $("wiki").split("$page", 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(2)
            .apply('Time',
              $("wiki").split($("time").timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
                .apply('TotalAdded', '$wiki.sum($added)')
                .sort('$TotalAdded', 'descending')
                .limit(3)
            )
        )
  #      .apply('PagesHaving',
  #        $("wiki").split("$page", 'Page')
  #          .apply('Count', '$wiki.sum($count)')
  #          .sort('$Count', 'descending')
  #          .filter($('Count').lessThan(30))
  #          .limit(100)
  #      )

      basicExecutor(ex).then((result) ->
        expect(result.toJS()).to.deep.equal([
          {
            "Count": 334129
            "Pages": [
              {
                "Count": 626
                "Page": "User:Addbot/log/wikidata"
                "Time": [
                  {
                    "Timestamp": {
                      "end": new Date("2013-02-26T20:00:00.000Z")
                      "start": new Date("2013-02-26T19:00:00.000Z")
                      "type": "TIME_RANGE"
                    }
                    "TotalAdded": 180454
                  }
                  {
                    "Timestamp": {
                      "end": new Date("2013-02-26T13:00:00.000Z")
                      "start": new Date("2013-02-26T12:00:00.000Z")
                      "type": "TIME_RANGE"
                    }
                    "TotalAdded": 178939
                  }
                  {
                    "Timestamp": {
                      "end": new Date("2013-02-26T01:00:00.000Z")
                      "start": new Date("2013-02-26T00:00:00.000Z")
                      "type": "TIME_RANGE"
                    }
                    "TotalAdded": 159582
                  }
                ]
              }
              {
                "Count": 329
                "Page": "User:Legobot/Wikidata/General"
                "Time": [
                  {
                    "Timestamp": {
                      "end": new Date("2013-02-26T16:00:00.000Z")
                      "start": new Date("2013-02-26T15:00:00.000Z")
                      "type": "TIME_RANGE"
                    }
                    "TotalAdded": 7609
                  }
                  {
                    "Timestamp": {
                      "end": new Date("2013-02-26T22:00:00.000Z")
                      "start": new Date("2013-02-26T21:00:00.000Z")
                      "type": "TIME_RANGE"
                    }
                    "TotalAdded": 6919
                  }
                  {
                    "Timestamp": {
                      "end": new Date("2013-02-26T17:00:00.000Z")
                      "start": new Date("2013-02-26T16:00:00.000Z")
                      "type": "TIME_RANGE"
                    }
                    "TotalAdded": 5717
                  }
                ]
              }
            ]
            "TotalAdded": 41412583
          }
        ])
        testComplete()
      ).done()

    it "works multi-dimensional GROUP BYs", (testComplete) ->
      ex = ply()
        .apply("wiki", $('wiki').filter($("language").isnt('en')))
        .apply('Cuts',
          $("wiki").split({
              'Language': "$language",
              'TimeByHour': '$time.timeBucket(PT1H)'
            })
            .apply('Count', $('wiki').count())
            .sort('$Count', 'descending')
            .limit(4)
        )

      basicExecutor(ex).then((result) ->
        expect(result.toJS()).to.deep.equal([
          {
            "Cuts": [
              {
                "Count": 1904
                "Language": "he"
                "TimeByHour": {
                  "end": new Date('2013-02-26T21:00:00Z')
                  "start": new Date('2013-02-26T20:00:00Z')
                  "type": "TIME_RANGE"
                }
              }
              {
                "Count": 1823
                "Language": "de"
                "TimeByHour": {
                  "end": new Date('2013-02-26T18:00:00Z')
                  "start": new Date('2013-02-26T17:00:00Z')
                  "type": "TIME_RANGE"
                }
              }
              {
                "Count": 1788
                "Language": "sv"
                "TimeByHour": {
                  "end": new Date('2013-02-26T17:00:00Z')
                  "start": new Date('2013-02-26T16:00:00Z')
                  "type": "TIME_RANGE"
                }
              }
              {
                "Count": 1776
                "Language": "nl"
                "TimeByHour": {
                  "end": new Date('2013-02-26T22:00:00Z')
                  "start": new Date('2013-02-26T21:00:00Z')
                  "type": "TIME_RANGE"
                }
              }
            ]
          }
        ])
        testComplete()
      ).done()

  describe "introspection", ->
    basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'mysql'
          table: 'wiki_day_agg'
          requester: mySqlRequester
        })
      }
    })

    it "works with introspection", (testComplete) ->
      ex = ply()
        .apply("wiki", $('wiki').filter($("language").is('en')))
        .apply('Count', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply('Time',
          $("wiki").split($("time").timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$Timestamp', 'ascending')
            .limit(3)
            .apply('Pages',
              $("wiki").split("$page", 'Page')
                .apply('Count', '$wiki.sum($count)')
                .sort('$Count', 'descending')
                .limit(2)
            )
        )

      basicExecutor(ex).then((result) ->
        expect(result.toJS()).to.deep.equal([
          {
            "Count": 334129
            "Time": [
              {
                "Pages": [
                  {
                    "Count": 130
                    "Page": "User:Addbot/log/wikidata"
                  }
                  {
                    "Count": 31
                    "Page": "Wikipedia:Categories_for_discussion/Speedy"
                  }
                ]
                "Timestamp": {
                  "end": new Date("2013-02-26T01:00:00.000Z")
                  "start": new Date("2013-02-26T00:00:00.000Z")
                  "type": "TIME_RANGE"
                }
                "TotalAdded": 2149342
              }
              {
                "Pages": [
                  {
                    "Count": 121
                    "Page": "User:Addbot/log/wikidata"
                  }
                  {
                    "Count": 34
                    "Page": "Ahmed_Elkady"
                  }
                ]
                "Timestamp": {
                  "end": new Date("2013-02-26T02:00:00.000Z")
                  "start": new Date("2013-02-26T01:00:00.000Z")
                  "type": "TIME_RANGE"
                }
                "TotalAdded": 1717907
              }
              {
                "Pages": [
                  {
                    "Count": 22
                    "Page": "User:Libsbml/sandbox"
                  }
                  {
                    "Count": 20
                    "Page": "The_Biggest_Loser:_Challenge_America"
                  }
                ]
                "Timestamp": {
                  "end": new Date("2013-02-26T03:00:00.000Z")
                  "start": new Date("2013-02-26T02:00:00.000Z")
                  "type": "TIME_RANGE"
                }
                "TotalAdded": 1258761
              }
            ]
            "TotalAdded": 41412583
          }
        ])
        testComplete()
      ).done()
  describe "fallback", ->
    basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'mysql'
          table: 'wiki_day_agg'
          requester: mySqlRequester
        })
      }
    })


    it "fallback doesnt happen if not null", (testComplete) ->
      ex = ply()
      .apply("wiki", $('wiki'))
      .apply('AvgPrice1', $('wiki').average($('added')).fallback(2))

      basicExecutor(ex).then((result) ->
        expect(result.toJS()).to.deep.equal([
          {
            "AvgPrice1": 216.5617
          }
        ])
        testComplete()
      ).done()

      ### INSERT INTO wiki_day_agg (page) values ('page'); ###

    it "fallback happens if null", (testComplete) ->
      ex = ply()
        .apply("wiki", $('wiki').filter($("page").is('page')))
        .apply('TotalAdded', $('wiki').sum($('added')).fallback(0))

      basicExecutor(ex).then((result) ->
        expect(result.toJS()).to.deep.equal([
          {
            "TotalAdded": 0
          }
        ])
        testComplete()
      ).done()

