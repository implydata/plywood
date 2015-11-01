{ expect } = require("chai")

{ WallTime } = require('chronoshift')
if not WallTime.rules
  tzData = require("chronoshift/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

{ druidRequesterFactory } = require('plywood-druid-requester')

plywood = require('../../build/plywood')
{ Expression, External, TimeRange, $, ply, basicExecutorFactory, helper } = plywood

info = require('../info')

druidRequester = druidRequesterFactory({
  host: info.druidHost
})

#druidRequester = helper.verboseRequesterFactory({
#  requester: druidRequester
#})

describe "DruidExternal", ->
  @timeout(10000);

  describe "defined attributes in datasource", ->
    basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'druid',
          dataSource: 'wikipedia',
          timeAttribute: 'time',
          context: null
          attributes: [
            { name: 'time', type: 'TIME' }
            { name: 'language', type: 'STRING' }
            { name: 'page', type: 'STRING' }
            { name: 'user', type: 'STRING' }
            { name: 'added', type: 'NUMBER' }
            { name: 'count', type: 'NUMBER' }
            { name: 'user_unique', special: 'unique' }
          ]
          filter: $('time').in(TimeRange.fromJS({
            start: new Date("2015-08-14T00:00:00Z")
            end: new Date("2015-08-15T00:00:00Z")
          }))
          requester: druidRequester
        })
      }
    })

    it "works timePart case", (testComplete) ->
      ex = ply()
        .apply("wiki", $('wiki').filter($("language").is('en')))
        .apply('HoursOfDay',
          $("wiki").split("$time.timePart(HOUR_OF_DAY, 'Etc/UTC')", 'HourOfDay')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$TotalAdded', 'descending')
            .limit(3)
        )

      # console.log("ex.simulateQueryPlan(context)", JSON.stringify(ex.simulateQueryPlan(context), null, 2));

      basicExecutor(ex).then((result) ->
        expect(result.toJS()).to.deep.equal([
          {
            "HoursOfDay": [
              {
                "HourOfDay": 0
                "TotalAdded": 4381326
              }
              {
                "HourOfDay": 3
                "TotalAdded": 4159589
              }
              {
                "HourOfDay": 1
                "TotalAdded": 3888962
              }
            ]
          }
        ])
        testComplete()
      ).done()

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
        .apply('PagesHaving',
          $("wiki").split("$page", 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .filter($('Count').lessThan(300))
            .limit(5)
        )

      basicExecutor(ex).then((result) ->
        expect(result.toJS()).to.deep.equal([
          {
            "Count": 122857
            "Pages": [
              {
                "Count": 262
                "Page": "Wikipedia:Administrators'_noticeboard/Incidents"
                "Time": [
                  {
                    "Timestamp": {
                      "end": new Date('2015-08-14T03:00:00.000Z')
                      "start": new Date('2015-08-14T02:00:00.000Z')
                      "type": "TIME_RANGE"
                    }
                    "TotalAdded": 100304
                  }
                  {
                    "Timestamp": {
                      "end": new Date('2015-08-14T20:00:00.000Z')
                      "start": new Date('2015-08-14T19:00:00.000Z')
                      "type": "TIME_RANGE"
                    }
                    "TotalAdded": 18856
                  }
                  {
                    "Timestamp": {
                      "end": new Date('2015-08-14T22:00:00.000Z')
                      "start": new Date('2015-08-14T21:00:00.000Z')
                      "type": "TIME_RANGE"
                    }
                    "TotalAdded": 11550
                  }
                ]
              }
              {
                "Count": 253
                "Page": "User:Cyde/List_of_candidates_for_speedy_deletion/Subpage"
                "Time": [
                  {
                    "Timestamp": {
                      "end": new Date('2015-08-14T07:00:00.000Z')
                      "start": new Date('2015-08-14T06:00:00.000Z')
                      "type": "TIME_RANGE"
                    }
                    "TotalAdded": 3416
                  }
                  {
                    "Timestamp": {
                      "end": new Date('2015-08-14T02:00:00.000Z')
                      "start": new Date('2015-08-14T01:00:00.000Z')
                      "type": "TIME_RANGE"
                    }
                    "TotalAdded": 2400
                  }
                  {
                    "Timestamp": {
                      "end": new Date('2015-08-14T16:00:00.000Z')
                      "start": new Date('2015-08-14T15:00:00.000Z')
                      "type": "TIME_RANGE"
                    }
                    "TotalAdded": 2213
                  }
                ]
              }
            ]
            "PagesHaving": [
              {
                "Count": 266
                "Page": "Wikipedia:Administrators'_noticeboard/Incidents"
              }
              {
                "Count": 253
                "Page": "User:Cyde/List_of_candidates_for_speedy_deletion/Subpage"
              }
              {
                "Count": 200
                "Page": "Wikipedia:Administrator_intervention_against_vandalism"
              }
              {
                "Count": 140
                "Page": "2015_Tianjin_explosions"
              }
              {
                "Count": 130
                "Page": "Indoor_Football_League_(1999–2000)"
              }
            ]
            "TotalAdded": 45268530
          }
        ])
        testComplete()
      ).done()

    it "works with uniques", (testComplete) ->
      ex = ply()
        .apply('UniquePages', $('wiki').countDistinct("$page"))
        .apply('UniqueUsers1', $('wiki').countDistinct("$user"))
        .apply('UniqueUsers2', $('wiki').countDistinct("$user_unique"))
        .apply('Diff', '$UniqueUsers1 - $UniqueUsers2')

      basicExecutor(ex).then((result) ->
        expect(result.toJS()).to.deep.equal([
          {
            "Diff": 24550.2655599543
            "UniquePages": 102748.42464311104
            "UniqueUsers1": 24550.2655599543
            "UniqueUsers2": 0 # this is 0 because uniques did not exist yet
          }
        ])
        testComplete()
      ).done()

    it "works with no applies in dimensions split dataset", (testComplete) ->
      ex = ply()
        .apply('Pages',
          $('wiki').split("$page", 'Page')
            .sort('$Page', 'descending')
            .limit(2)
            .apply('Users',
              $('wiki').split('$user', 'User')
                .apply('Count', $('wiki').count())
                .sort('$Count', 'descending')
                .limit(2)
            )
        )

      basicExecutor(ex).then((result) ->
        expect(result.toJS()).to.deep.equal([
          {
            "Pages": [
              {
                "Page": "!distain"
                "Users": [
                  {
                    "Count": 1
                    "User": "SteEis"
                  }
                ]
              }
              {
                "Page": "\"A\"_Is_for_Alibi"
                "Users": [
                  {
                    "Count": 1
                    "User": "Fitnr"
                  }
                ]
              }
            ]
          }
        ])
        testComplete()
      ).done()

    it "works with no applies in time split dataset", (testComplete) ->
      ex = ply()
        .apply('ByHour',
          $('wiki').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
            .sort('$TimeByHour', 'ascending')
            .limit(3)
            .apply('Users',
              $('wiki').split('$page', 'Page')
                .apply('Count', $('wiki').count())
                .sort('$Count', 'descending')
                .limit(2)
          )
        )

      basicExecutor(ex).then((result) ->
        expect(result.toJS()).to.deep.equal([
          {
            "ByHour": [
              {
                "TimeByHour": {
                  "end": new Date('2015-08-14T01:00:00.000Z')
                  "start": new Date('2015-08-14T00:00:00.000Z')
                  "type": "TIME_RANGE"
                }
                "Users": [
                  {
                    "Count": 34
                    "Page": "Wikipedia:Administrator_intervention_against_vandalism"
                  }
                  {
                    "Count": 27
                    "Page": "User_talk:Thine_Antique_Pen"
                  }
                ]
              }
              {
                "TimeByHour": {
                  "end": new Date('2015-08-14T02:00:00.000Z')
                  "start": new Date('2015-08-14T01:00:00.000Z')
                  "type": "TIME_RANGE"
                }
                "Users": [
                  {
                    "Count": 40
                    "Page": "User_talk:MherHzzor"
                  }
                  {
                    "Count": 23
                    "Page": "Patas_monkey"
                  }
                ]
              }
              {
                "TimeByHour": {
                  "end": new Date('2015-08-14T03:00:00.000Z')
                  "start": new Date('2015-08-14T02:00:00.000Z')
                  "type": "TIME_RANGE"
                }
                "Users": [
                  {
                    "Count": 21
                    "Page": "Olive_baboon"
                  }
                  {
                    "Count": 16
                    "Page": "Blue_monkey"
                  }
                ]
              }
            ]
          }
        ])
        testComplete()
      ).done()

    it "works with contains (case sensitive) filter", (testComplete) ->
      ex = ply()
        .apply('wiki', $('wiki').filter($('page').contains('wiki')))
        .apply('Pages',
          $('wiki').split($("page"), 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        )

      basicExecutor(ex).then((result) ->
        expect(result.toJS()).to.deep.equal([
          {
            "Pages": [
              {
                "Count": 10
                "Page": "User_talk:Trmwikifa"
              }
              {
                "Count": 8
                "Page": "User_talk:2015wiki"
              }
              {
                "Count": 7
                "Page": "Benutzer:Paddy1111~dewiki/test2"
              }
            ]
          }
        ])
        testComplete()
      ).done()

    it "works with contains (case insensitive) filter", (testComplete) ->
      ex = ply()
        .apply('wiki', $('wiki').filter($('page').contains('wiki', 'ignoreCase')))
        .apply('Pages',
          $('wiki').split($("page"), 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        )

      basicExecutor(ex).then((result) ->
        expect(result.toJS()).to.deep.equal([
          {
            "Pages": [
              {
                "Count": 266
                "Page": "Wikipedia:Administrators'_noticeboard/Incidents"
              }
              {
                "Count": 243
                "Page": "Wikipedia:Löschkandidaten/14._August_2015"
              }
              {
                "Count": 200
                "Page": "Wikipedia:Administrator_intervention_against_vandalism"
              }
            ]
          }
        ])
        testComplete()
      ).done()

    it "works with concat split", (testComplete) ->
      ex = ply()
        .apply('Pages',
          $('wiki').split("'!!!<' ++ $page ++ '>!!!'", 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        )

      basicExecutor(ex).then((result) ->
        expect(result.toJS()).to.deep.equal([
          {
            "Pages": [
              {
                "Count": 262
                "Page": "!!!<Wikipedia:Administrators'_noticeboard/Incidents>!!!"
              }
              {
                "Count": 253
                "Page": "!!!<User:Cyde/List_of_candidates_for_speedy_deletion/Subpage>!!!"
              }
              {
                "Count": 242
                "Page": "!!!<Wikipedia:Löschkandidaten/14._August_2015>!!!"
              }
            ]
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
                "Count": 1487
                "Language": "de"
                "TimeByHour": {
                  "end": new Date('2015-08-14T21:00:00Z')
                  "start": new Date('2015-08-14T20:00:00Z')
                  "type": "TIME_RANGE"
                }
              }
              {
                "Count": 1449
                "Language": "de"
                "TimeByHour": {
                  "end": new Date('2015-08-14T14:00:00Z')
                  "start": new Date('2015-08-14T13:00:00Z')
                  "type": "TIME_RANGE"
                }
              }
              {
                "Count": 1430
                "Language": "de"
                "TimeByHour": {
                  "end": new Date('2015-08-14T12:00:00Z')
                  "start": new Date('2015-08-14T11:00:00Z')
                  "type": "TIME_RANGE"
                }
              }
              {
                "Count": 1418
                "Language": "fr"
                "TimeByHour": {
                  "end": new Date('2015-08-14T09:00:00Z')
                  "start": new Date('2015-08-14T08:00:00Z')
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
          engine: 'druid',
          dataSource: 'wikipedia',
          timeAttribute: 'time',
          context: null
          filter: $('time').in(TimeRange.fromJS({
            start: new Date("2015-09-14T00:00:00Z")
            end: new Date("2015-09-15T00:00:00Z")
          }))
          requester: druidRequester
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
            "Count": 124632
            "Time": [
              {
                "Pages": [
                  {
                    "Count": 35
                    "Page": "Moses_Malone"
                  }
                  {
                    "Count": 25
                    "Page": "Wikipedia:Articles_for_deletion/Log/2015_September_14"
                  }
                ]
                "Timestamp": {
                  "end": new Date('2015-09-14T01:00:00.000Z')
                  "start": new Date('2015-09-14T00:00:00.000Z')
                  "type": "TIME_RANGE"
                }
                "TotalAdded": 2550858
              }
              {
                "Pages": [
                  {
                    "Count": 43
                    "Page": "Edith_Wilmans"
                  }
                  {
                    "Count": 21
                    "Page": "Template:Quote/testcases2"
                  }
                ]
                "Timestamp": {
                  "end": new Date('2015-09-14T02:00:00.000Z')
                  "start": new Date('2015-09-14T01:00:00.000Z')
                  "type": "TIME_RANGE"
                }
                "TotalAdded": 2090200
              }
              {
                "Pages": [
                  {
                    "Count": 58
                    "Page": "Margie_Neal"
                  }
                  {
                    "Count": 24
                    "Page": "User:Curiocurio/sandbox"
                  }
                ]
                "Timestamp": {
                  "end": new Date('2015-09-14T03:00:00.000Z')
                  "start": new Date('2015-09-14T02:00:00.000Z')
                  "type": "TIME_RANGE"
                }
                "TotalAdded": 3617079
              }
            ]
            "TotalAdded": 47744225
          }
        ])
        testComplete()
      ).done()
