var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var { druidRequesterFactory } = require('plywood-druid-requester');

var plywood = require('../../build/plywood');
var { Expression, External, TimeRange, $, ply, basicExecutorFactory, helper } = plywood;

var info = require('../info');

var druidRequester = druidRequesterFactory({
  host: info.druidHost
});

//druidRequester = helper.verboseRequesterFactory({
//  requester: druidRequester
//})

describe("DruidExternal", function() {
  this.timeout(10000),

    describe("defined attributes in datasource", function() {
      var basicExecutor = basicExecutorFactory({
        datasets: {
          wiki: External.fromJS({
            engine: 'druid',
            dataSource: 'wikipedia',
            timeAttribute: 'time',
            context: {
              timeout: 10000
            },
            attributes: [
              { name: 'time', type: 'TIME' },
              { name: 'sometimeLater', type: 'TIME' },
              { name: 'channel', type: 'STRING' },
              { name: 'page', type: 'STRING' },
              { name: 'page_unique', special: 'unique' },
              { name: 'user', type: 'STRING' },
              { name: 'newPage', type: 'BOOLEAN' },
              { name: 'anonymous', type: 'BOOLEAN' },
              { name: 'added', type: 'NUMBER' },
              { name: 'count', type: 'NUMBER' },
              { name: 'commentLength', type: 'NUMBER' },
              { name: 'metroCode', type: 'STRING' },
              { name: 'cityName', type: 'STRING' },
              { name: 'user_unique', special: 'unique' }
            ],
            filter: $('time').in(TimeRange.fromJS({
              start: new Date("2015-09-12T00:00:00Z"),
              end: new Date("2015-09-13T00:00:00Z")
            })),
            druidVersion: info.druidVersion,
            requester: druidRequester
          })
        }
      });

      it("works timePart case", function(testComplete) {
        var ex = ply()
          .apply("wiki", $('wiki').filter($("channel").is('en')))
          .apply(
            'HoursOfDay',
            $("wiki").split("$time.timePart(HOUR_OF_DAY, 'Etc/UTC')", 'HourOfDay')
              .apply('TotalAdded', '$wiki.sum($added)')
              .sort('$TotalAdded', 'descending')
              .limit(3)
          );

        // console.log("ex.simulateQueryPlan(context)", JSON.stringify(ex.simulateQueryPlan(context), null, 2));

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "HoursOfDay": [
                  {
                    "HourOfDay": 2,
                    "TotalAdded": 3045966
                  },
                  {
                    "HourOfDay": 17,
                    "TotalAdded": 1883290
                  },
                  {
                    "HourOfDay": 3,
                    "TotalAdded": 1825954
                  }
                ]
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it("works in advanced case", function(testComplete) {
        var ex = ply()
          .apply("wiki", $('wiki').filter($("channel").is('en')))
          .apply('Count', '$wiki.sum($count)')
          .apply('TotalAdded', '$wiki.sum($added)')
          .apply(
            'Pages',
            $("wiki").split("$page", 'Page')
              .apply('Count', '$wiki.sum($count)')
              .sort('$Count', 'descending')
              .limit(2)
              .apply(
                'Time',
                $("wiki").split($("time").timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
                  .apply('TotalAdded', '$wiki.sum($added)')
                  .sort('$TotalAdded', 'descending')
                  .limit(3)
              )
          )
          .apply(
            'PagesHaving',
            $("wiki").split("$page", 'Page')
              .apply('Count', '$wiki.sum($count)')
              .sort('$Count', 'descending')
              .filter($('Count').lessThan(300))
              .limit(4)
          );

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "Count": 114711,
                "Pages": [
                  {
                    "Count": 255,
                    "Page": "User:Cyde/List of candidates for speedy deletion/Subpage",
                    "Time": [
                      {
                        "Timestamp": {
                          "end": new Date('2015-09-12T13:00:00.000Z'),
                          "start": new Date('2015-09-12T12:00:00.000Z'),
                          "type": "TIME_RANGE"
                        },
                        "TotalAdded": 9231
                      },
                      {
                        "Timestamp": {
                          "end": new Date('2015-09-13T00:00:00.000Z'),
                          "start": new Date('2015-09-12T23:00:00.000Z'),
                          "type": "TIME_RANGE"
                        },
                        "TotalAdded": 3956
                      },
                      {
                        "Timestamp": {
                          "end": new Date('2015-09-12T02:00:00.000Z'),
                          "start": new Date('2015-09-12T01:00:00.000Z'),
                          "type": "TIME_RANGE"
                        },
                        "TotalAdded": 3363
                      }
                    ]
                  },
                  {
                    "Count": 240,
                    "Page": "Jeremy Corbyn",
                    "Time": [
                      {
                        "Timestamp": {
                          "end": new Date('2015-09-12T16:00:00.000Z'),
                          "start": new Date('2015-09-12T15:00:00.000Z'),
                          "type": "TIME_RANGE"
                        },
                        "TotalAdded": 28193
                      },
                      {
                        "Timestamp": {
                          "end": new Date('2015-09-12T19:00:00.000Z'),
                          "start": new Date('2015-09-12T18:00:00.000Z'),
                          "type": "TIME_RANGE"
                        },
                        "TotalAdded": 2419
                      },
                      {
                        "Timestamp": {
                          "end": new Date('2015-09-12T11:00:00.000Z'),
                          "start": new Date('2015-09-12T10:00:00.000Z'),
                          "type": "TIME_RANGE"
                        },
                        "TotalAdded": 2041
                      }
                    ]
                  }
                ],
                "PagesHaving": [
                  {
                    "Count": 255,
                    "Page": "User:Cyde/List of candidates for speedy deletion/Subpage"
                  },
                  {
                    "Count": 241,
                    "Page": "Jeremy Corbyn"
                  },
                  {
                    "Count": 228,
                    "Page": "Wikipedia:Administrators' noticeboard/Incidents"
                  },
                  {
                    "Count": 146,
                    "Page": "Wikipedia:Administrator intervention against vandalism"
                  }
                ],
                "TotalAdded": 32553107
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it("works with uniques", function(testComplete) {
        var ex = ply()
          .apply('UniquePages1', $('wiki').countDistinct("$page"))
          .apply('UniquePages2', $('wiki').countDistinct("$page_unique"))
          .apply('UniqueUsers1', $('wiki').countDistinct("$user"))
          .apply('UniqueUsers2', $('wiki').countDistinct("$user_unique"));
        //.apply('UniqueDiff', '$UniqueUsers1 - $UniqueUsers2')

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "UniquePages1": 278906.2678236051,
                "UniquePages2": 281588.11316378025,
                "UniqueUsers1": 39220.49269175933,
                "UniqueUsers2": 37712.65497107271
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it("works with no applies in dimensions split dataset", function(testComplete) {
        var ex = ply()
          .apply(
            'Pages',
            $('wiki').split("$page", 'Page')
              .sort('$Page', 'descending')
              .limit(2)
              .apply(
                'Users',
                $('wiki').split('$user', 'User')
                  .apply('Count', $('wiki').count())
                  .sort('$Count', 'descending')
                  .limit(2)
              )
          );

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "Pages": [
                  {
                    "Page": "!T.O.O.H.!",
                    "Users": [
                      {
                        "Count": 1,
                        "User": "Cameronsmiley2345qwerty"
                      }
                    ]
                  },
                  {
                    "Page": "\"The Secret Life of...\"",
                    "Users": [
                      {
                        "Count": 2,
                        "User": "Vikiçizer"
                      }
                    ]
                  }
                ]
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it("works with absolute", function(testComplete) {
        var ex = ply()
          .apply("Count", $('wiki').filter($("channel").is('en')).count())
          .apply('Negate', $('Count').negate())
          .apply('Abs', $('Count').negate().absolute().negate().absolute());

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "Abs": 113240,
                "Count": 113240,
                "Negate": -113240
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it("works with power", function(testComplete) {
        var ex = ply()
          .apply("Count", $('wiki').filter($("channel").is('en')).count())
          .apply('Square Root', $('Count').power(0.5))
          .apply('Squared', $('Count').power(2))
          .apply('One', $('Count').power(0));

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "Count": 113240,
                "One": 113240,
                "Square Root": 336.5115154047481,
                "Squared": 12823297600
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it("works with no applies in time split dataset", function(testComplete) {
        var ex = ply()
          .apply(
            'ByHour',
            $('wiki').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
              .sort('$TimeByHour', 'ascending')
              .limit(3)
              .apply(
                'Users',
                $('wiki').split('$page', 'Page')
                  .apply('Count', $('wiki').count())
                  .sort('$Count', 'descending')
                  .limit(2)
              )
          );

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "ByHour": [
                  {
                    "TimeByHour": {
                      "end": new Date('2015-09-12T01:00:00.000Z'),
                      "start": new Date('2015-09-12T00:00:00.000Z'),
                      "type": "TIME_RANGE"
                    },
                    "Users": [
                      {
                        "Count": 10,
                        "Page": "POOP"
                      },
                      {
                        "Count": 9,
                        "Page": "Israel Ballet"
                      }
                    ]
                  },
                  {
                    "TimeByHour": {
                      "end": new Date('2015-09-12T02:00:00.000Z'),
                      "start": new Date('2015-09-12T01:00:00.000Z'),
                      "type": "TIME_RANGE"
                    },
                    "Users": [
                      {
                        "Count": 26,
                        "Page": "Campeonato Mundial de Voleibol Femenino Sub-20 de 2015"
                      },
                      {
                        "Count": 22,
                        "Page": "Flüchtlingskrise in Europa 2015"
                      }
                    ]
                  },
                  {
                    "TimeByHour": {
                      "end": new Date('2015-09-12T03:00:00.000Z'),
                      "start": new Date('2015-09-12T02:00:00.000Z'),
                      "type": "TIME_RANGE"
                    },
                    "Users": [
                      {
                        "Count": 28,
                        "Page": "Wikipedia:Administrators' noticeboard/Incidents"
                      },
                      {
                        "Count": 16,
                        "Page": "2015 World Wrestling Championships"
                      }
                    ]
                  }
                ]
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it("works with contains (case sensitive) filter", function(testComplete) {
        var ex = ply()
          .apply('wiki', $('wiki').filter($('page').contains('wiki')))
          .apply(
            'Pages',
            $('wiki').split($("page"), 'Page')
              .apply('Count', '$wiki.sum($count)')
              .sort('$Count', 'descending')
              .limit(3)
          );

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "Pages": [
                  {
                    "Count": 25,
                    "Page": "Wikipedia:Checklijst langdurig structureel vandalisme/1wikideb1"
                  },
                  {
                    "Count": 12,
                    "Page": "Diskuse s wikipedistou:Zdenekk2"
                  },
                  {
                    "Count": 11,
                    "Page": "Overleg gebruiker:Wwikix"
                  }
                ]
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it("works with contains (case insensitive) filter", function(testComplete) {
        var ex = ply()
          .apply('wiki', $('wiki').filter($('page').contains('wiki', 'ignoreCase')))
          .apply(
            'Pages',
            $('wiki').split($("page"), 'Page')
              .apply('Count', '$wiki.sum($count)')
              .sort('$Count', 'descending')
              .limit(3)
          );

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "Pages": [
                  {
                    "Count": 228,
                    "Page": "Wikipedia:Administrators' noticeboard/Incidents"
                  },
                  {
                    "Count": 186,
                    "Page": "Wikipedia:Vandalismusmeldung"
                  },
                  {
                    "Count": 146,
                    "Page": "Wikipedia:Administrator intervention against vandalism"
                  }
                ]
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it("works with match filter", function(testComplete) {
        var ex = ply()
          .apply('wiki', $('wiki').filter($('page').match('^.*Bot.*$')))
          .apply(
            'Pages',
            $('wiki').split($("page"), 'Page')
              .apply('Count', '$wiki.sum($count)')
              .sort('$Count', 'descending')
              .limit(3)
          );

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "Pages": [
                  {
                    "Count": 54,
                    "Page": "Wikipedia:Usernames for administrator attention/Bot"
                  },
                  {
                    "Count": 23,
                    "Page": "Usuari:TronaBot/log:Activitat reversors per hores"
                  },
                  {
                    "Count": 23,
                    "Page": "Usuari:TronaBot/log:Reversions i patrullatge"
                  }
                ]
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it("works with concat split", function(testComplete) {
        var ex = ply()
          .apply(
            'Pages',
            $('wiki').split("'!!!<' ++ $page ++ '>!!!'", 'Page')
              .apply('Count', '$wiki.sum($count)')
              .sort('$Count', 'descending')
              .limit(3)
          );

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "Pages": [
                  {
                    "Count": 316,
                    "Page": "!!!<Jeremy Corbyn>!!!"
                  },
                  {
                    "Count": 255,
                    "Page": "!!!<User:Cyde/List of candidates for speedy deletion/Subpage>!!!"
                  },
                  {
                    "Count": 223,
                    "Page": "!!!<Wikipedia:Administrators' noticeboard/Incidents>!!!"
                  }
                ]
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it("works with substr split", function(testComplete) {
        var ex = ply()
          .apply(
            'Pages',
            $('wiki').split("$page.substr(0,2)", 'Page')
              .apply('Count', '$wiki.sum($count)')
              .sort('$Count', 'descending')
              .limit(3)
          );

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "Pages": [
                  {
                    "Count": 22503,
                    "Page": "Ca"
                  },
                  {
                    "Count": 20338,
                    "Page": "Us"
                  },
                  {
                    "Count": 15332,
                    "Page": "Wi"
                  }
                ]
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it("works with extract split", function(testComplete) {
        var ex = ply()
          .apply(
            'Pages',
            $('wiki').split($('page').extract('([0-9]+\\.[0-9]+\\.[0-9]+)'), 'Page')
              .apply('Count', '$wiki.sum($count)')
              .sort('$Count', 'descending')
              .limit(3)
          );

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "Pages": [
                  {
                    "Count": 387184,
                    "Page": null
                  },
                  {
                    "Count": 21,
                    "Page": "75.108.94"
                  },
                  {
                    "Count": 14,
                    "Page": "120.29.65"
                  }
                ]
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it.skip("works with lookup split", function(testComplete) {
        var ex = ply()
          .apply(
            'Channels',
            $('wiki').split($('channel').lookup('wikipedia-channel-lookup'), 'Channel')
              .apply('Count', '$wiki.sum($count)')
              .sort('$Count', 'descending')
              .limit(3)
          );

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "Channels": [
                  {
                    "Count": 122857,
                    "Channel": "English"
                  },
                  {
                    "Count": 22862,
                    "Channel": "German"
                  },
                  {
                    "Count": 22140,
                    "Channel": "French"
                  }
                ]
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it.skip("works with absolute value split", function(testComplete) {
        var ex = ply()
          .apply(
            'AbsSplit',
            $('wiki').split($('commentLength').absolute(), 'AbsCommentLength')
              .sort('$AbsCommentLength', 'descending')
              .limit(4)
          );

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "AbsSplit": [
                  {
                    "AbsCommentLength": 1
                  },
                  {
                    "AbsCommentLength": 10
                  },
                  {
                    "AbsCommentLength": 100
                  },
                  {
                    "AbsCommentLength": 101
                  }
                ]
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it("can timeBucket a time column that is the timeAttribute one", function(testComplete) {
        var ex = ply()
          .apply(
            'Time',
            $("wiki").split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeCol')
              .sort('$TimeCol', 'descending')
              .limit(2)
          );

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([{
              "Time": [
                {
                  "TimeCol": {
                    "end": new Date('2015-09-13T00:00:00.000Z'),
                    "start": new Date('2015-09-12T23:00:00.000Z'),
                    "type": "TIME_RANGE"
                  }
                },
                {
                  "TimeCol": {
                    "end": new Date('2015-09-12T23:00:00.000Z'),
                    "start": new Date('2015-09-12T22:00:00.000Z'),
                    "type": "TIME_RANGE"
                  }
                }
              ]
            }

            ]);
            return testComplete();
          }
        ).done();
      });

      it("can timeBucket a time column that's not the timeAttribute one", function(testComplete) {
        var ex = ply()
          .apply(
            'TimeLater',
            $("wiki").split($("sometimeLater").timeBucket('PT1H', 'Etc/UTC'), 'SometimeLater')
              .limit(5)
          );

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "TimeLater": [
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T01:00:00.000Z'),
                      "start": new Date('2016-09-12T00:00:00.000Z'),
                      "type": "TIME_RANGE"
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T02:00:00.000Z'),
                      "start": new Date('2016-09-12T01:00:00.000Z'),
                      "type": "TIME_RANGE"
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T03:00:00.000Z'),
                      "start": new Date('2016-09-12T02:00:00.000Z'),
                      "type": "TIME_RANGE"
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T04:00:00.000Z'),
                      "start": new Date('2016-09-12T03:00:00.000Z'),
                      "type": "TIME_RANGE"
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T05:00:00.000Z'),
                      "start": new Date('2016-09-12T04:00:00.000Z'),
                      "type": "TIME_RANGE"
                    }
                  }
                ]
              }
            ]);
            return testComplete();
          }
        ).done();
      });

      it("works multi-dimensional GROUP BYs", function(testComplete) {
        var ex = ply()
          .apply("wiki", $('wiki').filter($("channel").isnt('en')))
          .apply(
            'Cuts',
            $("wiki").split({
                'Channel': "$channel",
                'TimeByHour': '$time.timeBucket(PT1H)',
                'NewPage': '$newPage',
                'ChannelIsDE': "$channel == 'de'"
              })
              .apply('Count', $('wiki').count())
              .sort('$Count', 'descending')
              .limit(4)
          );

        return basicExecutor(ex).then(function(result) {
            expect(result.toJS()).to.deep.equal([
              {
                "Cuts": [
                  {
                    "Channel": "vi",
                    "ChannelIsDE": false,
                    "Count": 12441,
                    "NewPage": null,
                    "TimeByHour": {
                      "end": new Date('2015-09-12T07:00:00.000Z'),
                      "start": new Date('2015-09-12T06:00:00.000Z'),
                      "type": "TIME_RANGE"
                    }
                  },
                  {
                    "Channel": "vi",
                    "ChannelIsDE": false,
                    "Count": 11833,
                    "NewPage": null,
                    "TimeByHour": {
                      "end": new Date('2015-09-12T08:00:00.000Z'),
                      "start": new Date('2015-09-12T07:00:00.000Z'),
                      "type": "TIME_RANGE"
                    }
                  },
                  {
                    "Channel": "vi",
                    "ChannelIsDE": false,
                    "Count": 6411,
                    "NewPage": null,
                    "TimeByHour": {
                      "end": new Date('2015-09-12T18:00:00.000Z'),
                      "start": new Date('2015-09-12T17:00:00.000Z'),
                      "type": "TIME_RANGE"
                    }
                  },
                  {
                    "Channel": "vi",
                    "ChannelIsDE": false,
                    "Count": 4942,
                    "NewPage": null,
                    "TimeByHour": {
                      "end": new Date('2015-09-12T16:00:00.000Z'),
                      "start": new Date('2015-09-12T15:00:00.000Z'),
                      "type": "TIME_RANGE"
                    }
                  }
                ]
              }
            ]);
            return testComplete();
          }
        ).done();
      });
    });


  describe("introspection", function() {
    var basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'druid',
          dataSource: 'wikipedia',
          timeAttribute: 'time',
          context: null,
          filter: $('time').in(TimeRange.fromJS({
            start: new Date("2015-09-12T00:00:00Z"),
            end: new Date("2015-09-13T00:00:00Z")
          })),
          druidVersion: info.druidVersion,
          requester: druidRequester
        })
      }
    });

    it("works with introspection", function(testComplete) {
      var ex = ply()
        .apply("wiki", $('wiki').filter($("channel").is('en')))
        .apply('Count', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply(
          'Time',
          $("wiki").split($("time").timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$Timestamp', 'ascending')
            .limit(3)
            .apply(
              'Pages',
              $("wiki").split("$page", 'Page')
                .apply('Count', '$wiki.sum($count)')
                .sort('$Count', 'descending')
                .limit(2)
            )
        );

      return basicExecutor(ex).then(function(result) {
          expect(result.toJS()).to.deep.equal([
            {
              "Count": 114711,
              "Time": [
                {
                  "Pages": [
                    {
                      "Count": 12,
                      "Page": "User talk:Dudeperson176123"
                    },
                    {
                      "Count": 8,
                      "Page": "User:Attar-Aram syria/sandbox"
                    }
                  ],
                  "Timestamp": {
                    "end": new Date('2015-09-12T01:00:00.000Z'),
                    "start": new Date('2015-09-12T00:00:00.000Z'),
                    "type": "TIME_RANGE"
                  },
                  "TotalAdded": 331925
                },
                {
                  "Pages": [
                    {
                      "Count": 17,
                      "Page": "John Adams"
                    },
                    {
                      "Count": 17,
                      "Page": "User:King Lui"
                    }
                  ],
                  "Timestamp": {
                    "end": new Date('2015-09-12T02:00:00.000Z'),
                    "start": new Date('2015-09-12T01:00:00.000Z'),
                    "type": "TIME_RANGE"
                  },
                  "TotalAdded": 1418072
                },
                {
                  "Pages": [
                    {
                      "Count": 28,
                      "Page": "Wikipedia:Administrators' noticeboard/Incidents"
                    },
                    {
                      "Count": 18,
                      "Page": "2015 World Wrestling Championships"
                    }
                  ],
                  "Timestamp": {
                    "end": new Date('2015-09-12T03:00:00.000Z'),
                    "start": new Date('2015-09-12T02:00:00.000Z'),
                    "type": "TIME_RANGE"
                  },
                  "TotalAdded": 3045966
                }
              ],
              "TotalAdded": 32553107
            }
          ]);
          return testComplete();
        }
      ).done();
    });
  });
});
