var { expect } = require("chai");
var { sane } = require('../utils');

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var { druidRequesterFactory } = require('plywood-druid-requester');

var plywood = require('../../build/plywood');
var { External, DruidExternal, TimeRange, $, ply, basicExecutorFactory, helper } = plywood;

var info = require('../info');

var druidRequester = druidRequesterFactory({
  host: info.druidHost
});

// druidRequester = helper.verboseRequesterFactory({
//   requester: druidRequester
// });

describe("Druid Functional", function() {
  this.timeout(10000);

  var wikiAttributes = [
    { "name": "time", "type": "TIME" },
    { "name": "added", "makerAction":{"action": "sum", "expression":{"name": "added", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "channel", "type": "STRING" },
    { "name": "cityName", "type": "STRING" },
    { "name": "comment", "type": "STRING" },
    { "name": "commentLength", "type": "NUMBER" },
    { "name": "count", "makerAction":{"action": "count"}, "type": "NUMBER", "unsplitable":true },
    { "name": "countryIsoCode", "type": "STRING" },
    { "name": "countryName", "type": "STRING" },
    { "name": "deleted", "makerAction":{"action": "sum", "expression":{"name": "deleted", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "delta", "makerAction":{"action": "sum", "expression":{"name": "delta", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "deltaBucket100", "type": "NUMBER" },
    { "name": "deltaByTen", "makerAction":{"action": "sum", "expression":{"name": "deltaByTen", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "delta_hist", "special": "histogram", "type": "NUMBER" },
    { "name": "isAnonymous", "type": "BOOLEAN" },
    { "name": "isMinor", "type": "BOOLEAN" },
    { "name": "isNew", "type": "BOOLEAN" },
    { "name": "isRobot", "type": "BOOLEAN" },
    { "name": "isUnpatrolled", "type": "BOOLEAN" },
    { "name": "max_delta", "makerAction":{"action": "max", "expression":{"name": "max_delta", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "metroCode", "type": "STRING" },
    { "name": "min_delta", "makerAction":{"action": "min", "expression":{"name": "min_delta", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "namespace", "type": "STRING" },
    { "name": "page", "type": "STRING" },
    { "name": "page_unique", "special": "unique", "type": "STRING" },
    { "name": "regionIsoCode", "type": "STRING" },
    { "name": "regionName", "type": "STRING" },
    { "name": "sometimeLater", "type": "TIME" },
    { "name": "user", "type": "STRING" },
    { "name": "userChars", "type": "SET/STRING" },
    { "name": "user_theta", "special": "theta", "type": "STRING" },
    { "name": "user_unique", "special": "unique", "type": "STRING" }
  ];

  describe("source list", () => {
    it("does a source list", (testComplete) => {
      DruidExternal.getSourceList(druidRequester)
        .then((sources) => {
          expect(sources).to.deep.equal(['wikipedia', 'wikipedia-compact']);
          testComplete();
        })
        .done()
    });
  });


  describe("defined attributes in datasource", () => {
    var wiki = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      context: {
        timeout: 10000,
        useCache: false,
        populateCache: false
      },
      attributes: wikiAttributes,
      filter: $('time').in(TimeRange.fromJS({
        start: new Date("2015-09-12T00:00:00Z"),
        end: new Date("2015-09-13T00:00:00Z")
      })),
      version: info.druidVersion,
      allowSelectQueries: true
    }, druidRequester);

    var wikiCompact = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia-compact',
      timeAttribute: 'time',
      context: {
        timeout: 10000,
        useCache: false,
        populateCache: false
      },
      attributes: [
        { name: 'time', type: 'TIME', makerAction: { action: 'timeFloor', duration: 'PT1H', timezone: 'Etc/UTC' } },
        { name: 'channel', type: 'STRING' },
        { name: 'isNew', type: 'BOOLEAN' },
        { name: 'isAnonymous', type: 'BOOLEAN' },
        { name: 'commentLength', type: 'NUMBER' },
        { name: 'metroCode', type: 'STRING' },
        { name: 'cityName', type: 'STRING' },

        { name: 'count', type: 'NUMBER', unsplitable: true },
        { name: 'delta', type: 'NUMBER', unsplitable: true },
        { name: 'added', type: 'NUMBER', unsplitable: true },
        { name: 'deleted', type: 'NUMBER', unsplitable: true },
        { name: 'page_unique', special: 'unique', unsplitable: true }
      ],
      filter: $('time').in(TimeRange.fromJS({
        start: new Date("2015-09-12T00:00:00Z"),
        end: new Date("2015-09-13T00:00:00Z")
      })),
      concealBuckets: true,
      version: info.druidVersion,
      allowSelectQueries: true
    }, druidRequester);

    var basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: wiki.addDelegate(wikiCompact)
      }
    });

    it("works basic case to CSV", (testComplete) => {
      var ex = ply()
        .apply("wiki", $('wiki').filter($("channel").is('en')))
        .apply(
          'Cities',
          $("wiki").split("$cityName", 'City')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$TotalAdded', 'descending')
            .limit(2)
        );

      basicExecutor(ex)
        .then((result) => {
          expect(result.toCSV({ lineBreak: '\n' })).to.deep.equal(sane`
            City,TotalAdded
            null,31529720
            Mineola,50836
          `);
          testComplete();
        })
        .done();
    });

    it("works timePart case", (testComplete) => {
      var ex = ply()
        .apply("wiki", $('wiki').filter($("channel").is('en')))
        .apply(
          'HoursOfDay',
          $("wiki").split("$time.timePart(HOUR_OF_DAY, 'Etc/UTC')", 'HourOfDay')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$TotalAdded', 'descending')
            .limit(3)
        );

      basicExecutor(ex)
        .then((result) => {
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
          testComplete();
        })
        .done();
    });

    it("works in advanced case", (testComplete) => {
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

      basicExecutor(ex)
        .then((result) => {
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
                  "Count": 241,
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
          testComplete();
        })
        .done();
    });

    it("works with uniques", (testComplete) => {
      var ex = ply()
        .apply('UniquePages1', $('wiki').countDistinct("$page"))
        .apply('UniquePages2', $('wiki').countDistinct("$page_unique"))
        .apply('UniqueUsers1', $('wiki').countDistinct("$user"))
        .apply('UniqueUsers2', $('wiki').countDistinct("$user_unique"))
        .apply('UniqueUsers3', $('wiki').countDistinct("$user_theta"))
        .apply('Diff_Users_1_2', '$UniqueUsers1 - $UniqueUsers2')
        .apply('Diff_Users_2_3', '$UniqueUsers2 - $UniqueUsers3')
        .apply('Diff_Users_1_3', '$UniqueUsers1 - $UniqueUsers3');

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Diff_Users_1_2": 1507.8377206866207,
              "Diff_Users_1_3": 1055.998647896362,
              "Diff_Users_2_3": -451.8390727902588,
              "UniquePages1": 279107.1992807899,
              "UniquePages2": 281588.11316378025,
              "UniqueUsers1": 39220.49269175933,
              "UniqueUsers2": 37712.65497107271,
              "UniqueUsers3": 38164.49404386297
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with filtered unique (in expression)", (testComplete) => {
      var ex = ply()
        .apply('UniquePagesEn', $('wiki').filter('$channel == en').countDistinct("$page"))
        .apply('UniquePagesEnOver2', '$UniquePagesEn / 2');

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "UniquePagesEn": 63849.8464587151,
              "UniquePagesEnOver2": 31924.92322935755
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with filtered uniques", (testComplete) => {
      var ex = ply()
        .apply('UniquePagesEn', $('wiki').filter('$channel == en').countDistinct("$page"))
        .apply('UniquePagesEs', $('wiki').filter('$channel == es').countDistinct("$page_unique"))
        .apply('UniquePagesChannelDiff', '$UniquePagesEn - $UniquePagesEs');

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "UniquePagesEn": 63849.8464587151,
              "UniquePagesEs": 6870.355969047973,
              "UniquePagesChannelDiff": 56979.49048966713
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with no applies in dimensions split dataset", (testComplete) => {
      var ex = ply()
        .apply(
          'Channels',
          $('wiki').split("$channel", 'Channel')
            .sort('$Channel', 'descending')
            .limit(2)
            .apply(
              'Users',
              $('wiki').split('$user', 'User')
                .apply('Count', $('wiki').sum('$count'))
                .sort('$Count', 'descending')
                .limit(2)
            )
        );

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Channels": [
                {
                  "Channel": "zh",
                  "Users": [
                    {
                      "Count": 3698,
                      "User": "Antigng-bot"
                    },
                    {
                      "Count": 503,
                      "User": "和平-bot"
                    }
                  ]
                },
                {
                  "Channel": "war",
                  "Users": [
                    {
                      "Count": 4,
                      "User": "JinJian"
                    },
                    {
                      "Count": 3,
                      "User": "Xqbot"
                    }
                  ]
                }
              ]
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with absolute", (testComplete) => {
      var ex = ply()
        .apply("Count", $('wiki').filter($("channel").is('en')).sum('$count'))
        .apply('Negate', $('Count').negate())
        .apply('Abs', $('Count').negate().absolute().negate().absolute());

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Abs": 114711,
              "Count": 114711,
              "Negate": -114711
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with split on a SET/STRING dimension", (testComplete) => {
      var ex = ply()
        .apply(
          'UserChars',
          $('wiki').split("$userChars", 'UserChar')
            .apply("Count", $('wiki').sum('$count'))
            .sort('$Count', 'descending')
            .limit(4)
        );

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "UserChars": [
                {
                  "Count": 223134,
                  "UserChar": "O"
                },
                {
                  "Count": 222676,
                  "UserChar": "A"
                },
                {
                  "Count": 216186,
                  "UserChar": "T"
                },
                {
                  "Count": 176986,
                  "UserChar": "B"
                }
              ]
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with all kinds of cool aggregates on totals level", (testComplete) => {
      var ex = ply()
        .apply("NumPages", $('wiki').countDistinct('$page'))
        .apply("NumEnPages", $('wiki').filter($("channel").is('en')).countDistinct('$page'))
        .apply("ChannelAdded", $('wiki').sum('$added'))
        .apply("ChannelENAdded", $('wiki').filter($("channel").is('en')).sum('$added'))
        .apply("ChannelENishAdded", $('wiki').filter($("channel").contains('en')).sum('$added'))
        .apply('Count', $('wiki').sum('$count'))
        .apply('CountSquareRoot', $('wiki').sum('$count').power(0.5))
        .apply('CountSquared', $('wiki').sum('$count').power(2))
        .apply('One', $('wiki').sum('$count').power(0))
        .apply('AddedByDeleted', $('wiki').sum('$added').divide($('wiki').sum('$deleted')))
        .apply('Delta95th', $('wiki').quantile('$delta_hist', 0.95))
        .apply('Delta99thX2', $('wiki').quantile('$delta_hist', 0.99).multiply(2));

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "AddedByDeleted": 24.909643797343193,
              "ChannelAdded": 97393743,
              "ChannelENAdded": 32553107,
              "ChannelENishAdded": 32553107,
              "Count": 392443,
              "CountSquareRoot": 626.4527117029664,
              "CountSquared": 154011508249,
              "NumEnPages": 63849.8464587151,
              "NumPages": 279107.1992807899,
              "One": 1,
              "Delta95th": 161.95517,
              "Delta99thX2": 328.9096984863281
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with all kinds of cool aggregates on split level", (testComplete) => {
      var ex = $('wiki').split('$isNew', 'isNew')
        .apply("NumPages", $('wiki').countDistinct('$page'))
        .apply("NumEnPages", $('wiki').filter($("channel").is('en')).countDistinct('$page'))
        .apply("ChannelAdded", $('wiki').sum('$added'))
        .apply("ChannelENAdded", $('wiki').filter($("channel").is('en')).sum('$added'))
        .apply("ChannelENishAdded", $('wiki').filter($("channel").contains('en')).sum('$added'))
        .apply('Count', $('wiki').sum('$count'))
        .apply('CountSquareRoot', $('wiki').sum('$count').power(0.5))
        .apply('CountSquared', $('wiki').sum('$count').power(2))
        .apply('One', $('wiki').sum('$count').power(0))
        .limit(3);

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "ChannelAdded": 53750772,
              "ChannelENAdded": 23136956,
              "ChannelENishAdded": 23136956,
              "Count": 368841,
              "CountSquareRoot": 607.3228136666694,
              "CountSquared": 136043683281,
              "NumEnPages": 57395.11747644384,
              "NumPages": 262439.60590555385,
              "One": 1,
              "isNew": false
            },
            {
              "ChannelAdded": 43642971,
              "ChannelENAdded": 9416151,
              "ChannelENishAdded": 9416151,
              "Count": 23602,
              "CountSquareRoot": 153.62942426501507,
              "CountSquared": 557054404,
              "NumEnPages": 8166.062824215849,
              "NumPages": 22270.407985514667,
              "One": 1,
              "isNew": true
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with no applies in time split dataset", (testComplete) => {
      var ex = ply()
        .apply(
          'ByHour',
          $('wiki').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
            .sort('$TimeByHour', 'ascending')
            .limit(3)
            .apply(
              'Users',
              $('wiki').split('$page', 'Page')
                .apply('Count', $('wiki').sum('$count'))
                .sort('$Count', 'descending')
                .limit(2)
            )
        );

      basicExecutor(ex)
        .then((result) => {
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
                      "Count": 12,
                      "Page": "User talk:Dudeperson176123"
                    },
                    {
                      "Count": 11,
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
                      "Count": 18,
                      "Page": "2015 World Wrestling Championships"
                    }
                  ]
                }
              ]
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("does not zero fill", (testComplete) => {
      var ex = $('wiki')
        .filter('$cityName == "El Paso"')
        .split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('Count', '$wiki.sum($count)')
        .sort('$TimeByHour', 'ascending');

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.have.length(2);
          testComplete();
        })
        .done();
    });

    it("works with time split with quantile", (testComplete) => {
      var ex = $('wiki')
        .filter('$cityName == "El Paso"')
        .split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('count', '$wiki.sum($count)')
        .apply('Delta95th', $('wiki').quantile('$delta_hist', 0.95))
        .sort('$TimeByHour', 'ascending')
        .limit(3);

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Delta95th": -39,
              "TimeByHour": {
                "end": new Date('2015-09-12T07:00:00.000Z'),
                "start": new Date('2015-09-12T06:00:00.000Z'),
                "type": "TIME_RANGE"
              },
              "count": 1
            },
            {
              "Delta95th": 0,
              "TimeByHour": {
                "end": new Date('2015-09-12T17:00:00.000Z'),
                "start": new Date('2015-09-12T16:00:00.000Z'),
                "type": "TIME_RANGE"
              },
              "count": 1
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with contains (case sensitive) filter", (testComplete) => {
      var ex = ply()
        .apply('wiki', $('wiki').filter($('page').contains('wiki')))
        .apply(
          'Pages',
          $('wiki').split($("page"), 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        );

      basicExecutor(ex)
        .then((result) => {
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
          testComplete();
        })
        .done();
    });

    it("works with contains(ignoreCase) filter", (testComplete) => {
      var ex = ply()
        .apply('wiki', $('wiki').filter($('page').contains('wiki', 'ignoreCase')))
        .apply(
          'Pages',
          $('wiki').split($("page"), 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        );

      basicExecutor(ex)
        .then((result) => {
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
          testComplete();
        })
        .done();
    });

    it("works with match() filter", (testComplete) => {
      var ex = ply()
        .apply('wiki', $('wiki').filter($('page').match('^.*Bot.*$')))
        .apply(
          'Pages',
          $('wiki').split($("page"), 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        );

      basicExecutor(ex)
        .then((result) => {
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
          testComplete();
        })
        .done();
    });

    it("works with split sort on string", (testComplete) => {
      var ex = ply()
        .apply(
          'Channels',
          $('wiki').split("$channel", 'Channel')
            .sort('$Channel', 'ascending')
            .limit(3)
        );

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Channels": [
                {
                  "Channel": "ar"
                },
                {
                  "Channel": "be"
                },
                {
                  "Channel": "bg"
                }
              ]
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with concat split", (testComplete) => {
      var ex = ply()
        .apply(
          'Pages',
          $('wiki').split("'!!!<' ++ $page ++ '>!!!'", 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        );

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Pages": [
                {
                  "Count": 318,
                  "Page": "!!!<Jeremy Corbyn>!!!"
                },
                {
                  "Count": 255,
                  "Page": "!!!<User:Cyde/List of candidates for speedy deletion/Subpage>!!!"
                },
                {
                  "Count": 228,
                  "Page": "!!!<Wikipedia:Administrators' noticeboard/Incidents>!!!"
                }
              ]
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with substr split", (testComplete) => {
      var ex = ply()
        .apply(
          'Pages',
          $('wiki').split("$page.substr(0,2)", 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        );

      basicExecutor(ex)
        .then((result) => {
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
          testComplete();
        })
        .done();
    });

    it("works with extract split", (testComplete) => {
      var ex = ply()
        .apply(
          'Pages',
          $('wiki').split($('page').extract('([0-9]+\\.[0-9]+\\.[0-9]+)'), 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        );

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Pages": [
                {
                  "Count": 387184,
                  "Page": null
                },
                {
                  "Count": 22,
                  "Page": "75.108.94"
                },
                {
                  "Count": 14,
                  "Page": "120.29.65"
                }
              ]
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with lookup split", (testComplete) => {
      var ex = ply()
        .apply(
          'Channels',
          $('wiki').split($('channel').lookup('channel-lookup'), 'Channel')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(4)
        )
        .apply(
          'ChannelFallbackLOL',
          $('wiki').split($('channel').lookup('channel-lookup').fallback('LOL'), 'Channel')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(4)
        )
        .apply(
          'ChannelFallbackSelf',
          $('wiki').split($('channel').lookup('channel-lookup').fallback('$channel'), 'Channel')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(4)
        );

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Channels": [
                { "Channel": null, "Count": 227040 },
                { "Channel": "English", "Count": 114711 },
                { "Channel": "French", "Count": 21285 },
                { "Channel": "Russian", "Count": 14031 }
              ],
              "ChannelFallbackLOL": [
                { "Channel": "LOL", "Count": 227040 },
                { "Channel": "English", "Count": 114711 },
                { "Channel": "French", "Count": 21285 },
                { "Channel": "Russian", "Count": 14031 }
              ],
              "ChannelFallbackSelf": [
                { "Channel": "English", "Count": 114711 },
                { "Channel": "vi", "Count": 99010 },
                { "Channel": "de", "Count": 25103 },
                { "Channel": "French", "Count": 21285 }
              ]
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with lookup IS filter", (testComplete) => {
      var ex = $('wiki').filter($('channel').lookup('channel-lookup').is('English')).sum('$count');

      basicExecutor(ex)
        .then((result) => {
          expect(result).to.equal(114711);
          testComplete();
        })
        .done();
    });

    it("works with lookup CONTAINS filter", (testComplete) => {
      var ex = $('wiki').filter($('channel').lookup('channel-lookup').contains('Eng', 'ignoreCase')).sum('$count');

      basicExecutor(ex)
        .then((result) => {
          expect(result).to.equal(114711);
          testComplete();
        })
        .done();
    });

    it("works with absolute number split", (testComplete) => {
      var ex = ply()
        .apply(
          'AbsSplitAsc',
          $('wiki').split($('commentLength').absolute(), 'AbsCommentLength')
            .apply('Count', '$wiki.sum($count)')
            .sort('$AbsCommentLength', 'ascending')
            .limit(3)
        )
        .apply(
          'AbsSplitDesc',
          $('wiki').split($('commentLength').absolute(), 'AbsCommentLength')
            .apply('Count', '$wiki.sum($count)')
            .sort('$AbsCommentLength', 'descending')
            .limit(3)
        );

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "AbsSplitAsc": [
                {
                  "AbsCommentLength": 1,
                  "Count": 734
                },
                {
                  "AbsCommentLength": 2,
                  "Count": 1456
                },
                {
                  "AbsCommentLength": 3,
                  "Count": 1976
                }
              ],
              "AbsSplitDesc": [
                {
                  "AbsCommentLength": 255,
                  "Count": 193
                },
                {
                  "AbsCommentLength": 254,
                  "Count": 59
                },
                {
                  "AbsCommentLength": 253,
                  "Count": 243
                }
              ]
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with bucketed number split", (testComplete) => {
      var ex = ply()
        .apply(
          'BucketSplitAsc',
          $('wiki').split($('commentLength').numberBucket(5), 'Bucket')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Bucket', 'ascending')
            .limit(3)
        )
        .apply(
          'BucketSplitDesc',
          $('wiki').split($('commentLength').numberBucket(5), 'Bucket')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Bucket', 'descending')
            .limit(3)
        );

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "BucketSplitAsc": [
                {
                  "Bucket": {
                    "end": 5,
                    "start": 0,
                    "type": "NUMBER_RANGE"
                  },
                  "Count": 6522
                },
                {
                  "Bucket": {
                    "end": 10,
                    "start": 5,
                    "type": "NUMBER_RANGE"
                  },
                  "Count": 15003
                },
                {
                  "Bucket": {
                    "end": 15,
                    "start": 10,
                    "type": "NUMBER_RANGE"
                  },
                  "Count": 70628
                }
              ],
              "BucketSplitDesc": [
                {
                  "Bucket": {
                    "end": 260,
                    "start": 255,
                    "type": "NUMBER_RANGE"
                  },
                  "Count": 193
                },
                {
                  "Bucket": {
                    "end": 255,
                    "start": 250,
                    "type": "NUMBER_RANGE"
                  },
                  "Count": 556
                },
                {
                  "Bucket": {
                    "end": 250,
                    "start": 245,
                    "type": "NUMBER_RANGE"
                  },
                  "Count": 1687
                }
              ]
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("can timeBucket a primary time column", (testComplete) => {
      var ex = ply()
        .apply(
          'Time',
          $("wiki").split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeCol')
            .sort('$TimeCol', 'descending')
            .limit(2)
        );

      basicExecutor(ex)
        .then((result) => {
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
          testComplete();
        })
        .done();
    });

    it("can timeBucket a secondary time column", (testComplete) => {
      var ex = ply()
        .apply(
          'TimeLater',
          $("wiki").split($("sometimeLater").timeBucket('PT1H', 'Etc/UTC'), 'SometimeLater')
            .limit(5)
        );

      basicExecutor(ex)
        .then((result) => {
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
          testComplete();
        })
        .done();
    });

    it("can timeBucket a secondary time column (complex duration, tz)", (testComplete) => {
      var ex = ply()
        .apply(
          'TimeLater',
          $("wiki").split($("sometimeLater").timeBucket('PT3H', 'Asia/Kathmandu'), 'SometimeLater')
            .limit(5)
        );

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "TimeLater": [
                {
                  "SometimeLater": {
                    "end": new Date('2016-09-12T03:15:00.000Z'),
                    "start": new Date('2016-09-12T00:15:00.000Z'),
                    "type": "TIME_RANGE"
                  }
                },
                {
                  "SometimeLater": {
                    "end": new Date('2016-09-12T06:15:00.000Z'),
                    "start": new Date('2016-09-12T03:15:00.000Z'),
                    "type": "TIME_RANGE"
                  }
                },
                {
                  "SometimeLater": {
                    "end": new Date('2016-09-12T09:15:00.000Z'),
                    "start": new Date('2016-09-12T06:15:00.000Z'),
                    "type": "TIME_RANGE"
                  }
                },
                {
                  "SometimeLater": {
                    "end": new Date('2016-09-12T12:15:00.000Z'),
                    "start": new Date('2016-09-12T09:15:00.000Z'),
                    "type": "TIME_RANGE"
                  }
                },
                {
                  "SometimeLater": {
                    "end": new Date('2016-09-12T15:15:00.000Z'),
                    "start": new Date('2016-09-12T12:15:00.000Z'),
                    "type": "TIME_RANGE"
                  }
                }
              ]
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("can do a sub-query", (testComplete) => {
      var ex = ply()
        .apply(
          'data1',
          $("wiki").split($("time").timeFloor('PT1H', 'Etc/UTC'), 'TimeCol')
            .apply('Count', '$wiki.sum($count)')
            .sort('$TimeCol', 'descending')
            .limit(2)
        )
        .apply('MinCount', '$data1.min($Count)')
        .apply('MaxCount', '$data1.max($Count)');

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "MaxCount": 15906,
              "MinCount": 14814,
              "data1": [
                {
                  "Count": 14814,
                  "TimeCol": {
                    "type": "TIME",
                    "value": new Date('2015-09-12T23:00:00.000Z')
                  }
                },
                {
                  "Count": 15906,
                  "TimeCol": {
                    "type": "TIME",
                    "value": new Date('2015-09-12T22:00:00.000Z')
                  }
                }
              ]
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works multi-dimensional GROUP BYs", (testComplete) => {
      var ex = ply()
        .apply("wiki", $('wiki').filter($("channel").isnt('en')))
        .apply(
          'Groups',
          $("wiki")
            .split({
              'Channel': "$channel",
              'TimeByHour': '$time.timeBucket(PT2H)',
              'IsNew': '$isNew',
              'ChannelIsDE': "$channel == 'de'"
            })
            .apply('Count', $('wiki').sum('$count'))
            .sort('$Count', 'descending')
            .limit(4)
        );

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Groups": [
                {
                  "Channel": "vi",
                  "ChannelIsDE": false,
                  "Count": 24258,
                  "IsNew": false,
                  "TimeByHour": {
                    "end": new Date('2015-09-12T08:00:00.000Z'),
                    "start": new Date('2015-09-12T06:00:00.000Z'),
                    "type": "TIME_RANGE"
                  }
                },
                {
                  "Channel": "vi",
                  "ChannelIsDE": false,
                  "Count": 11215,
                  "IsNew": false,
                  "TimeByHour": {
                    "end": new Date('2015-09-12T18:00:00.000Z'),
                    "start": new Date('2015-09-12T16:00:00.000Z'),
                    "type": "TIME_RANGE"
                  }
                },
                {
                  "Channel": "vi",
                  "ChannelIsDE": false,
                  "Count": 9246,
                  "IsNew": false,
                  "TimeByHour": {
                    "end": new Date('2015-09-12T16:00:00.000Z'),
                    "start": new Date('2015-09-12T14:00:00.000Z'),
                    "type": "TIME_RANGE"
                  }
                },
                {
                  "Channel": "vi",
                  "ChannelIsDE": false,
                  "Count": 8917,
                  "IsNew": false,
                  "TimeByHour": {
                    "end": new Date('2015-09-12T10:00:00.000Z'),
                    "start": new Date('2015-09-12T08:00:00.000Z'),
                    "type": "TIME_RANGE"
                  }
                }
              ]
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works nested GROUP BYs", (testComplete) => {
      var ex = $('wiki')
        .split({ 'isNew': '$isNew', 'isRobot': '$isRobot' })
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .split('$isNew', 'isNew', 'data')
        .apply('SumTotalEdits', '$data.sum($TotalEdits)');

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "SumTotalEdits": 368841,
              "isNew": false
            },
            {
              "SumTotalEdits": 23602,
              "isNew": true
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with raw (SELECT)", (testComplete) => {
      var ex = $('wiki').filter('$cityName == "El Paso"');

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "added": 0,
              "channel": "en",
              "cityName": "El Paso",
              "comment": "/* Clubs and organizations */",
              "commentLength": 29,
              "count": 1,
              "countryIsoCode": "US",
              "countryName": "United States",
              "deleted": 39,
              "delta": -39,
              "deltaBucket100": -100,
              "deltaByTen": -3.9000000953674316,
              "delta_hist": null,
              "isAnonymous": true,
              "isMinor": false,
              "isNew": false,
              "isRobot": false,
              "isUnpatrolled": false,
              "max_delta": -39,
              "metroCode": "765",
              "min_delta": -39,
              "namespace": "Main",
              "page": "Clint High School",
              "page_unique": null,
              "regionIsoCode": "TX",
              "regionName": "Texas",
              "sometimeLater": {
                "type": "TIME",
                "value": new Date('2016-09-12T06:05:00Z')
              },
              "time": {
                "type": "TIME",
                "value": new Date('2015-09-12T06:05:00.000Z')
              },
              "user": "104.58.160.128",
              "userChars": {
                "elements": [".", "0", "1", "2", "4", "5", "6", "8"],
                "setType": "STRING",
                "type": "SET"
              },
              "user_theta": null,
              "user_unique": null
            },
            {
              "added": 0,
              "channel": "en",
              "cityName": "El Paso",
              "comment": "/* Early life */ spelling",
              "commentLength": 25,
              "count": 1,
              "countryIsoCode": "US",
              "countryName": "United States",
              "deleted": 0,
              "delta": 0,
              "deltaBucket100": 0,
              "deltaByTen": 0,
              "delta_hist": null,
              "isAnonymous": true,
              "isMinor": false,
              "isNew": false,
              "isRobot": false,
              "isUnpatrolled": false,
              "max_delta": 0,
              "metroCode": "765",
              "min_delta": 0,
              "namespace": "Main",
              "page": "Reggie Williams (linebacker)",
              "page_unique": null,
              "regionIsoCode": "TX",
              "regionName": "Texas",
              "sometimeLater": {
                "type": "TIME",
                "value": new Date('2016-09-12T16:14:00Z')
              },
              "time": {
                "type": "TIME",
                "value": new Date('2015-09-12T16:14:00.000Z')
              },
              "user": "67.10.203.15",
              "userChars": {
                "elements": [".", "0", "1", "2", "3", "5", "6", "7"],
                "setType": "STRING",
                "type": "SET"
              },
              "user_theta": null,
              "user_unique": null
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("gets the right number of results in a big raw (SELECT ascending)", (testComplete) => {
      var limit = 15001;
      var ex = $('wiki')
        .filter('$cityName == null')
        .select('time', 'cityName')
        .sort('$time', 'ascending')
        .limit(limit);

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().length).to.equal(limit);
          testComplete();
        })
        .done();
    });

    it("gets the right number of results in a big raw (SELECT descending)", (testComplete) => {
      var limit = 15001;
      var ex = $('wiki')
        .filter('$cityName == null')
        .select('time', 'cityName')
        .sort('$time', 'descending')
        .limit(limit);

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().length).to.equal(limit);
          testComplete();
        })
        .done();
    });

    it("works with raw (SELECT) inside a split", (testComplete) => {
      var ex = $('wiki')
        .filter('$cityName.match("^San")')
        .split('$cityName', 'City')
        .apply('Edits', '$wiki.sum($count)')
        .sort('$Edits', 'descending')
        .limit(2)
        .apply(
          'Latest2Events',
          $('wiki').sort('$time', 'descending')
            .select("time", "channel", "commentLength")
            .limit(3)
        );

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "City": "Santiago",
              "Edits": 135,
              "Latest2Events": [
                {
                  "channel": "es",
                  "commentLength": 15,
                  "time": {
                    "type": "TIME",
                    "value": new Date('2015-09-12T23:35:00.000Z')
                  }
                },
                {
                  "channel": "es",
                  "commentLength": 73,
                  "time": {
                    "type": "TIME",
                    "value": new Date('2015-09-12T23:17:00.000Z')
                  }
                },
                {
                  "channel": "es",
                  "commentLength": 18,
                  "time": {
                    "type": "TIME",
                    "value": new Date('2015-09-12T23:14:00.000Z')
                  }
                }
              ]
            },
            {
              "City": "San Juan",
              "Edits": 41,
              "Latest2Events": [
                {
                  "channel": "en",
                  "commentLength": 20,
                  "time": {
                    "type": "TIME",
                    "value": new Date('2015-09-12T22:57:00.000Z')
                  }
                },
                {
                  "channel": "en",
                  "commentLength": 20,
                  "time": {
                    "type": "TIME",
                    "value": new Date('2015-09-12T22:55:00.000Z')
                  }
                },
                {
                  "channel": "en",
                  "commentLength": 15,
                  "time": {
                    "type": "TIME",
                    "value": new Date('2015-09-12T19:42:00.000Z')
                  }
                }
              ]
            }
          ]);
          testComplete();
        })
        .done();
    });

  });


  describe("incorrect user chars", () => {
    var wikiUserCharAsNumber = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      allowEternity: true,
      attributes: [
        { name: 'time', type: 'TIME' },
        { name: "channel", type: "STRING" },
        { name: 'userChars', type: 'NUMBER' }, // This is incorrect
        { name: 'count', type: 'NUMBER', unsplitable: true }
      ]
    }, druidRequester);

    it("works with number addition", (testComplete) => {
      var ex = $('wiki').split("$userChars + 10", 'U')
        .apply('Count', '$wiki.sum($count)')
        .sort('$Count', 'descending')
        .limit(3);

      ex.compute({ wiki: wikiUserCharAsNumber })
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Count": 2618887,
              "U": null
            },
            {
              "Count": 75475,
              "U": 10
            },
            {
              "Count": 68663,
              "U": 11
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with number bucketing", (testComplete) => {
      var ex = $('wiki').split("$userChars.numberBucket(5, 2.5)", 'U')
        .apply('Count', '$wiki.sum($count)')
        .sort('$Count', 'descending')
        .limit(3);

      ex.compute({ wiki: wikiUserCharAsNumber })
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Count": 2618887,
              "U": null
            },
            {
              "Count": 189960,
              "U": {
                "end": 2.5,
                "start": -2.5,
                "type": "NUMBER_RANGE"
              }
            },
            {
              "Count": 151159,
              "U": {
                "end": 7.5,
                "start": 2.5,
                "type": "NUMBER_RANGE"
              }
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works with power", (testComplete) => {
      var ex = $('wiki').split("$userChars.power(2)", 'U')
        .apply('Count', '$wiki.sum($count)')
        .sort('$Count', 'descending')
        .limit(3);

      ex.compute({ wiki: wikiUserCharAsNumber })
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Count": 2618887,
              "U": null
            },
            {
              "Count": 75475,
              "U": 0
            },
            {
              "Count": 68663,
              "U": 1
            }
          ]);
          testComplete();
        })
        .done();
    });

  });


  describe("introspection", () => {
    var wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      filter: $('time').in(TimeRange.fromJS({
        start: new Date("2015-09-12T00:00:00Z"),
        end: new Date("2015-09-13T00:00:00Z")
      })),
      attributeOverrides: [
        { "name": "sometimeLater", "type": "TIME" },
        { "name": "commentLength", "type": "NUMBER" },
        { "name": "deltaBucket100", "type": "NUMBER" },
        { "name": "isAnonymous", "type": "BOOLEAN" },
        { "name": "isMinor", "type": "BOOLEAN" },
        { "name": "isNew", "type": "BOOLEAN" },
        { "name": "isRobot", "type": "BOOLEAN" },
        { "name": "isUnpatrolled", "type": "BOOLEAN" }
      ]
    }, druidRequester);

    var basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: wikiExternal
      }
    });

    it("introspects version and attributes", (testComplete) => {
      wikiExternal.introspect()
        .then((introspectedExternal) => {
          expect(introspectedExternal.version).to.equal(info.druidVersion);
          expect(introspectedExternal.toJS().attributes).to.deep.equal(wikiAttributes);
          testComplete();
        })
        .done();
    });

    it("works with introspection", (testComplete) => {
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

      basicExecutor(ex)
        .then((result) => {
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
          testComplete();
        })
        .done();
    });
  });
});
