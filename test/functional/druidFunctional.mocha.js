/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2017 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { expect } = require("chai");
let { sane } = require('../utils');

let { druidRequesterFactory } = require('plywood-druid-requester');

let plywood = require('../plywood');
let { External, DruidExternal, TimeRange, $, i$, ply, basicExecutorFactory, verboseRequesterFactory, Expression } = plywood;

let info = require('../info');

let druidRequester = druidRequesterFactory({
  host: info.druidHost
});

// druidRequester = verboseRequesterFactory({
//   requester: druidRequester
// });

describe("Druid Functional", function() {
  this.timeout(10000);

  let wikiAttributes = [
    { "name": "time", "type": "TIME" },
    { "name": "added", "maker":{"op": "sum", "expression":{"name": "added", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "channel", "type": "STRING" },
    { "name": "cityName", "type": "STRING" },
    { "name": "comment", "type": "STRING" },
    { "name": "commentLength", "type": "NUMBER" },
    { "name": "commentLengthStr", "type": "STRING" },
    { "name": "count", "maker":{"op": "count"}, "type": "NUMBER", "unsplitable":true },
    { "name": "countryIsoCode", "type": "STRING" },
    { "name": "countryName", "type": "STRING" },
    { "name": "deleted", "maker":{"op": "sum", "expression":{"name": "deleted", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "delta", "maker":{"op": "sum", "expression":{"name": "delta", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "deltaBucket100", "type": "NUMBER" },
    { "name": "deltaByTen", "maker":{"op": "sum", "expression":{"name": "deltaByTen", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "delta_hist", "special": "histogram", "type": "NUMBER" },
    { "name": "isAnonymous", "type": "BOOLEAN" },
    { "name": "isMinor", "type": "BOOLEAN" },
    { "name": "isNew", "type": "BOOLEAN" },
    { "name": "isRobot", "type": "BOOLEAN" },
    { "name": "isUnpatrolled", "type": "BOOLEAN" },
    { "name": "max_delta", "maker":{"op": "max", "expression":{"name": "max_delta", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "metroCode", "type": "STRING" },
    { "name": "min_delta", "maker":{"op": "min", "expression":{"name": "min_delta", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
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

  let customTransforms = {
      sliceLastChar: {
        extractionFn: {
          "type" : "javascript",
          "function" : "function(x) { return x.slice(-1) }"
        }
      },
      getLastChar: {
        extractionFn: {
          "type" : "javascript",
          "function" : "function(x) { return x.charAt(x.length - 1) }"
        }
      },
      timesTwo: {
        extractionFn: {
          "type" : "javascript",
          "function" : "function(x) { return x * 2 }"
        }
      },
      concatWithConcat: {
        extractionFn: {
          "type" : "javascript",
          "function" : "function(x) { return x.concat('concat') }"
        }
      }
    };

  describe("source list", () => {
    it("does a source list", () => {
      return DruidExternal.getSourceList(druidRequester)
        .then((sources) => {
          expect(sources).to.deep.equal(['wikipedia', 'wikipedia-compact']);
        });
    });
  });


  describe("defined attributes in datasource", () => {
    let wiki = External.fromJS({
      engine: 'druid',
      source: 'wikipedia',
      timeAttribute: 'time',
      context: info.druidContext,
      attributes: wikiAttributes,
      customTransforms,
      filter: $('time').in(TimeRange.fromJS({
        start: new Date("2015-09-12T00:00:00Z"),
        end: new Date("2015-09-13T00:00:00Z")
      })),
      version: info.druidVersion,
      allowSelectQueries: true
    }, druidRequester);

    let wikiCompact = External.fromJS({
      engine: 'druid',
      source: 'wikipedia-compact',
      timeAttribute: 'time',
      context: info.druidContext,
      attributes: [
        { name: 'time', type: 'TIME', maker: { action: 'timeFloor', duration: 'PT1H', timezone: 'Etc/UTC' } },
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
      customTransforms,
      concealBuckets: true,
      version: info.druidVersion,
      allowSelectQueries: true
    }, druidRequester);

    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: wiki.addDelegate(wikiCompact)
      }
    });

    it("works basic case to CSV", () => {
      let ex = ply()
        .apply("wiki", $('wiki').filter($("channel").is('en')))
        .apply(
          'Cities',
          $("wiki").split("$cityName", 'City')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$TotalAdded', 'descending')
            .limit(2)
        );

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toCSV({ lineBreak: '\n' })).to.deep.equal(sane`
            City,TotalAdded
            NULL,31529720
            Mineola,50836
          `);
        });
    });

    it("aggregate and splits plus select work with ordering last split first", () => {
      let ex = $('wiki')
        .split({ 'isNew': '$isNew', 'isRobot': '$isRobot' })
        .apply('Count', $('wiki').sum('$count'))
        .apply('Page', $("wiki").split("$page", 'Page').limit(3))
        .select('Page', 'Count', 'isRobot', 'isNew')
        .limit(1);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.getNestedColumns().map((c => c.name))).to.deep.equal(['Page', 'Count', 'isRobot', 'isNew']);
        });
    });

    it("aggregate and splits plus select work with ordering 2", () => {
      let ex = $('wiki')
        .split({ 'isNew': '$isNew', 'isRobot': '$isRobot' })
        .apply('Count', $('wiki').sum('$count'))
        .apply('Page', $("wiki").split("$page", 'Page').limit(3))
        .select('isRobot', 'Page', 'isNew', 'Count')
        .limit(1);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.getNestedColumns().map((c => c.name))).to.deep.equal(['isRobot', 'Page', 'isNew', 'Count']);
        });
    });

    it("aggregate and splits plus select work with ordering, aggregate first", () => {
      let ex = $('wiki')
        .split({ 'isNew': '$isNew', 'isRobot': '$isRobot' })
        .apply('Count', $('wiki').sum('$count'))
        .apply('Page', $("wiki").split("$page", 'Page').limit(3))
        .select('Count', 'isRobot', 'Page', 'isNew')
        .limit(1);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.getNestedColumns().map((c => c.name))).to.deep.equal(['Count', 'isRobot', 'Page', 'isNew']);
        });
    });

    it("works timePart case", () => {
      let ex = ply()
        .apply("wiki", $('wiki').filter($("channel").is('en')))
        .apply(
          'HoursOfDay',
          $("wiki").split("$time.timePart(HOUR_OF_DAY, 'Etc/UTC')", 'HourOfDay')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$TotalAdded', 'descending')
            .limit(3)
        );

      return basicExecutor(ex)
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
        });
    });

    it("works with quarter call case", () => {
      let ex = $('wiki')
        .filter($("channel").is('en'))
        .split("$time.timePart(QUARTER, 'Etc/UTC')", 'Quarter');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Quarter": 3
            }
          ]);
        });
    });

    it("works with yearly call case long", () => {
      let ex = $('wiki')
        .split(i$('time').timeFloor('P3M'), 'tqr___time_ok');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "tqr___time_ok": {
                "type": "TIME",
                "value": new Date('2015-07-01T00:00:00.000Z')
              }
            }
          ]);
        });
    });

    it("works in advanced case", () => {
      let ex = ply()
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

      return basicExecutor(ex)
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
        });
    });

    it("works with case transform in filter split and apply", () => {
      let ex = $('wiki')
        .filter($("channel").transformCase('upperCase').is('EN'))
        .split($("page").transformCase('lowerCase'), 'page')
        .apply('SumIndexA', $('wiki').sum($("page").transformCase('upperCase').indexOf("A")))
        .limit(8);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "SumIndexA": -1,
              "page": "!t.o.o.h.!"
            },
            {
              "SumIndexA": 1,
              "page": "'ajde jano"
            },
            {
              "SumIndexA": 1,
              "page": "'asir region"
            },
            {
              "SumIndexA": 1,
              "page": "'asta bowen"
            },
            {
              "SumIndexA": 1,
              "page": "'atika wahbi al-khazraji"
            },
            {
              "SumIndexA": -2,
              "page": "'cue detective"
            },
            {
              "SumIndexA": -1,
              "page": "'from hell' letter"
            },
            {
              "SumIndexA": 2,
              "page": "'marriage, migration and gender'"
            }
          ]);
        });
    });

    it("works with custom transform in filter and split", () => {
      let ex = $('wiki')
        .filter($("page").customTransform('sliceLastChar').is('z'))
        .split($("page").customTransform('getLastChar'), 'lastChar')
        .limit(8);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "lastChar": "z"
            }
          ]);
        });
    });

    it("works with custom transform in filter and split for numeric dimension", () => {
      let ex = $('wiki')
        .filter($("commentLength").customTransform('concatWithConcat', 'STRING').is("'100concat'"))
        .split($("commentLength").customTransform('timesTwo', 'STRING'), "Times Two")
        .limit(8);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Times Two": "200"
            }
          ]);
        });
    });

    it("works with uniques", () => {
      let ex = ply()
        .apply('UniqueIsRobot', $('wiki').countDistinct("$isRobot"))
        .apply('UniqueUserChars', $('wiki').countDistinct("$userChars"))
        .apply('UniquePages1', $('wiki').countDistinct("$page"))
        .apply('UniquePages2', $('wiki').countDistinct("$page_unique"))
        .apply('UniqueUsers1', $('wiki').countDistinct("$user"))
        .apply('UniqueUsers2', $('wiki').countDistinct("$user_unique"))
        .apply('UniqueUsers3', $('wiki').countDistinct("$user_theta"))
        .apply('Diff_Users_1_2', '$UniqueUsers1 - $UniqueUsers2')
        .apply('Diff_Users_2_3', '$UniqueUsers2 - $UniqueUsers3')
        .apply('Diff_Users_1_3', '$UniqueUsers1 - $UniqueUsers3');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "UniqueIsRobot": 2.000977198748901,
              "UniqueUserChars": 1376.0314194627178,
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
        });
    });

    it("works with filtered unique (in expression)", () => {
      let ex = ply()
        .apply('UniquePagesEn', $('wiki').filter('$channel == en').countDistinct("$page"))
        .apply('UniquePagesEnOver2', '$UniquePagesEn / 2');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "UniquePagesEn": 63849.8464587151,
              "UniquePagesEnOver2": 31924.92322935755
            }
          ]);
        });
    });

    it("works with filtered uniques", () => {
      let ex = ply()
        .apply('UniquePagesEn', $('wiki').filter('$channel == en').countDistinct("$page"))
        .apply('UniquePagesEs', $('wiki').filter('$channel == es').countDistinct("$page_unique"))
        .apply('UniquePagesChannelDiff', '$UniquePagesEn - $UniquePagesEs');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "UniquePagesEn": 63849.8464587151,
              "UniquePagesEs": 6870.355969047973,
              "UniquePagesChannelDiff": 56979.49048966713
            }
          ]);
        });
    });

    it("works with multiple columns", () => {
      let ex = $('wiki').countDistinct("$channel ++ 'lol' ++ $user");

      return basicExecutor(ex)
        .then((result) => {
          expect(result).to.deep.equal(40082.255930715364);
        });
    });

    it("works with no applies in dimensions split dataset", () => {
      let ex = ply()
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

      return basicExecutor(ex)
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
        });
    });

    it("works with absolute", () => {
      let ex = ply()
        .apply("Count", $('wiki').filter($("channel").is('en')).sum('$count'))
        .apply('Negate', $('Count').negate())
        .apply('Abs', $('Count').negate().absolute().negate().absolute());

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Abs": 114711,
              "Count": 114711,
              "Negate": -114711
            }
          ]);
        });
    });

    it("works with split on a SET/STRING dimension", () => {
      let ex = ply()
        .apply(
          'UserChars',
          $('wiki').split("$userChars", 'UserChar')
            .apply("Count", $('wiki').sum('$count'))
            .sort('$Count', 'descending')
            .limit(4)
        );

      return basicExecutor(ex)
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
        });
    });

    it("works with all kinds of cool aggregates on totals level", () => {
      let ex = ply()
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

      return basicExecutor(ex)
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
        });
    });

    it("works with all kinds of cool aggregates on split level", () => {
      let ex = $('wiki').split('$isNew', 'isNew')
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

      return basicExecutor(ex)
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
        });
    });

    it("works with no applies in time split dataset", () => {
      let ex = ply()
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

      return basicExecutor(ex)
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
        });
    });

    it("does not zero fill", () => {
      let ex = $('wiki')
        .filter('$cityName == "El Paso"')
        .split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('Count', '$wiki.sum($count)')
        .sort('$TimeByHour', 'ascending');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.have.length(2);
        });
    });

    it("works with time split with quantile", () => {
      let ex = $('wiki')
        .filter('$cityName == "El Paso"')
        .split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('count', '$wiki.sum($count)')
        .apply('Delta95th', $('wiki').quantile('$delta_hist', 0.95))
        .sort('$TimeByHour', 'ascending')
        .limit(3);

      return basicExecutor(ex)
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
        });
    });

    it("works with contains (case sensitive) filter", () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('page').contains('wiki')))
        .apply(
          'Pages',
          $('wiki').split($("page"), 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        );

      return basicExecutor(ex)
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
        });
    });

    it("works with contains(ignoreCase) filter", () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('page').contains('wiki', 'ignoreCase')))
        .apply(
          'Pages',
          $('wiki').split($("page"), 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        );

      return basicExecutor(ex)
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
        });
    });

    it("works with match() filter", () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('page').match('^.*Bot.*$')))
        .apply(
          'Pages',
          $('wiki').split($("page"), 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        );

      return basicExecutor(ex)
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
        });
    });

    it("works with split sort on string", () => {
      let ex = ply()
        .apply(
          'Channels',
          $('wiki').split("$channel", 'Channel')
            .sort('$Channel', 'ascending')
            .limit(3)
        );

      return basicExecutor(ex)
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
        });
    });

    it("works with concat split", () => {
      let ex = ply()
        .apply(
          'Pages',
          $('wiki').split("'!!!<' ++ $page ++ '>!!!'", 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        );

      return basicExecutor(ex)
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
        });
    });

    it("works with substr split", () => {
      let ex = ply()
        .apply(
          'Pages',
          $('wiki').split("$page.substr(0,2)", 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        );

      return basicExecutor(ex)
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
        });
    });

    it("works with extract split", () => {
      let ex = ply()
        .apply(
          'Pages',
          $('wiki').split($('page').extract('([0-9]+\\.[0-9]+\\.[0-9]+)'), 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3)
        );

      return basicExecutor(ex)
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
        });
    });

    it("works with lookup split", () => {
      let ex = ply()
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

      return basicExecutor(ex)
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
        });
    });

    it("works with lookup IS filter", () => {
      let ex = $('wiki').filter($('channel').lookup('channel-lookup').is('English')).sum('$count');

      return basicExecutor(ex)
        .then((result) => {
          expect(result).to.equal(114711);
        });
    });

    it("works with lookup CONTAINS filter", () => {
      let ex = $('wiki').filter($('channel').lookup('channel-lookup').contains('Eng', 'ignoreCase')).sum('$count');

      return basicExecutor(ex)
        .then((result) => {
          expect(result).to.equal(114711);
        });
    });

    it("works with string manipulation after cast action", () => {
      let ex = $('wiki').filter($('deltaBucket100').absolute().cast('STRING').substr(0,5).cast('NUMBER').is(1000)).sum('$count');

      return basicExecutor(ex)
        .then((result) => {
          expect(result).to.equal(1621);
        });
    });

    it("works with numeric fallback", () => {
      let ex = $('wiki').sum('($added / ($added - $added)).fallback(10)');

      return basicExecutor(ex)
        .then((result) => {
          expect(result).to.equal(109199 * 10);
        });
    });

    it("works with absolute number split", () => {
      let ex = ply()
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

      return basicExecutor(ex)
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
        });
    });

    it("works with bucketed number split", () => {
      let ex = ply()
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

      return basicExecutor(ex)
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
        });
    });

    it("works with bucketed split on derived column", () => {
      let ex = ply()
        .apply(
          'BucketSplitAsc',
          $('wiki').split($('comment').length().numberBucket(5), 'Bucket')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Bucket', 'ascending')
            .limit(3)
        )
        .apply(
          'BucketSplitDesc',
          $('wiki').split($('comment').length().numberBucket(5), 'Bucket')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Bucket', 'descending')
            .limit(3)
        );

      return basicExecutor(ex)
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
                  "Count": 6521
                },
                {
                  "Bucket": {
                    "end": 10,
                    "start": 5,
                    "type": "NUMBER_RANGE"
                  },
                  "Count": 15004
                },
                {
                  "Bucket": {
                    "end": 15,
                    "start": 10,
                    "type": "NUMBER_RANGE"
                  },
                  "Count": 70627
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
        });
    });

    it("can timeBucket a primary time column", () => {
      let ex = ply()
        .apply(
          'Time',
          $("wiki").split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeCol')
            .sort('$TimeCol', 'descending')
            .limit(2)
        );

      return basicExecutor(ex)
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
        });
    });

    it("can timeBucket a secondary time column", () => {
      let ex = ply()
        .apply(
          'TimeLater',
          $("wiki").split($("sometimeLater").timeBucket('PT1H', 'Etc/UTC'), 'SometimeLater')
            .limit(5)
        );

      return basicExecutor(ex)
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
        });
    });

    it("can timeBucket a secondary time column (complex duration, tz)", () => {
      let ex = ply()
        .apply(
          'TimeLater',
          $("wiki").split($("sometimeLater").timeBucket('PT3H', 'Asia/Kathmandu'), 'SometimeLater')
            .limit(5)
        );

      return basicExecutor(ex)
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
        });
    });

    it("can do a sub-query", () => {
      let ex = ply()
        .apply(
          'data1',
          $("wiki").split($("time").timeFloor('PT1H', 'Etc/UTC'), 'TimeCol')
            .apply('Count', '$wiki.sum($count)')
            .sort('$TimeCol', 'descending')
            .limit(2)
        )
        .apply('MinCount', '$data1.min($Count)')
        .apply('MaxCount', '$data1.max($Count)');

      return basicExecutor(ex)
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
        });
    });

    it("works multi-dimensional GROUP BYs", () => {
      let ex = ply()
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

      return basicExecutor(ex)
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
        });
    });

    it("works multi-dimensional GROUP BYs with time", () => {
      let ex = ply()
        .apply("wiki", $('wiki').filter($("channel").isnt('en')))
        .apply(
          'Groups',
          $("wiki")
            .split({
              'channel': "$channel",
              '__time': '$time.timeBucket(PT2H)',
            })
            .apply('Count', $('wiki').sum('$count'))
            .sort('$Count', 'descending')
            .limit(4)
        );

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Groups": [
                {
                  "Count": 24276,
                  "__time": {
                    "end": new Date('2015-09-12T08:00:00.000Z'),
                    "start": new Date('2015-09-12T06:00:00.000Z'),
                    "type": "TIME_RANGE"
                  },
                  "channel": "vi"
                },
                {
                  "Count": 11223,
                  "__time": {
                    "end": new Date('2015-09-12T18:00:00.000Z'),
                    "start": new Date('2015-09-12T16:00:00.000Z'),
                    "type": "TIME_RANGE"
                  },
                  "channel": "vi"
                },
                {
                  "Count": 9258,
                  "__time": {
                    "end": new Date('2015-09-12T16:00:00.000Z'),
                    "start": new Date('2015-09-12T14:00:00.000Z'),
                    "type": "TIME_RANGE"
                  },
                  "channel": "vi"
                },
                {
                  "Count": 8928,
                  "__time": {
                    "end": new Date('2015-09-12T10:00:00.000Z'),
                    "start": new Date('2015-09-12T08:00:00.000Z'),
                    "type": "TIME_RANGE"
                  },
                  "channel": "vi"
                }
              ]
            }
          ]);
        });
    });

    it("works nested GROUP BYs", () => {
      let ex = $('wiki')
        .split({ 'isNew': '$isNew', 'isRobot': '$isRobot' })
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .split('$isNew', 'isNew', 'data')
        .apply('SumTotalEdits', '$data.sum($TotalEdits)');

      return basicExecutor(ex)
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
        });
    });

    it("works string range", () => {
      let ex = $('wiki').filter('$cityName > "nice"')
        .filter('$comment < "zebra"')
        .filter('$page >= "car"')
        .filter('$countryName <= "mauritius"')
        .split({ 'cityName': '$cityName', 'page': '$page', 'comment': '$comment', 'country': '$countryName' });

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "cityName": "Ōita",
              "comment": "/* 1982年（昭和57年） */",
              "country": "Japan",
              "page": "日本のテレビアニメ作品一覧 (1980年代)"
            },
            {
              "cityName": "Ōita",
              "comment": "/* 劇場版 */",
              "country": "Japan",
              "page": "ドクタースランプ"
            }
          ]);
        });
    });

    it("works with raw (SELECT) + filter", () => {
      let ex = $('wiki').filter('$cityName == "El Paso"');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "added": 0,
              "channel": "en",
              "cityName": "El Paso",
              "comment": "/* Clubs and organizations */",
              "commentLength": 29,
              "commentLengthStr": "29",
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
              "page_unique": "AQAAAQAAAADYAQ==",
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
              "user_theta": "AgMDAAAazJMBAAAAAACAP4LYKgb0JYUx",
              "user_unique": "AQAAAQAAAAFzBQ=="
            },
            {
              "added": 0,
              "channel": "en",
              "cityName": "El Paso",
              "comment": "/* Early life */ spelling",
              "commentLength": 25,
              "commentLengthStr": "25",
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
              "page_unique": "AQAAAQAAAAOhEA==",
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
              "user_theta": "AgMDAAAazJMBAAAAAACAPykGTa6KslE/",
              "user_unique": "AQAAAQAAAAOIQA=="
            }
          ]);
        });
    });

    it("works with raw (SELECT) + limit", () => {
      let ex = $('wiki').limit(1);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "added": 0,
              "channel": "ca",
              "cityName": null,
              "comment": "/* Enllaços externs */",
              "commentLength": 22,
              "commentLengthStr": "22",
              "count": 1,
              "countryIsoCode": null,
              "countryName": null,
              "deleted": 1,
              "delta": -1,
              "deltaBucket100": -100,
              "deltaByTen": -0.10000000149011612,
              "delta_hist": null,
              "isAnonymous": false,
              "isMinor": false,
              "isNew": false,
              "isRobot": false,
              "isUnpatrolled": true,
              "max_delta": -1,
              "metroCode": null,
              "min_delta": -1,
              "namespace": "Main",
              "page": "Israel Ballet",
              "page_unique": "AQAAAQAAAAHHIA==",
              "regionIsoCode": null,
              "regionName": null,
              "sometimeLater": {
                "type": "TIME",
                "value": new Date('2016-09-12T00:46:00.000Z')
              },
              "time": {
                "type": "TIME",
                "value": new Date('2015-09-12T00:46:00.000Z')
              },
              "user": "ChandraHelsinky",
              "userChars": {
                "elements": ["A", "C", "D", "E", "H", "I", "K", "L", "N", "R", "S", "Y"],
                "setType": "STRING",
                "type": "SET"
              },
              "user_theta": "AgMDAAAazJMBAAAAAACAP5xSyQDWkQwY",
              "user_unique": "AQAAAQAAAALGBA=="
            }
          ]);
        });
    });

    it("gets the right number of results in a big raw (SELECT ascending)", () => {
      let limit = 15001;
      let ex = $('wiki')
        .filter('$cityName == null')
        .select('time', 'cityName')
        .sort('$time', 'ascending')
        .limit(limit);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().length).to.equal(limit);
        });
    });

    it("gets the right number of results in a big raw (SELECT descending)", () => {
      let limit = 15001;
      let ex = $('wiki')
        .filter('$cityName == null')
        .select('time', 'cityName')
        .sort('$time', 'descending')
        .limit(limit);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().length).to.equal(limit);
        });
    });

    it("works with raw (SELECT) inside a split", () => {
      let ex = $('wiki')
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

      return basicExecutor(ex)
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
        });
    });

    it("works with multi-value dimension regexp having filter", () => {
      let ex = $("wiki")
        .filter('$userChars.match("[ABN]")')
        .split("$userChars", 'userChar')
        .filter('$userChar.match("B|N")')
        .limit(5);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            { "userChar": "B" },
            { "userChar": "N" }
          ]);
        });
    });

    it("works with basic collect", () => {
      let ex = $('wiki').split('$channel', 'channel').collect('$channel');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal({
            "elements": [
              "ar", "be", "bg", "ca", "ce", "ceb", "cs", "da", "de", "el", "en", "eo", "es", "et", "eu", "fa", "fi",
              "fr", "gl", "he", "hi", "hr", "hu", "hy", "id", "it", "ja", "kk", "ko", "la", "lt", "min", "ms", "nl",
              "nn", "no", "pl", "pt", "ro", "ru", "sh", "simple", "sk", "sl", "sr", "sv", "tr", "uk", "uz", "vi",
              "war", "zh"
            ],
            "setType": "STRING"
          });
        });
    });

    it("works with advanced collect", () => {
      let ex = $('wiki').split('$channel', 'channel')
        .apply('Edits', '$wiki.sum($count)')
        .sort('$Edits', 'descending')
        .limit(5)
        .collect('$channel');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal({
            "elements": [
              "en", "vi", "de", "fr", "ru"
            ],
            "setType": "STRING"
          });
        });
    });

    it("works with collect as a sub-filter", () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter(
          $('channel').in(
            $('wiki').split('$channel', 'channel')
              .apply('Edits', '$wiki.sum($count)')
              .sort('$Edits', 'descending')
              .limit(5)
              .collect('$channel')
          )
        ))
        .apply('Count', '$wiki.sum($count)')
        .apply('Added', '$wiki.sum($added)');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Added": 54157728,
              "Count": 274140
            }
          ]);
        });
    });

    describe("plyql end to end", () => {
      it("should work with <= <", () => {
        let ex = Expression.parseSQL(sane`
          SELECT
          SUM(added) AS 'TotalAdded'
          FROM \`wiki\`
          WHERE \`channel\`="en" AND '2015-09-12T10:00:00' <= \`time\` AND \`time\` < '2015-09-12T12:00:00'
        `);

        return basicExecutor(ex.expression)
          .then((result) => {
            expect(result.toJS()).to.deep.equal([
              {
                "TotalAdded": 2274537
              }
            ]);
          });
      });

      it("should work with between and without top level GROUP BY", () => {
        let ex = Expression.parseSQL(sane`
          SELECT
          \`page\` AS 'Page',
          SUM(added) AS 'TotalAdded'
          FROM \`wiki\`
          WHERE \`channel\`="en" AND \`time\` BETWEEN '2015-09-12T10:00:00' AND '2015-09-12T12:00:00'
          GROUP BY 1
          ORDER BY \`TotalAdded\` DESC
          LIMIT 5
        `);

        return basicExecutor(ex.expression)
          .then((result) => {
            expect(result.data).to.deep.equal([
              {
                "Page": "Wikipedia:Administrators' noticeboard/Archive274",
                "TotalAdded": 96997
              },
              {
                "Page": "User:Afernand74/sandbox 2",
                "TotalAdded": 37103
              },
              {
                "Page": "Draft:Nha San Collective",
                "TotalAdded": 29978
              },
              {
                "Page": "Crazy Frog",
                "TotalAdded": 28685
              },
              {
                "Page": "Template:Singapore MRT stations",
                "TotalAdded": 27126
              }
            ]);
          });
      });

    })

  });


  describe("incorrect user chars", () => {
    let wikiUserCharAsNumber = External.fromJS({
      engine: 'druid',
      source: 'wikipedia',
      timeAttribute: 'time',
      allowEternity: true,
      attributes: [
        { name: 'time', type: 'TIME' },
        { name: "channel", type: "STRING" },
        { name: 'userChars', type: 'NUMBER' }, // This is incorrect
        { name: 'count', type: 'NUMBER', unsplitable: true }
      ]
    }, druidRequester);

    it("works with number addition", () => {
      let ex = $('wiki').split("$userChars + 10", 'U')
        .apply('Count', '$wiki.sum($count)')
        .sort('$Count', 'descending')
        .limit(3);

      return ex.compute({ wiki: wikiUserCharAsNumber })
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Count": 2658542,
              "U": null
            },
            {
              "Count": 68663,
              "U": 11
            },
            {
              "Count": 45822,
              "U": 12
            }
          ]);
        });
    });

    it("works with number bucketing", () => {
      let ex = $('wiki').split("$userChars.numberBucket(5, 2.5)", 'U')
        .apply('Count', '$wiki.sum($count)')
        .sort('$Count', 'descending')
        .limit(3);

      return ex.compute({ wiki: wikiUserCharAsNumber })
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Count": 2658542,
              "U": null
            },
            {
              "Count": 151159,
              "U": {
                "end": 7.5,
                "start": 2.5,
                "type": "NUMBER_RANGE"
              }
            },
            {
              "Count": 150305,
              "U": {
                "end": 2.5,
                "start": -2.5,
                "type": "NUMBER_RANGE"
              }
            }
          ]);
        });
    });

    it("works with power", () => {
      let ex = $('wiki').split("$userChars.power(2)", 'U')
        .apply('Count', '$wiki.sum($count)')
        .sort('$Count', 'descending')
        .limit(3);

      return ex.compute({ wiki: wikiUserCharAsNumber })
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Count": 2658542,
              "U": null
            },
            {
              "Count": 68663,
              "U": 1
            },
            {
              "Count": 45822,
              "U": 4
            }
          ]);
        });
    });

    it("works with bad casts", () => {
      let ex = $('wiki').split({ 'numberCast': '$channel.cast("NUMBER")', 'dateCast': '$userChars.cast("TIME")' })
        .apply('Count', '$wiki.sum($count)')
        .sort('$Count', 'descending')
        .limit(3);

      return ex.compute({ wiki: wikiUserCharAsNumber })
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Count": 2658542,
              "dateCast": null,
              "numberCast": null
            },
            {
              "Count": 379865,
              "dateCast": {
                "type": "TIME",
                "value": new Date('1970-01-01T00:00:00.000Z')
              },
              "numberCast": null
            }
          ]);
        });
    });

  });


  describe("introspection", () => {
    let wikiExternal = External.fromJS({
      engine: 'druid',
      source: 'wikipedia',
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

    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: wikiExternal
      }
    });

    it("introspects version and attributes", () => {
      return wikiExternal.introspect()
        .then((introspectedExternal) => {
          expect(introspectedExternal.version).to.equal(info.druidVersion);
          expect(introspectedExternal.toJS().attributes).to.deep.equal(wikiAttributes);
        });
    });

    it("works with introspection", () => {
      let ex = ply()
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

      return basicExecutor(ex)
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
        });
    });

  });


  describe("introspection (union dataSource)", () => {
    let doubleWikiExternal = External.fromJS({
      engine: 'druid',
      source: ['wikipedia', 'wikipedia-compact'],
      timeAttribute: 'time',
      filter: $('time').in(TimeRange.fromJS({
        start: new Date("2015-09-12T00:00:00Z"),
        end: new Date("2015-09-13T00:00:00Z")
      }))
    }, druidRequester);

    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: doubleWikiExternal
      }
    });

    it("works with introspection", () => {
      let ex = ply()
        .apply("wiki", $('wiki').filter($("channel").is('en')))
        .apply('Count', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply(
          'Time',
          $("wiki").split($("time").timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$Timestamp', 'ascending')
            .limit(3)
        );

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Count": 229422,
              "Time": [
                {
                  "Timestamp": {
                    "end": new Date('2015-09-12T01:00:00.000Z'),
                    "start": new Date('2015-09-12T00:00:00.000Z'),
                    "type": "TIME_RANGE"
                  },
                  "TotalAdded": 663850
                },
                {
                  "Timestamp": {
                    "end": new Date('2015-09-12T02:00:00.000Z'),
                    "start": new Date('2015-09-12T01:00:00.000Z'),
                    "type": "TIME_RANGE"
                  },
                  "TotalAdded": 2836144
                },
                {
                  "Timestamp": {
                    "end": new Date('2015-09-12T03:00:00.000Z'),
                    "start": new Date('2015-09-12T02:00:00.000Z'),
                    "type": "TIME_RANGE"
                  },
                  "TotalAdded": 6091932
                }
              ],
              "TotalAdded": 65106214
            }
          ]);
        });
    });

  });

});
