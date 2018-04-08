/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2018 Imply Data, Inc.
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
let { External, DruidExternal, TimeRange, $, i$, ply, r, basicExecutorFactory, verboseRequesterFactory, Expression } = plywood;

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
    {
      "name": "time",
      "nativeType": "__time",
      "range": {
        "bounds": "[]",
        "end": new Date('2015-09-12T23:59:00.000Z'),
        "start": new Date('2015-09-12T00:46:00.000Z')
      },
      "type": "TIME"
    },
    {
      "maker": {
        "expression": {
          "name": "added",
          "op": "ref"
        },
        "op": "sum"
      },
      "name": "added",
      "nativeType": "LONG",
      "type": "NUMBER",
      "unsplitable": true
    },
    {
      "cardinality": 52,
      "name": "channel",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "zh",
        "start": "ar"
      },
      "type": "STRING"
    },
    {
      "cardinality": 3719,
      "name": "cityName",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "Ōita",
        "start": ""
      },
      "type": "STRING"
    },
    {
      "cardinality": 138678,
      "name": "comment",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "ｒｖ",
        "start": "!"
      },
      "type": "STRING"
    },
    {
      "name": "commentLength",
      "nativeType": "LONG",
      "type": "NUMBER"
    },
    {
      "cardinality": 255,
      "name": "commentLengthStr",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "99",
        "start": "1"
      },
      "type": "STRING"
    },
    {
      "maker": {
        "op": "count"
      },
      "name": "count",
      "nativeType": "LONG",
      "type": "NUMBER",
      "unsplitable": true
    },
    {
      "cardinality": 157,
      "name": "countryIsoCode",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "ZW",
        "start": ""
      },
      "type": "STRING"
    },
    {
      "cardinality": 157,
      "name": "countryName",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "Zimbabwe",
        "start": ""
      },
      "type": "STRING"
    },
    {
      "maker": {
        "expression": {
          "name": "deleted",
          "op": "ref"
        },
        "op": "sum"
      },
      "name": "deleted",
      "nativeType": "LONG",
      "type": "NUMBER",
      "unsplitable": true
    },
    {
      "maker": {
        "expression": {
          "name": "delta",
          "op": "ref"
        },
        "op": "sum"
      },
      "name": "delta",
      "nativeType": "LONG",
      "type": "NUMBER",
      "unsplitable": true
    },
    {
      "name": "deltaBucket100",
      "nativeType": "FLOAT",
      "type": "NUMBER"
    },
    {
      "maker": {
        "expression": {
          "name": "deltaByTen",
          "op": "ref"
        },
        "op": "sum"
      },
      "name": "deltaByTen",
      "nativeType": "DOUBLE",
      "type": "NUMBER",
      "unsplitable": true
    },
    {
      "name": "delta_hist",
      "nativeType": "approximateHistogram",
      "type": "NULL",
      "unsplitable": true
    },
    {
      "name": "isAnonymous",
      "type": "BOOLEAN"
    },
    {
      "name": "isMinor",
      "type": "BOOLEAN"
    },
    {
      "name": "isNew",
      "type": "BOOLEAN"
    },
    {
      "name": "isRobot",
      "type": "BOOLEAN"
    },
    {
      "name": "isUnpatrolled",
      "type": "BOOLEAN"
    },
    {
      "maker": {
        "expression": {
          "name": "max_delta",
          "op": "ref"
        },
        "op": "max"
      },
      "name": "max_delta",
      "nativeType": "LONG",
      "type": "NUMBER",
      "unsplitable": true
    },
    {
      "cardinality": 167,
      "name": "metroCode",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "881",
        "start": ""
      },
      "type": "STRING"
    },
    {
      "maker": {
        "expression": {
          "name": "min_delta",
          "op": "ref"
        },
        "op": "min"
      },
      "name": "min_delta",
      "nativeType": "LONG",
      "type": "NUMBER",
      "unsplitable": true
    },
    {
      "cardinality": 416,
      "name": "namespace",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "파일",
        "start": "2"
      },
      "type": "STRING"
    },
    {
      "cardinality": 279893,
      "name": "page",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "［Alexandros］",
        "start": "!T.O.O.H.!"
      },
      "type": "STRING"
    },
    {
      "name": "page_unique",
      "nativeType": "hyperUnique",
      "type": "NULL",
      "unsplitable": true
    },
    {
      "cardinality": 671,
      "name": "regionIsoCode",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "null",
        "start": ""
      },
      "type": "STRING"
    },
    {
      "cardinality": 1068,
      "name": "regionName",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "Świętokrzyskie",
        "start": ""
      },
      "type": "STRING"
    },
    {
      "name": "sometimeLater",
      "type": "TIME"
    },
    {
      "name": "sometimeLaterMs",
      "nativeType": "LONG",
      "type": "NUMBER"
    },
    {
      "cardinality": 38234,
      "name": "user",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "ＫＡＺＵ",
        "start": "! Bikkit !"
      },
      "type": "STRING"
    },
    {
      "cardinality": 1401,
      "name": "userChars",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "～",
        "start": " "
      },
      "type": "SET/STRING"
    },
    {
      "name": "user_theta",
      "nativeType": "thetaSketch",
      "type": "NULL",
      "unsplitable": true
    },
    {
      "name": "user_unique",
      "nativeType": "hyperUnique",
      "type": "NULL",
      "unsplitable": true
    }
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
          "function" : "function(x) { return String(x).concat('concat') }"
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
      filter: $('time').overlap(new Date("2015-09-12T00:00:00Z"), new Date("2015-09-13T00:00:00Z")),
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
        { name: 'page_unique', type: 'NULL', nativeType: 'hyperUnique', unsplitable: true }
      ],
      filter: $('time').overlap(TimeRange.fromJS({
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
            null,31529720
            Mineola,50836
          `);
        });
    });

    it("works with basic error (non-existent lookup)", () => {
      let ex = $('wiki').split("$cityName.lookup(blah)", 'B');

      return basicExecutor(ex)
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((e) => {
          expect(e.message).to.contain('Lookup [blah] not found');
        });
    });

    it("works basic total", () => {
      let ex = ply()
        .apply("wiki", $('wiki').filter($("channel").is('en')))
        .apply("Count", '$wiki.count()');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal({
            "attributes": [
              {
                "name": "wiki",
                "type": "DATASET"
              },
              {
                "name": "Count",
                "type": "NUMBER"
              }
            ],
            "data": [
              {
                "Count": 35485
              }
            ]
          });

          expect(result.flatten().toJS()).to.deep.equal({
            "attributes": [
              {
                "name": "Count",
                "type": "NUMBER"
              }
            ],
            "data": [
              {
                "Count": 35485
              }
            ]
          });
        });
    });

    it("works with max time 1", () => {
      let ex = ply()
        .apply("max(time)", '$wiki.max($time)');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal({
            "attributes": [
              {
                "name": "max(time)",
                "type": "TIME"
              }
            ],
            "data": [
              {
                "max(time)": new Date('2015-09-12T23:00:00.000Z')
              }
            ]
          });
        });
    });

    it("works with max time 2", () => {
      let ex = $('wiki').max('$time');

      return basicExecutor(ex)
        .then((result) => {
          expect(result).to.deep.equal(new Date('2015-09-12T23:00:00.000Z'));
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
          expect(result.attributes.map((c => c.name))).to.deep.equal(['Page', 'Count', 'isRobot', 'isNew']);
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
          expect(result.attributes.map((c => c.name))).to.deep.equal(['isRobot', 'Page', 'isNew', 'Count']);
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
          expect(result.attributes.map((c => c.name))).to.deep.equal(['Count', 'isRobot', 'Page', 'isNew']);
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
          expect(result.toJS().data).to.deep.equal([
            {
              "HoursOfDay": {
                "attributes": [
                  {
                    "name": "HourOfDay",
                    "type": "NUMBER"
                  },
                  {
                    "name": "TotalAdded",
                    "type": "NUMBER"
                  }
                ],
                "data": [
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
                ],
                "keys": [
                  "HourOfDay"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Quarter": 3
            }
          ]);
        });
    });

    it("works with time floor + timezone", () => {
      let ex = $('wiki')
        .split({ t: $('time').timeFloor('P1D', 'Europe/Paris'), robot: '$isRobot'})
        .apply('cnt', '$wiki.count()')
        .sort('$cnt', 'descending')
        .limit(10);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "cnt": 216909,
              "robot": false,
              "t": new Date('2015-09-11T22:00:00.000Z')
            },
            {
              "cnt": 143468,
              "robot": true,
              "t": new Date('2015-09-11T22:00:00.000Z')
            },
            {
              "cnt": 19213,
              "robot": false,
              "t": new Date('2015-09-12T22:00:00.000Z')
            },
            {
              "cnt": 11392,
              "robot": true,
              "t": new Date('2015-09-12T22:00:00.000Z')
            }
          ]);
        });
    });

    it("works with yearly call case long", () => {
      let ex = $('wiki')
        .split(i$('time').timeFloor('P3M'), 'tqr___time_ok');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "tqr___time_ok": new Date('2015-07-01T00:00:00.000Z'),
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Count": 114711,
              "Pages": {
                "attributes": [
                  {
                    "name": "Page",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  },
                  {
                    "name": "Time",
                    "type": "DATASET"
                  }
                ],
                "data": [
                  {
                    "Count": 255,
                    "Page": "User:Cyde/List of candidates for speedy deletion/Subpage",
                    "Time": {
                      "attributes": [
                        {
                          "name": "Timestamp",
                          "type": "TIME_RANGE"
                        },
                        {
                          "name": "TotalAdded",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Timestamp": {
                            "end": new Date('2015-09-12T13:00:00.000Z'),
                            "start": new Date('2015-09-12T12:00:00.000Z')
                          },
                          "TotalAdded": 9231
                        },
                        {
                          "Timestamp": {
                            "end": new Date('2015-09-13T00:00:00.000Z'),
                            "start": new Date('2015-09-12T23:00:00.000Z')
                          },
                          "TotalAdded": 3956
                        },
                        {
                          "Timestamp": {
                            "end": new Date('2015-09-12T02:00:00.000Z'),
                            "start": new Date('2015-09-12T01:00:00.000Z')
                          },
                          "TotalAdded": 3363
                        }
                      ],
                      "keys": [
                        "Timestamp"
                      ]
                    }
                  },
                  {
                    "Count": 241,
                    "Page": "Jeremy Corbyn",
                    "Time": {
                      "attributes": [
                        {
                          "name": "Timestamp",
                          "type": "TIME_RANGE"
                        },
                        {
                          "name": "TotalAdded",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Timestamp": {
                            "end": new Date('2015-09-12T16:00:00.000Z'),
                            "start": new Date('2015-09-12T15:00:00.000Z')
                          },
                          "TotalAdded": 28193
                        },
                        {
                          "Timestamp": {
                            "end": new Date('2015-09-12T19:00:00.000Z'),
                            "start": new Date('2015-09-12T18:00:00.000Z')
                          },
                          "TotalAdded": 2419
                        },
                        {
                          "Timestamp": {
                            "end": new Date('2015-09-12T11:00:00.000Z'),
                            "start": new Date('2015-09-12T10:00:00.000Z')
                          },
                          "TotalAdded": 2041
                        }
                      ],
                      "keys": [
                        "Timestamp"
                      ]
                    }
                  }
                ],
                "keys": [
                  "Page"
                ]
              },
              "PagesHaving": {
                "attributes": [
                  {
                    "name": "Page",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
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
                "keys": [
                  "Page"
                ]
              },
              "TotalAdded": 32553107
            }
          ]);
        });
    });

    it("works in advanced case (with trim)", () => {
      let ex = ply()
        .apply("wiki", $('wiki').filter($("channel").is('en')))
        .apply('Count', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply(
          'Pages',
          $("wiki").split("$page", 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(100)
            .apply(
              'Time',
              $("wiki").split("$user", 'User')
                .apply('TotalAdded', '$wiki.sum($added)')
                .sort('$TotalAdded', 'descending')
                .limit(24)
            )
        );

      return basicExecutor(ex, { maxRows: 5 })
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "Count": 114711,
              "Pages": {
                "attributes": [
                  {
                    "name": "Page",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  },
                  {
                    "name": "Time",
                    "type": "DATASET"
                  }
                ],
                "data": [
                  {
                    "Count": 255,
                    "Page": "User:Cyde/List of candidates for speedy deletion/Subpage",
                    "Time": {
                      "attributes": [
                        {
                          "name": "User",
                          "type": "STRING"
                        },
                        {
                          "name": "TotalAdded",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "TotalAdded": 35445,
                          "User": "Cydebot"
                        }
                      ],
                      "keys": [
                        "User"
                      ]
                    }
                  },
                  {
                    "Count": 241,
                    "Page": "Jeremy Corbyn",
                    "Time": {
                      "attributes": [
                        {
                          "name": "User",
                          "type": "STRING"
                        },
                        {
                          "name": "TotalAdded",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "TotalAdded": 30035,
                          "User": "Hazhk"
                        }
                      ],
                      "keys": [
                        "User"
                      ]
                    }
                  }
                ],
                "keys": [
                  "Page"
                ]
              },
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
          expect(result.toJS().data).to.deep.equal([
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
        .apply('Temp', '$wiki.count()') // ToDo: Temp fix
        .limit(8);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              Temp: 1984,
              "lastChar": "z"
            }
          ]);
        });
    });

    it("works with custom transform in filter and split for numeric dimension", () => {
      let ex = $('wiki')
        .filter($("commentLength").customTransform('concatWithConcat', 'STRING').is("'100concat'"))
        .split($("commentLength").customTransform('timesTwo', 'STRING'), "Times Two")
        .apply('Temp', '$wiki.count()') // ToDo: Temp fix
        .limit(8);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              Temp: 275,
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Diff_Users_1_2": 1507,
              "Diff_Users_1_3": 1055.505956137029,
              "Diff_Users_2_3": -451.494043862971,
              "UniqueIsRobot": 2,
              "UniquePages1": 279107,
              "UniquePages2": 281588,
              "UniqueUserChars": 1376,
              "UniqueUsers1": 39220,
              "UniqueUsers2": 37713,
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
          expect(result.toJS().data).to.deep.equal([
            {
              "UniquePagesEn": 63850,
              "UniquePagesEnOver2": 31925
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
          expect(result.toJS().data).to.deep.equal([
            {
              "UniquePagesEn": 63850,
              "UniquePagesEs": 6870,
              "UniquePagesChannelDiff": 56980
            }
          ]);
        });
    });

    it("works with multiple columns", () => {
      let ex = $('wiki').countDistinct("$channel ++ 'lol' ++ $user");

      return basicExecutor(ex)
        .then((result) => {
          expect(result).to.deep.equal(40082);
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Channels": {
                "attributes": [
                  {
                    "name": "Channel",
                    "type": "STRING"
                  },
                  {
                    "name": "Users",
                    "type": "DATASET"
                  }
                ],
                "data": [
                  {
                    "Channel": "zh",
                    "Users": {
                      "attributes": [
                        {
                          "name": "User",
                          "type": "STRING"
                        },
                        {
                          "name": "Count",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Count": 3698,
                          "User": "Antigng-bot"
                        },
                        {
                          "Count": 503,
                          "User": "和平-bot"
                        }
                      ],
                      "keys": [
                        "User"
                      ]
                    }
                  },
                  {
                    "Channel": "war",
                    "Users": {
                      "attributes": [
                        {
                          "name": "User",
                          "type": "STRING"
                        },
                        {
                          "name": "Count",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Count": 4,
                          "User": "JinJian"
                        },
                        {
                          "Count": 3,
                          "User": "Xqbot"
                        }
                      ],
                      "keys": [
                        "User"
                      ]
                    }
                  }
                ],
                "keys": [
                  "Channel"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
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
          expect(result.toJS().data).to.deep.equal([
            {
              "UserChars": {
                "attributes": [
                  {
                    "name": "UserChar",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
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
                ],
                "keys": [
                  "UserChar"
                ]
              }
            }
          ]);
        });
    });

    it("works with split on a SET/STRING dimension + time + filter", () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter('$userChars.is("O")'))
        .apply(
          'UserChars',
          $('wiki').split("$userChars", 'UserChar')
            .apply("Count", $('wiki').sum('$count'))
            .sort('$Count', 'descending')
            .limit(2)
            .apply(
              "Split",
              $('wiki').split('$time.timeBucket(PT12H)', 'T')
                .apply("Count", $('wiki').sum('$count'))
                .sort("$T", 'ascending')
            )
        );

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "UserChars": {
                "attributes": [
                  {
                    "name": "UserChar",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  },
                  {
                    "name": "Split",
                    "type": "DATASET"
                  }
                ],
                "data": [
                  {
                    "Count": 223134,
                    "Split": {
                      "attributes": [
                        {
                          "name": "T",
                          "type": "TIME_RANGE"
                        },
                        {
                          "name": "Count",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Count": 93346,
                          "T": {
                            "end": new Date('2015-09-12T12:00:00.000Z'),
                            "start": new Date('2015-09-12T00:00:00.000Z')
                          }
                        },
                        {
                          "Count": 129788,
                          "T": {
                            "end": new Date('2015-09-13T00:00:00.000Z'),
                            "start": new Date('2015-09-12T12:00:00.000Z')
                          }
                        }
                      ],
                      "keys": [
                        "T"
                      ]
                    },
                    "UserChar": "O"
                  },
                  {
                    "Count": 173377,
                    "Split": {
                      "attributes": [
                        {
                          "name": "T",
                          "type": "TIME_RANGE"
                        },
                        {
                          "name": "Count",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Count": 74042,
                          "T": {
                            "end": new Date('2015-09-12T12:00:00.000Z'),
                            "start": new Date('2015-09-12T00:00:00.000Z')
                          }
                        },
                        {
                          "Count": 99335,
                          "T": {
                            "end": new Date('2015-09-13T00:00:00.000Z'),
                            "start": new Date('2015-09-12T12:00:00.000Z')
                          }
                        }
                      ],
                      "keys": [
                        "T"
                      ]
                    },
                    "UserChar": "T"
                  }
                ],
                "keys": [
                  "UserChar"
                ]
              }
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
        .apply('Delta99thX2', $('wiki').quantile('$delta_hist', 0.99).multiply(2))
        .apply('Delta98thEn', $('wiki').filter($("channel").is('en')).quantile('$delta_hist', 0.98))
        .apply('Delta98thDe', $('wiki').filter($("channel").is('de')).quantile('$delta_hist', 0.98));

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "AddedByDeleted": 24.909643797343193,
              "ChannelAdded": 97393743,
              "ChannelENAdded": 32553107,
              "ChannelENishAdded": 32553107,
              "Count": 392443,
              "CountSquareRoot": 626.4527117029664,
              "CountSquared": 154011508249,
              "NumEnPages": 63850,
              "NumPages": 279107,
              "One": 1,
              "Delta95th": 161.95517,
              "Delta99thX2": 328.9096984863281,
              "Delta98thEn": 176.93568,
              "Delta98thDe": 112.789635
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
          expect(result.toJS().data).to.deep.equal([
            {
              "ChannelAdded": 53750772,
              "ChannelENAdded": 23136956,
              "ChannelENishAdded": 23136956,
              "Count": 368841,
              "CountSquareRoot": 607.3228136666694,
              "CountSquared": 136043683281,
              "NumEnPages": 57395,
              "NumPages": 262440,
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
              "NumEnPages": 8166,
              "NumPages": 22270,
              "One": 1,
              "isNew": true
            }
          ]);
        });
    });

    it("works with no applies in time split dataset (+rawQueries monitoring)", () => {
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

      let rawQueries = [];
      return basicExecutor(ex, { rawQueries })
        .then((result) => {
          expect(rawQueries).to.deep.equal([
            {
              "engine": "druid",
              "query": {
                "context": {
                  "populateCache": false,
                  "skipEmptyBuckets": "true",
                  "timeout": 10000,
                  "useCache": false
                },
                "dataSource": "wikipedia-compact",
                "granularity": {
                  "period": "PT1H",
                  "timeZone": "Etc/UTC",
                  "type": "period"
                },
                "intervals": "2015-09-12T00Z/2015-09-13T00Z",
                "queryType": "timeseries"
              }
            },
            {
              "engine": "druid",
              "query": {
                "aggregations": [
                  {
                    "fieldName": "count",
                    "name": "Count",
                    "type": "longSum"
                  }
                ],
                "context": {
                  "populateCache": false,
                  "timeout": 10000,
                  "useCache": false
                },
                "dataSource": "wikipedia",
                "dimension": {
                  "dimension": "page",
                  "outputName": "Page",
                  "type": "default"
                },
                "granularity": "all",
                "intervals": "2015-09-12T00Z/2015-09-12T01Z",
                "metric": "Count",
                "queryType": "topN",
                "threshold": 2
              }
            },
            {
              "engine": "druid",
              "query": {
                "aggregations": [
                  {
                    "fieldName": "count",
                    "name": "Count",
                    "type": "longSum"
                  }
                ],
                "context": {
                  "populateCache": false,
                  "timeout": 10000,
                  "useCache": false
                },
                "dataSource": "wikipedia",
                "dimension": {
                  "dimension": "page",
                  "outputName": "Page",
                  "type": "default"
                },
                "granularity": "all",
                "intervals": "2015-09-12T01Z/2015-09-12T02Z",
                "metric": "Count",
                "queryType": "topN",
                "threshold": 2
              }
            },
            {
              "engine": "druid",
              "query": {
                "aggregations": [
                  {
                    "fieldName": "count",
                    "name": "Count",
                    "type": "longSum"
                  }
                ],
                "context": {
                  "populateCache": false,
                  "timeout": 10000,
                  "useCache": false
                },
                "dataSource": "wikipedia",
                "dimension": {
                  "dimension": "page",
                  "outputName": "Page",
                  "type": "default"
                },
                "granularity": "all",
                "intervals": "2015-09-12T02Z/2015-09-12T03Z",
                "metric": "Count",
                "queryType": "topN",
                "threshold": 2
              }
            }
          ]);

          expect(result.toJS().data).to.deep.equal([
            {
              "ByHour": {
                "attributes": [
                  {
                    "name": "TimeByHour",
                    "type": "TIME_RANGE"
                  },
                  {
                    "name": "Users",
                    "type": "DATASET"
                  }
                ],
                "data": [
                  {
                    "TimeByHour": {
                      "end": new Date('2015-09-12T01:00:00.000Z'),
                      "start": new Date('2015-09-12T00:00:00.000Z')
                    },
                    "Users": {
                      "attributes": [
                        {
                          "name": "Page",
                          "type": "STRING"
                        },
                        {
                          "name": "Count",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Count": 12,
                          "Page": "User talk:Dudeperson176123"
                        },
                        {
                          "Count": 11,
                          "Page": "Israel Ballet"
                        }
                      ],
                      "keys": [
                        "Page"
                      ]
                    }
                  },
                  {
                    "TimeByHour": {
                      "end": new Date('2015-09-12T02:00:00.000Z'),
                      "start": new Date('2015-09-12T01:00:00.000Z')
                    },
                    "Users": {
                      "attributes": [
                        {
                          "name": "Page",
                          "type": "STRING"
                        },
                        {
                          "name": "Count",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Count": 26,
                          "Page": "Campeonato Mundial de Voleibol Femenino Sub-20 de 2015"
                        },
                        {
                          "Count": 22,
                          "Page": "Flüchtlingskrise in Europa 2015"
                        }
                      ],
                      "keys": [
                        "Page"
                      ]
                    }
                  },
                  {
                    "TimeByHour": {
                      "end": new Date('2015-09-12T03:00:00.000Z'),
                      "start": new Date('2015-09-12T02:00:00.000Z')
                    },
                    "Users": {
                      "attributes": [
                        {
                          "name": "Page",
                          "type": "STRING"
                        },
                        {
                          "name": "Count",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Count": 28,
                          "Page": "Wikipedia:Administrators' noticeboard/Incidents"
                        },
                        {
                          "Count": 18,
                          "Page": "2015 World Wrestling Championships"
                        }
                      ],
                      "keys": [
                        "Page"
                      ]
                    }
                  }
                ],
                "keys": [
                  "TimeByHour"
                ]
              }
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
          expect(result.toJS().data).to.have.length(2);
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Delta95th": -39,
              "TimeByHour": {
                "end": new Date('2015-09-12T07:00:00.000Z'),
                "start": new Date('2015-09-12T06:00:00.000Z')
              },
              "count": 1
            },
            {
              "Delta95th": 0,
              "TimeByHour": {
                "end": new Date('2015-09-12T17:00:00.000Z'),
                "start": new Date('2015-09-12T16:00:00.000Z')
              },
              "count": 1
            }
          ]);
        });
    });

    it.skip("works with single apply on string column (total)", () => {
      let ex = $('wiki')
        .apply('count', '$wiki.sum($commentLengthStr.cast("NUMBER"))');

      return basicExecutor(ex)
        .then((result) => {
          console.log('result', result);
          expect(result.toJS().data).to.deep.equal([

          ]);
        });
    });

    it("works with applies on string columns", () => {
      let ex = $('wiki')
        .split($("channel"), 'Channel')
        .apply('sum_cl', '$wiki.sum($commentLength)')
        .apply('sum_cls', '$wiki.sum($commentLengthStr.cast("NUMBER"))')
        .apply('min_cls', '$wiki.min($commentLengthStr.cast("NUMBER"))')
        .apply('max_cls', '$wiki.max($commentLengthStr.cast("NUMBER"))')
        .limit(3);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "max_cls": 253,
              "min_cls": 1,
              "sum_cl": 267579,
              "sum_cls": 267579,
              "Channel": "ar"
            },
            {
              "max_cls": 253,
              "min_cls": 4,
              "sum_cl": 12192,
              "sum_cls": 12192,
              "Channel": "be"
            },
            {
              "max_cls": 253,
              "min_cls": 1,
              "sum_cl": 46398,
              "sum_cls": 46398,
              "Channel": "bg"
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Pages": {
                "attributes": [
                  {
                    "name": "Page",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
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
                ],
                "keys": [
                  "Page"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Pages": {
                "attributes": [
                  {
                    "name": "Page",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
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
                ],
                "keys": [
                  "Page"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Pages": {
                "attributes": [
                  {
                    "name": "Page",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
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
                ],
                "keys": [
                  "Page"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Channels": {
                "attributes": [
                  {
                    "name": "Channel",
                    "type": "STRING"
                  }
                ],
                "data": [
                  {
                    "Channel": "ar"
                  },
                  {
                    "Channel": "be"
                  },
                  {
                    "Channel": "bg"
                  }
                ],
                "keys": [
                  "Channel"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Pages": {
                "attributes": [
                  {
                    "name": "Page",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
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
                ],
                "keys": [
                  "Page"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Pages": {
                "attributes": [
                  {
                    "name": "Page",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
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
                ],
                "keys": [
                  "Page"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Pages": {
                "attributes": [
                  {
                    "name": "Page",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
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
                ],
                "keys": [
                  "Page"
                ]
              }
            }
          ]);
        });
    });

    it('works with constant lookup split', () => {
      let ex = $('wiki').split(r('en').lookup('channel-lookup'), 'Channel')
        .apply('Count', '$wiki.sum($count)');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "Channel": "English",
              "Count": 392443
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
          expect(result.toJS().data).to.deep.equal([
            {
              "ChannelFallbackLOL": {
                "attributes": [
                  {
                    "name": "Channel",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
                  {
                    "Channel": "LOL",
                    "Count": 227040
                  },
                  {
                    "Channel": "English",
                    "Count": 114711
                  },
                  {
                    "Channel": "French",
                    "Count": 21285
                  },
                  {
                    "Channel": "Russian",
                    "Count": 14031
                  }
                ],
                "keys": [
                  "Channel"
                ]
              },
              "ChannelFallbackSelf": {
                "attributes": [
                  {
                    "name": "Channel",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
                  {
                    "Channel": "English",
                    "Count": 114711
                  },
                  {
                    "Channel": "vi",
                    "Count": 99010
                  },
                  {
                    "Channel": "de",
                    "Count": 25103
                  },
                  {
                    "Channel": "French",
                    "Count": 21285
                  }
                ],
                "keys": [
                  "Channel"
                ]
              },
              "Channels": {
                "attributes": [
                  {
                    "name": "Channel",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
                  {
                    "Channel": null,
                    "Count": 227040
                  },
                  {
                    "Channel": "English",
                    "Count": 114711
                  },
                  {
                    "Channel": "French",
                    "Count": 21285
                  },
                  {
                    "Channel": "Russian",
                    "Count": 14031
                  }
                ],
                "keys": [
                  "Channel"
                ]
              }
            }
          ]);
        });
    });

    it("works with count distinct on lookup", () => {
      let ex = ply()
        .apply('CntDistChannelNormal', $('wiki').countDistinct($('channel')))
        .apply('CntDistChannelLookup',  $('wiki').countDistinct($('channel').lookup('channel-lookup')))
        .apply('CntDistChannelLookupXPage',  $('wiki').countDistinct($('channel').lookup('channel-lookup').concat('$page.substr(0, 1)')));

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "CntDistChannelLookup": 6,
              "CntDistChannelNormal": 53,
              "CntDistChannelLookupXPage": 2641
            }
          ]);
        });
    });

    it("works with quantiles", () => {
      let ex = ply()
        .apply('deltaHist95', $('wiki').quantile($('delta_hist'), 0.95))
        .apply('deltaHistMedian', $('wiki').quantile($('delta_hist'), 0.5))
        .apply('deltaBucket95', $('wiki').quantile($('deltaBucket100'), 0.95))
        .apply('deltaBucketMedian', $('wiki').quantile($('deltaBucket100'), 0.5))
        .apply('commentLength95', $('wiki').quantile($('commentLength'), 0.95))
        .apply('commentLengthMedian', $('wiki').quantile($('commentLength'), 0.5));

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "commentLength95": 145.4637,
              "commentLengthMedian": 28.108896,
              "deltaBucket95": -500, // ToDo: find out why this is
              "deltaBucketMedian": -500,
              "deltaHist95": 161.95517,
              "deltaHistMedian": 129.0191
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
          expect(result.toJS().data).to.deep.equal([
            {
              "AbsSplitAsc": {
                "attributes": [
                  {
                    "name": "AbsCommentLength",
                    "type": "NUMBER"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
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
                "keys": [
                  "AbsCommentLength"
                ]
              },
              "AbsSplitDesc": {
                "attributes": [
                  {
                    "name": "AbsCommentLength",
                    "type": "NUMBER"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
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
                ],
                "keys": [
                  "AbsCommentLength"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
            {
              "BucketSplitAsc": {
                "attributes": [
                  {
                    "name": "Bucket",
                    "type": "NUMBER_RANGE"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
                  {
                    "Bucket": {
                      "end": 5,
                      "start": 0
                    },
                    "Count": 6522
                  },
                  {
                    "Bucket": {
                      "end": 10,
                      "start": 5
                    },
                    "Count": 15003
                  },
                  {
                    "Bucket": {
                      "end": 15,
                      "start": 10
                    },
                    "Count": 70628
                  }
                ],
                "keys": [
                  "Bucket"
                ]
              },
              "BucketSplitDesc": {
                "attributes": [
                  {
                    "name": "Bucket",
                    "type": "NUMBER_RANGE"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
                  {
                    "Bucket": {
                      "end": 260,
                      "start": 255
                    },
                    "Count": 193
                  },
                  {
                    "Bucket": {
                      "end": 255,
                      "start": 250
                    },
                    "Count": 556
                  },
                  {
                    "Bucket": {
                      "end": 250,
                      "start": 245
                    },
                    "Count": 1687
                  }
                ],
                "keys": [
                  "Bucket"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
            {
              "BucketSplitAsc": {
                "attributes": [
                  {
                    "name": "Bucket",
                    "type": "NUMBER_RANGE"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
                  {
                    "Bucket": {
                      "end": 5,
                      "start": 0
                    },
                    "Count": 6521
                  },
                  {
                    "Bucket": {
                      "end": 10,
                      "start": 5
                    },
                    "Count": 15004
                  },
                  {
                    "Bucket": {
                      "end": 15,
                      "start": 10
                    },
                    "Count": 70627
                  }
                ],
                "keys": [
                  "Bucket"
                ]
              },
              "BucketSplitDesc": {
                "attributes": [
                  {
                    "name": "Bucket",
                    "type": "NUMBER_RANGE"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
                  {
                    "Bucket": {
                      "end": 260,
                      "start": 255
                    },
                    "Count": 193
                  },
                  {
                    "Bucket": {
                      "end": 255,
                      "start": 250
                    },
                    "Count": 556
                  },
                  {
                    "Bucket": {
                      "end": 250,
                      "start": 245
                    },
                    "Count": 1687
                  }
                ],
                "keys": [
                  "Bucket"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Time": {
                "attributes": [
                  {
                    "name": "TimeCol",
                    "type": "TIME_RANGE"
                  }
                ],
                "data": [
                  {
                    "TimeCol": {
                      "end": new Date('2015-09-13T00:00:00.000Z'),
                      "start": new Date('2015-09-12T23:00:00.000Z')
                    }
                  },
                  {
                    "TimeCol": {
                      "end": new Date('2015-09-12T23:00:00.000Z'),
                      "start": new Date('2015-09-12T22:00:00.000Z')
                    }
                  }
                ],
                "keys": [
                  "TimeCol"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
            {
              "TimeLater": {
                "attributes": [
                  {
                    "name": "SometimeLater",
                    "type": "TIME_RANGE"
                  }
                ],
                "data": [
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T01:00:00.000Z'),
                      "start": new Date('2016-09-12T00:00:00.000Z')
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T02:00:00.000Z'),
                      "start": new Date('2016-09-12T01:00:00.000Z')
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T03:00:00.000Z'),
                      "start": new Date('2016-09-12T02:00:00.000Z')
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T04:00:00.000Z'),
                      "start": new Date('2016-09-12T03:00:00.000Z')
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T05:00:00.000Z'),
                      "start": new Date('2016-09-12T04:00:00.000Z')
                    }
                  }
                ],
                "keys": [
                  "SometimeLater"
                ]
              }
            }
          ]);
        });
    });

    it("can timeBucket a secondary time column (complex duration, tz - Asia/Kolkata)", () => {
      let ex = ply()
        .apply(
          'TimeLater',
          $("wiki").split($("sometimeLater").timeBucket('PT3H', 'Asia/Kolkata'), 'SometimeLater')
            .limit(5)
        );

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "TimeLater": {
                "attributes": [
                  {
                    "name": "SometimeLater",
                    "type": "TIME_RANGE"
                  }
                ],
                "data": [
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T03:30:00.000Z'),
                      "start": new Date('2016-09-12T00:30:00.000Z')
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T06:30:00.000Z'),
                      "start": new Date('2016-09-12T03:30:00.000Z')
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T09:30:00.000Z'),
                      "start": new Date('2016-09-12T06:30:00.000Z')
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T12:30:00.000Z'),
                      "start": new Date('2016-09-12T09:30:00.000Z')
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T15:30:00.000Z'),
                      "start": new Date('2016-09-12T12:30:00.000Z')
                    }
                  }
                ],
                "keys": [
                  "SometimeLater"
                ]
              }
            }
          ]);
        });
    });

    it.skip("can timeBucket a secondary time column (complex duration, tz - Kathmandu)", () => { // ToDo: wait for https://github.com/druid-io/druid/issues/4073
      let ex = ply()
        .apply(
          'TimeLater',
          $("wiki").split($("sometimeLater").timeBucket('PT3H', 'Asia/Kathmandu'), 'SometimeLater')
            .limit(5)
        );

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "TimeLater": {
                "attributes": [
                  {
                    "name": "SometimeLater",
                    "type": "TIME_RANGE"
                  }
                ],
                "data": [
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T03:15:00.000Z'),
                      "start": new Date('2016-09-12T00:15:00.000Z')
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T06:15:00.000Z'),
                      "start": new Date('2016-09-12T03:15:00.000Z')
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T09:15:00.000Z'),
                      "start": new Date('2016-09-12T06:15:00.000Z')
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T12:15:00.000Z'),
                      "start": new Date('2016-09-12T09:15:00.000Z')
                    }
                  },
                  {
                    "SometimeLater": {
                      "end": new Date('2016-09-12T15:15:00.000Z'),
                      "start": new Date('2016-09-12T12:15:00.000Z')
                    }
                  }
                ],
                "keys": [
                  "SometimeLater"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
            {
              "MaxCount": 15906,
              "MinCount": 14814,
              "data1": {
                "attributes": [
                  {
                    "name": "TimeCol",
                    "type": "TIME"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
                  {
                    "Count": 14814,
                    "TimeCol": new Date('2015-09-12T23:00:00.000Z')
                  },
                  {
                    "Count": 15906,
                    "TimeCol": new Date('2015-09-12T22:00:00.000Z')
                  }
                ],
                "keys": [
                  "TimeCol"
                ]
              }
            }
          ]);
        });
    });

    it.skip("can do a sub-split in aggregator", () => {
      let ex = $("wiki").split('$channel', 'Channel')
        .apply('Count', '$wiki.sum($count)')
        .apply('MinByRobot', '$wiki.split($isRobot, Blah).apply(Cnt, $wiki.sum($count)).min($Cnt)')
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([

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
          expect(result.toJS().data).to.deep.equal([
            {
              "Groups": {
                "attributes": [
                  {
                    "name": "Channel",
                    "type": "STRING"
                  },
                  {
                    "name": "ChannelIsDE",
                    "type": "BOOLEAN"
                  },
                  {
                    "name": "IsNew",
                    "type": "BOOLEAN"
                  },
                  {
                    "name": "TimeByHour",
                    "type": "TIME_RANGE"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
                  {
                    "Channel": "vi",
                    "ChannelIsDE": false,
                    "Count": 24258,
                    "IsNew": false,
                    "TimeByHour": {
                      "end": new Date('2015-09-12T08:00:00.000Z'),
                      "start": new Date('2015-09-12T06:00:00.000Z')
                    }
                  },
                  {
                    "Channel": "vi",
                    "ChannelIsDE": false,
                    "Count": 11215,
                    "IsNew": false,
                    "TimeByHour": {
                      "end": new Date('2015-09-12T18:00:00.000Z'),
                      "start": new Date('2015-09-12T16:00:00.000Z')
                    }
                  },
                  {
                    "Channel": "vi",
                    "ChannelIsDE": false,
                    "Count": 9246,
                    "IsNew": false,
                    "TimeByHour": {
                      "end": new Date('2015-09-12T16:00:00.000Z'),
                      "start": new Date('2015-09-12T14:00:00.000Z')
                    }
                  },
                  {
                    "Channel": "vi",
                    "ChannelIsDE": false,
                    "Count": 8917,
                    "IsNew": false,
                    "TimeByHour": {
                      "end": new Date('2015-09-12T10:00:00.000Z'),
                      "start": new Date('2015-09-12T08:00:00.000Z')
                    }
                  }
                ],
                "keys": [
                  "Channel",
                  "ChannelIsDE",
                  "IsNew",
                  "TimeByHour"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Groups": {
                "attributes": [
                  {
                    "name": "__time",
                    "type": "TIME_RANGE"
                  },
                  {
                    "name": "channel",
                    "type": "STRING"
                  },
                  {
                    "name": "Count",
                    "type": "NUMBER"
                  }
                ],
                "data": [
                  {
                    "Count": 24276,
                    "__time": {
                      "end": new Date('2015-09-12T08:00:00.000Z'),
                      "start": new Date('2015-09-12T06:00:00.000Z')
                    },
                    "channel": "vi"
                  },
                  {
                    "Count": 11223,
                    "__time": {
                      "end": new Date('2015-09-12T18:00:00.000Z'),
                      "start": new Date('2015-09-12T16:00:00.000Z')
                    },
                    "channel": "vi"
                  },
                  {
                    "Count": 9258,
                    "__time": {
                      "end": new Date('2015-09-12T16:00:00.000Z'),
                      "start": new Date('2015-09-12T14:00:00.000Z')
                    },
                    "channel": "vi"
                  },
                  {
                    "Count": 8928,
                    "__time": {
                      "end": new Date('2015-09-12T10:00:00.000Z'),
                      "start": new Date('2015-09-12T08:00:00.000Z')
                    },
                    "channel": "vi"
                  }
                ],
                "keys": [
                  "__time",
                  "channel"
                ]
              }
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
          expect(result.toJS().data).to.deep.equal([
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
          expect(result.toJS().data).to.deep.equal([
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
          expect(result.toJS()).to.deep.equal({
            "attributes": [
              {
                "name": "time",
                "type": "TIME"
              },
              {
                "name": "added",
                "type": "NUMBER"
              },
              {
                "name": "channel",
                "type": "STRING"
              },
              {
                "name": "cityName",
                "type": "STRING"
              },
              {
                "name": "comment",
                "type": "STRING"
              },
              {
                "name": "commentLength",
                "type": "NUMBER"
              },
              {
                "name": "commentLengthStr",
                "type": "STRING"
              },
              {
                "name": "count",
                "type": "NUMBER"
              },
              {
                "name": "countryIsoCode",
                "type": "STRING"
              },
              {
                "name": "countryName",
                "type": "STRING"
              },
              {
                "name": "deleted",
                "type": "NUMBER"
              },
              {
                "name": "delta",
                "type": "NUMBER"
              },
              {
                "name": "deltaBucket100",
                "type": "NUMBER"
              },
              {
                "name": "deltaByTen",
                "type": "NUMBER"
              },
              {
                "name": "delta_hist",
                "type": "NULL"
              },
              {
                "name": "isAnonymous",
                "type": "BOOLEAN"
              },
              {
                "name": "isMinor",
                "type": "BOOLEAN"
              },
              {
                "name": "isNew",
                "type": "BOOLEAN"
              },
              {
                "name": "isRobot",
                "type": "BOOLEAN"
              },
              {
                "name": "isUnpatrolled",
                "type": "BOOLEAN"
              },
              {
                "name": "max_delta",
                "type": "NUMBER"
              },
              {
                "name": "metroCode",
                "type": "STRING"
              },
              {
                "name": "min_delta",
                "type": "NUMBER"
              },
              {
                "name": "namespace",
                "type": "STRING"
              },
              {
                "name": "page",
                "type": "STRING"
              },
              {
                "name": "page_unique",
                "type": "NULL"
              },
              {
                "name": "regionIsoCode",
                "type": "STRING"
              },
              {
                "name": "regionName",
                "type": "STRING"
              },
              {
                "name": "sometimeLater",
                "type": "TIME"
              },
              {
                "name": "sometimeLaterMs",
                "type": "NUMBER"
              },
              {
                "name": "user",
                "type": "STRING"
              },
              {
                "name": "userChars",
                "type": "SET/STRING"
              },
              {
                "name": "user_theta",
                "type": "NULL"
              },
              {
                "name": "user_unique",
                "type": "NULL"
              }
            ],
            "data": [
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
                "deltaByTen": -3.9,
                "delta_hist": "/84BwhwAAA==",
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
                "sometimeLater": new Date('2016-09-12T06:05:00.000Z'),
                "sometimeLaterMs": 1473660300000,
                "time": new Date('2015-09-12T06:05:00.000Z'),
                "user": "104.58.160.128",
                "userChars": {
                  "elements": [
                    ".",
                    "0",
                    "1",
                    "2",
                    "4",
                    "5",
                    "6",
                    "8"
                  ],
                  "setType": "STRING"
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
                "delta_hist": "/84BAAAAAA==",
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
                "sometimeLater": new Date('2016-09-12T16:14:00.000Z'),
                "sometimeLaterMs": 1473696840000,
                "time": new Date('2015-09-12T16:14:00.000Z'),
                "user": "67.10.203.15",
                "userChars": {
                  "elements": [
                    ".",
                    "0",
                    "1",
                    "2",
                    "3",
                    "5",
                    "6",
                    "7"
                  ],
                  "setType": "STRING"
                },
                "user_theta": "AgMDAAAazJMBAAAAAACAPykGTa6KslE/",
                "user_unique": "AQAAAQAAAAOIQA=="
              }
            ]
          });
        });
    });

    it("works with raw (SELECT) + limit", () => {
      let ex = $('wiki').limit(1);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
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
              "deltaByTen": -0.1,
              "delta_hist": "/84Bv4AAAA==",
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
              "sometimeLater": new Date('2016-09-12T00:46:00.000Z'),
              "sometimeLaterMs": 1473641160000,
              "time": new Date('2015-09-12T00:46:00.000Z'),
              "user": "ChandraHelsinky",
              "userChars": {
                "elements": ["A", "C", "D", "E", "H", "I", "K", "L", "N", "R", "S", "Y"],
                "setType": "STRING"
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
          expect(result.toJS().data.length).to.equal(limit);
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
          expect(result.toJS().data.length).to.equal(limit);
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
          expect(result.toJS().data).to.deep.equal([
            { "userChar": "B" },
            { "userChar": "N" }
          ]);
        });
    });

    it("works with multi-value dimension list (in) having filter", () => {
      let ex = $("wiki")
        .filter('$userChars.match("[ABN]")')
        .split("$userChars", 'userChar')
        .filter('$userChar == "B" or $userChar == "N"')
        .limit(5);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Added": 54157728,
              "Count": 274140
            }
          ]);
        });
    });

    it("works with filtered double split", () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('time').overlap(new Date('2015-09-11T23:59:00Z'), new Date('2015-09-12T23:59:00Z'))))
        .apply('count', '$wiki.sum($count)')
        .apply(
          'SPLIT',
          $('wiki').split('$page','page')
            .filter($('page').overlap(['Jeremy Corbyn', 'KalyeSerye']))
            .apply('count', '$wiki.sum($count)')
            .sort('$count', 'descending')
            .limit(2)
            .apply(
              'SPLIT',
              $('wiki').split('$time.timeBucket(PT1H)','time')
                .apply('count', '$wiki.sum($count)')
                .sort('$time', 'ascending')
                .limit(2)
            )
        );

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "SPLIT": {
                "attributes": [
                  {
                    "name": "page",
                    "type": "STRING"
                  },
                  {
                    "name": "count",
                    "type": "NUMBER"
                  },
                  {
                    "name": "SPLIT",
                    "type": "DATASET"
                  }
                ],
                "data": [
                  {
                    "SPLIT": {
                      "attributes": [
                        {
                          "name": "time",
                          "type": "TIME_RANGE"
                        },
                        {
                          "name": "count",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "count": 1,
                          "time": {
                            "end": new Date('2015-09-12T02:00:00.000Z'),
                            "start": new Date('2015-09-12T01:00:00.000Z')
                          }
                        },
                        {
                          "count": 1,
                          "time": {
                            "end": new Date('2015-09-12T08:00:00.000Z'),
                            "start": new Date('2015-09-12T07:00:00.000Z')
                          }
                        }
                      ],
                      "keys": [
                        "time"
                      ]
                    },
                    "count": 318,
                    "page": "Jeremy Corbyn"
                  },
                  {
                    "SPLIT": {
                      "attributes": [
                        {
                          "name": "time",
                          "type": "TIME_RANGE"
                        },
                        {
                          "name": "count",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "count": 1,
                          "time": {
                            "end": new Date('2015-09-12T02:00:00.000Z'),
                            "start": new Date('2015-09-12T01:00:00.000Z')
                          }
                        },
                        {
                          "count": 1,
                          "time": {
                            "end": new Date('2015-09-12T03:00:00.000Z'),
                            "start": new Date('2015-09-12T02:00:00.000Z')
                          }
                        }
                      ],
                      "keys": [
                        "time"
                      ]
                    },
                    "count": 69,
                    "page": "KalyeSerye"
                  }
                ],
                "keys": [
                  "page"
                ]
              },
              "count": 392239
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
            expect(result.toJS().data).to.deep.equal([
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

      it("works with two datasets totals only", () => {
        let ex = ply()
          .apply("wikiA", $('wiki').filter($('time').overlap(new Date("2015-09-12T12:00:00Z"), new Date("2015-09-13T00:00:00Z"))))
          .apply("wikiB", $('wiki').filter($('time').overlap(new Date("2015-09-12T00:00:00Z"), new Date("2015-09-12T12:00:00Z"))))
          .apply('CountA', '$wikiA.sum($count)')
          .apply('TotalAddedA', '$wikiA.sum($added)')
          .apply('CountB', '$wikiB.sum($count)');

        return basicExecutor(ex)
          .then((result) => {
            expect(result.toJS().data).to.deep.equal([
              {
                "CountA": 227318,
                "CountB": 165125,
                "TotalAddedA": 55970642
              }
            ]);
          });
      });

      it.skip("works with two datasets with split", () => {
        let ex = ply()
          .apply("wikiA", $('wiki').filter($('time').overlap(new Date("2015-09-12T12:00:00Z"), new Date("2015-09-13T00:00:00Z"))))
          .apply("wikiB", $('wiki').filter($('time').overlap(new Date("2015-09-12T00:00:00Z"), new Date("2015-09-12T12:00:00Z"))))
          .apply('CountA', '$wikiA.sum($count)')
          .apply('TotalAddedA', '$wikiA.sum($added)')
          .apply('CountB', '$wikiB.sum($count)')
          .apply(
            'Sub',
            $('wikiA').split('$user', 'User').join($('wikiB').split('$user', 'User'))
              .apply('CountA', '$wikiA.sum($count)')
              .apply('CountB', '$wikiB.sum($count)')
          );

        return basicExecutor(ex)
          .then((result) => {
            expect(result.toJS().data).to.deep.equal({

            });
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Count": 2658542,
              "U": 0
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Count": 2658542,
              "U": null
            },
            {
              "Count": 151159,
              "U": {
                "end": 7.5,
                "start": 2.5
              }
            },
            {
              "Count": 150305,
              "U": {
                "end": 2.5,
                "start": -2.5
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Count": 2658542,
              "U": 0
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Count": 2658542,
              "dateCast": null,
              "numberCast": 0
            },
            {
              "Count": 379865,
              "dateCast": new Date('1970-01-01T00:00:00.000Z'),
              "numberCast": 0
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
      filter: $('time').overlap(TimeRange.fromJS({
        start: new Date("2015-09-12T00:00:00Z"),
        end: new Date("2015-09-13T00:00:00Z")
      })),
      attributeOverrides: [
        { "name": "sometimeLater", "type": "TIME" },
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
          expect(introspectedExternal.toJS().attributes.slice(0, 3)).to.deep.equal([
            {
              "name": "time",
              "nativeType": "__time",
              "range": {
                "bounds": "[]",
                "end": new Date('2015-09-12T23:59:00.000Z'),
                "start": new Date('2015-09-12T00:46:00.000Z')
              },
              "type": "TIME"
            },
            {
              "maker": {
                "expression": {
                  "name": "added",
                  "op": "ref"
                },
                "op": "sum"
              },
              "name": "added",
              "nativeType": "LONG",
              "type": "NUMBER",
              "unsplitable": true
            },
            {
              "name": "channel",
              "nativeType": "STRING",
              "type": "STRING"
            }
          ]);
        });
    });

    it("introspects attributes (shallow)", () => {
      return wikiExternal.introspect({ depth: 'shallow' })
        .then((introspectedExternal) => {
          expect(introspectedExternal.toJS().attributes.slice(0, 3)).to.deep.equal([
            {
              "name": "time",
              "nativeType": "__time",
              "type": "TIME"
            },
            {
              "maker": {
                "expression": {
                  "name": "added",
                  "op": "ref"
                },
                "op": "sum"
              },
              "name": "added",
              "nativeType": "LONG",
              "type": "NUMBER",
              "unsplitable": true
            },
            {
              "name": "channel",
              "nativeType": "STRING",
              "type": "STRING"
            }
          ]);
        });
    });

    it("introspects attributes (deep)", () => {
      return wikiExternal.introspect({ depth: 'deep' })
        .then((introspectedExternal) => {
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Count": 114711,
              "Time": {
                "attributes": [
                  {
                    "name": "Timestamp",
                    "type": "TIME_RANGE"
                  },
                  {
                    "name": "TotalAdded",
                    "type": "NUMBER"
                  },
                  {
                    "name": "Pages",
                    "type": "DATASET"
                  }
                ],
                "data": [
                  {
                    "Pages": {
                      "attributes": [
                        {
                          "name": "Page",
                          "type": "STRING"
                        },
                        {
                          "name": "Count",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Count": 12,
                          "Page": "User talk:Dudeperson176123"
                        },
                        {
                          "Count": 8,
                          "Page": "User:Attar-Aram syria/sandbox"
                        }
                      ],
                      "keys": [
                        "Page"
                      ]
                    },
                    "Timestamp": {
                      "end": new Date('2015-09-12T01:00:00.000Z'),
                      "start": new Date('2015-09-12T00:00:00.000Z')
                    },
                    "TotalAdded": 331925
                  },
                  {
                    "Pages": {
                      "attributes": [
                        {
                          "name": "Page",
                          "type": "STRING"
                        },
                        {
                          "name": "Count",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Count": 17,
                          "Page": "John Adams"
                        },
                        {
                          "Count": 17,
                          "Page": "User:King Lui"
                        }
                      ],
                      "keys": [
                        "Page"
                      ]
                    },
                    "Timestamp": {
                      "end": new Date('2015-09-12T02:00:00.000Z'),
                      "start": new Date('2015-09-12T01:00:00.000Z')
                    },
                    "TotalAdded": 1418072
                  },
                  {
                    "Pages": {
                      "attributes": [
                        {
                          "name": "Page",
                          "type": "STRING"
                        },
                        {
                          "name": "Count",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Count": 28,
                          "Page": "Wikipedia:Administrators' noticeboard/Incidents"
                        },
                        {
                          "Count": 18,
                          "Page": "2015 World Wrestling Championships"
                        }
                      ],
                      "keys": [
                        "Page"
                      ]
                    },
                    "Timestamp": {
                      "end": new Date('2015-09-12T03:00:00.000Z'),
                      "start": new Date('2015-09-12T02:00:00.000Z')
                    },
                    "TotalAdded": 3045966
                  }
                ],
                "keys": [
                  "Timestamp"
                ]
              },
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
      filter: $('time').overlap(TimeRange.fromJS({
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
          expect(result.toJS().data).to.deep.equal([
            {
              "Count": 229422,
              "Time": {
                "attributes": [
                  {
                    "name": "Timestamp",
                    "type": "TIME_RANGE"
                  },
                  {
                    "name": "TotalAdded",
                    "type": "NUMBER"
                  }
                ],
                "data": [
                  {
                    "Timestamp": {
                      "end": new Date('2015-09-12T01:00:00.000Z'),
                      "start": new Date('2015-09-12T00:00:00.000Z')
                    },
                    "TotalAdded": 663850
                  },
                  {
                    "Timestamp": {
                      "end": new Date('2015-09-12T02:00:00.000Z'),
                      "start": new Date('2015-09-12T01:00:00.000Z')
                    },
                    "TotalAdded": 2836144
                  },
                  {
                    "Timestamp": {
                      "end": new Date('2015-09-12T03:00:00.000Z'),
                      "start": new Date('2015-09-12T02:00:00.000Z')
                    },
                    "TotalAdded": 6091932
                  }
                ],
                "keys": [
                  "Timestamp"
                ]
              },
              "TotalAdded": 65106214
            }
          ]);
        });
    });

  });

  describe("introspection on non existent dataSource", () => {
    let wikiExternal = External.fromJS({
      engine: 'druid',
      source: 'wikipedia_borat',
      timeAttribute: 'time',
      filter: $('time').overlap(TimeRange.fromJS({
        start: new Date("2015-09-12T00:00:00Z"),
        end: new Date("2015-09-13T00:00:00Z")
      }))
    }, druidRequester);

    it("fail correctly", () => {
      return wikiExternal.introspect()
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch((e) => {
          expect(e.message).to.contain('No such datasource');
        })
    });
  });

});
