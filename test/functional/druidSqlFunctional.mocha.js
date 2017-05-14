/*
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
let { External, DruidSQLExternal, TimeRange, $, i$, ply, r, basicExecutorFactory, verboseRequesterFactory, Expression } = plywood;

let info = require('../info');

let druidRequester = druidRequesterFactory({
  host: info.druidHost
});

// druidRequester = verboseRequesterFactory({
//  requester: druidRequester
// });

describe("DruidSQL Functional", function() {
  this.timeout(10000);

  let wikiAttributes = [
    {
      "name": "__time",
      "nativeType": "TIMESTAMP",
      "type": "TIME"
    },
    {
      "name": "added",
      "nativeType": "BIGINT",
      "type": "NUMBER"
    },
    {
      "name": "channel",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "cityName",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "comment",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "commentLength",
      "nativeType": "BIGINT",
      "type": "NUMBER"
    },
    {
      "name": "commentLengthStr",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "count",
      "nativeType": "BIGINT",
      "type": "NUMBER"
    },
    {
      "name": "countryIsoCode",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "countryName",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "deleted",
      "nativeType": "BIGINT",
      "type": "NUMBER"
    },
    {
      "name": "delta",
      "nativeType": "BIGINT",
      "type": "NUMBER"
    },
    {
      "name": "deltaBucket100",
      "nativeType": "FLOAT",
      "type": "NUMBER"
    },
    {
      "name": "deltaByTen",
      "nativeType": "FLOAT",
      "type": "NUMBER"
    },
    {
      "name": "delta_hist",
      "nativeType": "OTHER",
      "type": "NULL"
    },
    {
      "name": "isAnonymous",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "isMinor",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "isNew",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "isRobot",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "isUnpatrolled",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "max_delta",
      "nativeType": "BIGINT",
      "type": "NUMBER"
    },
    {
      "name": "metroCode",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "min_delta",
      "nativeType": "BIGINT",
      "type": "NUMBER"
    },
    {
      "name": "namespace",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "page",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "page_unique",
      "nativeType": "OTHER",
      "type": "NULL"
    },
    {
      "name": "regionIsoCode",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "regionName",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "sometimeLater",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "sometimeLaterMs",
      "nativeType": "BIGINT",
      "type": "NUMBER"
    },
    {
      "name": "user",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "userChars",
      "nativeType": "VARCHAR",
      "type": "STRING"
    },
    {
      "name": "user_theta",
      "nativeType": "OTHER",
      "type": "NULL"
    },
    {
      "name": "user_unique",
      "nativeType": "OTHER",
      "type": "NULL"
    }
  ];

  let wikiDerivedAttributes = {
    pageInBrackets: "'[' ++ $page ++ ']'"
  };

  describe("source list", () => {
    it("does a source list", () => {
      return DruidSQLExternal.getSourceList(druidRequester)
        .then((sources) => {
          expect(sources).to.deep.equal(['wikipedia', 'wikipedia-compact']);
        })
    });

  });


  describe("defined attributes in datasource", () => {
    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'druidsql',
          source: 'wikipedia',
          attributes: wikiAttributes,
          derivedAttributes: wikiDerivedAttributes
        }, druidRequester)
      }
    });

    it("works in simple case", () => {
      let ex = $("wiki").split("$channel", 'Channel')
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "Channel": "en",
              "Count": 114711
            },
            {
              "Channel": "vi",
              "Count": 99010
            },
            {
              "Channel": "de",
              "Count": 25103
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
          'Namespaces',
          $("wiki").split("$namespace", 'Namespace')
            .apply('Added', '$wiki.sum($added)')
            .sort('$Added', 'descending')
            .limit(2)
            .apply(
              'Time',
              $("wiki").split($("__time").timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
                .apply('TotalAdded', '$wiki.sum($added)')
                .sort('$TotalAdded', 'descending')
                .limit(3)
            )
        );
      // .apply(
      //   'PagesHaving',
      //   $("wiki").split("$page", 'Page')
      //     .apply('Count', '$wiki.sum($count)')
      //     .sort('$Count', 'descending')
      //     .filter($('Count').lessThan(30))
      //     .limit(3)
      // );

      let rawQueries = [];
      return basicExecutor(ex, { rawQueries })
        .then((result) => {
          expect(rawQueries).to.deep.equal([
            {
              "query": {
                "query": "SELECT\nSUM(\"count\") AS \"Count\",\nSUM(\"added\") AS \"TotalAdded\"\nFROM \"wikipedia\"\nWHERE (\"channel\"='en')\nGROUP BY ''"
              }
            },
            {
              "query": {
                "query": "SELECT\n\"namespace\" AS \"Namespace\",\nSUM(\"added\") AS \"Added\"\nFROM \"wikipedia\"\nWHERE (\"channel\"='en')\nGROUP BY \"namespace\"\nORDER BY \"Added\" DESC\nLIMIT 2"
              }
            },
            {
              "query": {
                "query": "SELECT\nFLOOR(\"__time\" TO hour) AS \"Timestamp\",\nSUM(\"added\") AS \"TotalAdded\"\nFROM \"wikipedia\"\nWHERE ((\"channel\"='en') AND (\"namespace\"='Main'))\nGROUP BY FLOOR(\"__time\" TO hour)\nORDER BY \"TotalAdded\" DESC\nLIMIT 3"
              }
            },
            {
              "query": {
                "query": "SELECT\nFLOOR(\"__time\" TO hour) AS \"Timestamp\",\nSUM(\"added\") AS \"TotalAdded\"\nFROM \"wikipedia\"\nWHERE ((\"channel\"='en') AND (\"namespace\"='User talk'))\nGROUP BY FLOOR(\"__time\" TO hour)\nORDER BY \"TotalAdded\" DESC\nLIMIT 3"
              }
            }
          ]);

          expect(result.toJS().data).to.deep.equal([
            {
              "Count": 114711,
              "Namespaces": {
                "attributes": [
                  {
                    "name": "Namespace",
                    "type": "STRING"
                  },
                  {
                    "name": "Added",
                    "type": "NUMBER"
                  },
                  {
                    "name": "Time",
                    "type": "DATASET"
                  }
                ],
                "data": [
                  {
                    "Added": 11594002,
                    "Namespace": "Main",
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
                            "end": new Date('2015-09-12T15:00:00.000Z'),
                            "start": new Date('2015-09-12T14:00:00.000Z')
                          },
                          "TotalAdded": 740968
                        },
                        {
                          "Timestamp": {
                            "end": new Date('2015-09-12T19:00:00.000Z'),
                            "start": new Date('2015-09-12T18:00:00.000Z')
                          },
                          "TotalAdded": 739956
                        },
                        {
                          "Timestamp": {
                            "end": new Date('2015-09-12T23:00:00.000Z'),
                            "start": new Date('2015-09-12T22:00:00.000Z')
                          },
                          "TotalAdded": 708543
                        }
                      ],
                      "keys": [
                        "Timestamp"
                      ]
                    }
                  },
                  {
                    "Added": 9210976,
                    "Namespace": "User talk",
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
                          "TotalAdded": 693571
                        },
                        {
                          "Timestamp": {
                            "end": new Date('2015-09-12T18:00:00.000Z'),
                            "start": new Date('2015-09-12T17:00:00.000Z')
                          },
                          "TotalAdded": 634804
                        },
                        {
                          "Timestamp": {
                            "end": new Date('2015-09-12T03:00:00.000Z'),
                            "start": new Date('2015-09-12T02:00:00.000Z')
                          },
                          "TotalAdded": 573768
                        }
                      ],
                      "keys": [
                        "Timestamp"
                      ]
                    }
                  }
                ],
                "keys": [
                  "Namespace"
                ]
              },
              "TotalAdded": 32553107
            }
          ]);
        });
    });

    it.skip("works with boolean GROUP BYs", () => {
      let ex = $("wiki").split($("channel").is("en"), 'ChannelIsEn')
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Count', 'descending');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "ChannelIsEn": false,
              "Count": 277732
            },
            {
              "ChannelIsEn": true,
              "Count": 114711
            }
          ]);
        });
    });

    it("works string range", () => {
      let ex = $('wiki')
        .filter($('cityName').greaterThan('Eagleton'))
        .split('$cityName', 'CityName')
        .sort('$CityName', 'descending')
        .limit(10);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "CityName": "Ōita"
            },
            {
              "CityName": "Łódź"
            },
            {
              "CityName": "İzmit"
            },
            {
              "CityName": "České Budějovice"
            },
            {
              "CityName": "Ürümqi"
            },
            {
              "CityName": "Ústí nad Labem"
            },
            {
              "CityName": "Évry"
            },
            {
              "CityName": "Épinay-sur-Seine"
            },
            {
              "CityName": "Épernay"
            },
            {
              "CityName": "Élancourt"
            }
          ]);
        });
    });
  });

  describe("incorrect commentLength and comment", () => {
    let wikiUserCharAsNumber = External.fromJS({
      engine: 'druidsql',
      source: 'wikipedia',
      timeAttribute: 'time',
      allowEternity: true,
      attributes: [
        { name: 'time', type: 'TIME' },
        { name: "comment", type: "STRING" },
        { name: 'page', type: 'NUMBER' }, // This is incorrect
        { name: 'count', type: 'NUMBER', unsplitable: true }
      ]
    }, druidRequester);

  });

  describe("introspection", () => {
    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'druidsql',
          source: 'wikipedia'
        }, druidRequester)
      }
    });

    it("introspects", () => {
      return External.fromJS({
        engine: 'druidsql',
        source: 'wikipedia'
      }, druidRequester).introspect()
        .then((external) => {
          expect(external.version).to.equal(info.druidVersion);
          expect(external.toJS().attributes).to.deep.equal(wikiAttributes);
        });
    });

    it.skip("works with introspection", () => { // ToDo: needs null check correction
      let ex = ply()
        .apply("wiki", $('wiki').filter($("channel").is('en')))
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply(
          'Time',
          $("wiki").split($("__time").timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$Timestamp', 'ascending')
            .limit(3)
            .apply(
              'Pages',
              $("wiki").split("$regionName", 'RegionName')
                .apply('Deleted', '$wiki.sum($deleted)')
                .sort('$Deleted', 'descending')
                .limit(2)
            )
        );

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
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
                          "name": "RegionName",
                          "type": "STRING"
                        },
                        {
                          "name": "Deleted",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Deleted": 11807,
                          "RegionName": null
                        },
                        {
                          "Deleted": 848,
                          "RegionName": "Ontario"
                        }
                      ],
                      "keys": [
                        "RegionName"
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
                          "name": "RegionName",
                          "type": "STRING"
                        },
                        {
                          "name": "Deleted",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Deleted": 109934,
                          "RegionName": null
                        },
                        {
                          "Deleted": 474,
                          "RegionName": "Indiana"
                        }
                      ],
                      "keys": [
                        "RegionName"
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
                          "name": "RegionName",
                          "type": "STRING"
                        },
                        {
                          "name": "Deleted",
                          "type": "NUMBER"
                        }
                      ],
                      "data": [
                        {
                          "Deleted": 124999,
                          "RegionName": null
                        },
                        {
                          "Deleted": 449,
                          "RegionName": "Georgia"
                        }
                      ],
                      "keys": [
                        "RegionName"
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

});
