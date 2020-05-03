/*
 * Copyright 2015-2020 Imply Data, Inc.
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

let { postgresRequesterFactory } = require('plywood-postgres-requester');

let plywood = require('../plywood');
let { External, PostgresExternal, TimeRange, $, ply, basicExecutorFactory, verboseRequesterFactory } = plywood;

let info = require('../info');

let postgresRequester = postgresRequesterFactory({
  host: info.postgresHost,
  database: info.postgresDatabase,
  user: info.postgresUser,
  password: info.postgresPassword
});

// postgresRequester = verboseRequesterFactory({
//  requester: postgresRequester
// });

describe("Postgres Functional", function() {
  this.timeout(10000);

  let wikiAttributes = [
    {
      "name": "__time",
      "nativeType": "timestamp without time zone",
      "type": "TIME"
    },
    {
      "name": "sometimeLater",
      "nativeType": "timestamp without time zone",
      "type": "TIME"
    },
    {
      "name": "channel",
      "nativeType": "character varying",
      "type": "STRING"
    },
    {
      "name": "cityName",
      "nativeType": "character varying",
      "type": "STRING"
    },
    {
      "name": "comment",
      "nativeType": "character varying",
      "type": "STRING"
    },
    {
      "name": "commentLength",
      "nativeType": "integer",
      "type": "NUMBER"
    },
    {
      "name": "commentLengthStr",
      "nativeType": "character varying",
      "type": "STRING"
    },
    {
      "name": "countryIsoCode",
      "nativeType": "character varying",
      "type": "STRING"
    },
    {
      "name": "countryName",
      "nativeType": "character varying",
      "type": "STRING"
    },
    {
      "name": "deltaBucket100",
      "nativeType": "integer",
      "type": "NUMBER"
    },
    {
      "name": "isAnonymous",
      "nativeType": "boolean",
      "type": "BOOLEAN"
    },
    {
      "name": "isMinor",
      "nativeType": "boolean",
      "type": "BOOLEAN"
    },
    {
      "name": "isNew",
      "nativeType": "boolean",
      "type": "BOOLEAN"
    },
    {
      "name": "isRobot",
      "nativeType": "boolean",
      "type": "BOOLEAN"
    },
    {
      "name": "isUnpatrolled",
      "nativeType": "boolean",
      "type": "BOOLEAN"
    },
    {
      "name": "metroCode",
      "nativeType": "integer",
      "type": "NUMBER"
    },
    {
      "name": "namespace",
      "nativeType": "character varying",
      "type": "STRING"
    },
    {
      "name": "page",
      "nativeType": "character varying",
      "type": "STRING"
    },
    {
      "name": "regionIsoCode",
      "nativeType": "character varying",
      "type": "STRING"
    },
    {
      "name": "regionName",
      "nativeType": "character varying",
      "type": "STRING"
    },
    {
      "name": "geohash",
      "nativeType": "character varying",
      "type": "STRING"
    },
    {
      "name": "user",
      "nativeType": "character varying",
      "type": "STRING"
    },
    {
      "name": "userChars",
      "nativeType": "character",
      "type": "SET/STRING"
    },
    {
      "name": "count",
      "nativeType": "bigint",
      "type": "NUMBER"
    },
    {
      "name": "added",
      "nativeType": "bigint",
      "type": "NUMBER"
    },
    {
      "name": "deleted",
      "nativeType": "bigint",
      "type": "NUMBER"
    },
    {
      "name": "delta",
      "nativeType": "bigint",
      "type": "NUMBER"
    },
    {
      "name": "min_delta",
      "nativeType": "integer",
      "type": "NUMBER"
    },
    {
      "name": "max_delta",
      "nativeType": "integer",
      "type": "NUMBER"
    },
    {
      "name": "deltaByTen",
      "nativeType": "double precision",
      "type": "NUMBER"
    }
  ];

  let wikiDerivedAttributes = {
    pageInBrackets: "'[' ++ $page ++ ']'"
  };

  describe("source list", () => {
    it("does a source list", () => {
      return PostgresExternal.getSourceList(postgresRequester)
        .then((sources) => {
          expect(sources).to.deep.equal(['wikipedia', 'wikipedia_raw']);
        })
    });

  });


  describe("defined attributes in datasource", () => {
    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'postgres',
          source: 'wikipedia',
          attributes: wikiAttributes,
          derivedAttributes: wikiDerivedAttributes
        }, postgresRequester)
      }
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

      return basicExecutor(ex)
        .then((result) => {
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

    it("works with boolean GROUP BYs", () => {
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

    it("filters on set/string select", () => {
      let ex = $('wiki').filter('$userChars.cardinality() > 5')
        .filter($("channel").is('war'))
        .select("userChars", "commentLength")
        .sort('$commentLength', 'descending')
        .limit(5);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "commentLength": 179,
              "userChars": {
                "elements": [
                  ".",
                  "1",
                  "2",
                  "4",
                  "7",
                  "8",
                  "9"
                ],
                "setType": "STRING"
              }
            },
            {
              "commentLength": 37,
              "userChars": {
                "elements": [
                  ".",
                  "0",
                  "1",
                  "2",
                  "4",
                  "6",
                  "7",
                  "9"
                ],
                "setType": "STRING"
              }
            },
            {
              "commentLength": 11,
              "userChars": {
                "elements": [
                  "A",
                  "B",
                  "I",
                  "K",
                  "N",
                  "O",
                  "R"
                ],
                "setType": "STRING"
              }
            }]
          );
        })
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
      engine: 'postgres',
      source: 'wikipedia',
      timeAttribute: 'time',
      allowEternity: true,
      attributes: [
        { name: 'time', type: 'TIME' },
        { name: "comment", type: "STRING" },
        { name: 'page', type: 'NUMBER' }, // This is incorrect
        { name: 'count', type: 'NUMBER', unsplitable: true }
      ]
    }, postgresRequester);

  });

  describe("introspection", () => {
    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'postgres',
          source: 'wikipedia'
        }, postgresRequester)
      }
    });

    it("introspects", () => {
      return External.fromJS({
        engine: 'postgres',
        source: 'wikipedia'
      }, postgresRequester).introspect()
        .then((external) => {
          expect(external.version).to.equal(info.postgresVersion);
          expect(external.toJS().attributes).to.deep.equal(wikiAttributes);
        });
    });

    it("works with introspection", () => {
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
