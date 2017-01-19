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
    { "name": "time", "type": "TIME" },
    { "name": "sometimeLater", "type": "TIME" },
    { "name": "channel", "type": "STRING" },
    { "name": "cityName", "type": "STRING" },
    { "name": "comment", "type": "STRING" },
    { "name": "commentLength", "type": "NUMBER" },
    { "name": "commentLengthStr", "type": "STRING" },
    { "name": "countryIsoCode", "type": "STRING" },
    { "name": "countryName", "type": "STRING" },
    { "name": "deltaBucket100", "type": "NUMBER" },
    { "name": "isAnonymous", "type": "BOOLEAN" },
    { "name": "isMinor", "type": "BOOLEAN" },
    { "name": "isNew", "type": "BOOLEAN" },
    { "name": "isRobot", "type": "BOOLEAN" },
    { "name": "isUnpatrolled", "type": "BOOLEAN" },
    { "name": "metroCode", "type": "NUMBER" },
    { "name": "namespace", "type": "STRING" },
    { "name": "page", "type": "STRING" },
    { "name": "regionIsoCode", "type": "STRING" },
    { "name": "regionName", "type": "STRING" },
    { "name": "user", "type": "STRING" },
    { "name": "userChars", "type": "SET/STRING" },
    { "name": "count", "type": "NUMBER" },
    { "name": "added", "type": "NUMBER" },
    { "name": "deleted", "type": "NUMBER" },
    { "name": "delta", "type": "NUMBER" },
    { "name": "min_delta", "type": "NUMBER" },
    { "name": "max_delta", "type": "NUMBER" },
    { "name": "deltaByTen", "type": "NUMBER" }
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
              $("wiki").split($("time").timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
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
          expect(result.toJS()).to.deep.equal([
            {
              "Count": 114711,
              "Namespaces": [
                {
                  "Added": 11594002,
                  "Namespace": "Main",
                  "Time": [
                    {
                      "Timestamp": {
                        "end": new Date('2015-09-12T15:00:00.000Z'),
                        "start": new Date('2015-09-12T14:00:00.000Z'),
                        "type": "TIME_RANGE"
                      },
                      "TotalAdded": 740968
                    },
                    {
                      "Timestamp": {
                        "end": new Date('2015-09-12T19:00:00.000Z'),
                        "start": new Date('2015-09-12T18:00:00.000Z'),
                        "type": "TIME_RANGE"
                      },
                      "TotalAdded": 739956
                    },
                    {
                      "Timestamp": {
                        "end": new Date('2015-09-12T23:00:00.000Z'),
                        "start": new Date('2015-09-12T22:00:00.000Z'),
                        "type": "TIME_RANGE"
                      },
                      "TotalAdded": 708543
                    }
                  ]
                },
                {
                  "Added": 9210976,
                  "Namespace": "User talk",
                  "Time": [
                    {
                      "Timestamp": {
                        "end": new Date('2015-09-12T13:00:00.000Z'),
                        "start": new Date('2015-09-12T12:00:00.000Z'),
                        "type": "TIME_RANGE"
                      },
                      "TotalAdded": 693571
                    },
                    {
                      "Timestamp": {
                        "end": new Date('2015-09-12T18:00:00.000Z'),
                        "start": new Date('2015-09-12T17:00:00.000Z'),
                        "type": "TIME_RANGE"
                      },
                      "TotalAdded": 634804
                    },
                    {
                      "Timestamp": {
                        "end": new Date('2015-09-12T03:00:00.000Z'),
                        "start": new Date('2015-09-12T02:00:00.000Z'),
                        "type": "TIME_RANGE"
                      },
                      "TotalAdded": 573768
                    }
                  ]
                }
              ],
              "TotalAdded": 32553107
              // "PagesHaving": [
              //   {
              //     "Count": 29,
              //     "Page": "User:King Lui"
              //   },
              //   {
              //     "Count": 29,
              //     "Page": "The Visit (2015 film)"
              //   },
              //   {
              //     "Count": 29,
              //     "Page": "Stargate production discography"
              //   }
              // ]
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
          expect(result.toJS()).to.deep.equal([
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
          expect(result.toJS()).to.deep.equal([
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
                "setType": "STRING",
                "type": "SET"
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
                "setType": "STRING",
                "type": "SET"
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
                "setType": "STRING",
                "type": "SET"
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
          expect(result.toJS()).to.deep.equal([
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
          $("wiki").split($("time").timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
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
          expect(result.toJS()).to.deep.equal([
            {
              "Time": [
                {
                  "Pages": [
                    {
                      "Deleted": 11807,
                      "RegionName": null
                    },
                    {
                      "Deleted": 848,
                      "RegionName": "Ontario"
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
                      "Deleted": 109934,
                      "RegionName": null
                    },
                    {
                      "Deleted": 474,
                      "RegionName": "Indiana"
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
                      "Deleted": 124999,
                      "RegionName": null
                    },
                    {
                      "Deleted": 449,
                      "RegionName": "Georgia"
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

});
