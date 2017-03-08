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

let { mySqlRequesterFactory } = require('plywood-mysql-requester');

let plywood = require('../plywood');
let { External, MySQLExternal, TimeRange, $, ply, basicExecutorFactory, verboseRequesterFactory } = plywood;

let info = require('../info');

let mySqlRequester = mySqlRequesterFactory({
  host: info.mySqlHost,
  database: info.mySqlDatabase,
  user: info.mySqlUser,
  password: info.mySqlPassword
});

//mySqlRequester = verboseRequesterFactory({
//  requester: mySqlRequester
//});

describe("MySQL Functional", function() {
  this.timeout(10000);

  let wikiAttributes = [
    {
      "name": "__time",
      "nativeType": "datetime",
      "type": "TIME"
    },
    {
      "name": "sometimeLater",
      "nativeType": "timestamp",
      "type": "TIME"
    },
    {
      "name": "channel",
      "nativeType": "varchar(255)",
      "type": "STRING"
    },
    {
      "name": "cityName",
      "nativeType": "varchar(255)",
      "type": "STRING"
    },
    {
      "name": "comment",
      "nativeType": "varchar(300)",
      "type": "STRING"
    },
    {
      "name": "commentLength",
      "nativeType": "int(11)",
      "type": "NUMBER"
    },
    {
      "name": "commentLengthStr",
      "nativeType": "varchar(10)",
      "type": "STRING"
    },
    {
      "name": "countryIsoCode",
      "nativeType": "varchar(255)",
      "type": "STRING"
    },
    {
      "name": "countryName",
      "nativeType": "varchar(255)",
      "type": "STRING"
    },
    {
      "name": "deltaBucket100",
      "nativeType": "int(11)",
      "type": "NUMBER"
    },
    {
      "name": "isAnonymous",
      "nativeType": "tinyint(1)",
      "type": "BOOLEAN"
    },
    {
      "name": "isMinor",
      "nativeType": "tinyint(1)",
      "type": "BOOLEAN"
    },
    {
      "name": "isNew",
      "nativeType": "tinyint(1)",
      "type": "BOOLEAN"
    },
    {
      "name": "isRobot",
      "nativeType": "tinyint(1)",
      "type": "BOOLEAN"
    },
    {
      "name": "isUnpatrolled",
      "nativeType": "tinyint(1)",
      "type": "BOOLEAN"
    },
    {
      "name": "metroCode",
      "nativeType": "int(11)",
      "type": "NUMBER"
    },
    {
      "name": "namespace",
      "nativeType": "varchar(255)",
      "type": "STRING"
    },
    {
      "name": "page",
      "nativeType": "varchar(255)",
      "type": "STRING"
    },
    {
      "name": "regionIsoCode",
      "nativeType": "varchar(255)",
      "type": "STRING"
    },
    {
      "name": "regionName",
      "nativeType": "varchar(255)",
      "type": "STRING"
    },
    {
      "name": "user",
      "nativeType": "varchar(255)",
      "type": "STRING"
    },
    {
      "name": "count",
      "nativeType": "bigint(21)",
      "type": "NUMBER"
    },
    {
      "name": "added",
      "nativeType": "decimal(32,0)",
      "type": "NUMBER"
    },
    {
      "name": "deleted",
      "nativeType": "decimal(32,0)",
      "type": "NUMBER"
    },
    {
      "name": "delta",
      "nativeType": "decimal(32,0)",
      "type": "NUMBER"
    },
    {
      "name": "min_delta",
      "nativeType": "int(11)",
      "type": "NUMBER"
    },
    {
      "name": "max_delta",
      "nativeType": "int(11)",
      "type": "NUMBER"
    },
    {
      "name": "deltaByTen",
      "nativeType": "double",
      "type": "NUMBER"
    }
  ];

  let wikiDerivedAttributes = {
    pageInBrackets: "'[' ++ $page ++ ']'"
  };

  describe("source list", () => {
    it("does a source list", () => {
      return MySQLExternal.getSourceList(mySqlRequester)
        .then((sources) => {
          expect(sources).to.contain('wikipedia');
          expect(sources).to.contain('wikipedia_raw');
        })
    });

  });


  describe("defined attributes in datasource", () => {
    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'mysql',
          source: 'wikipedia',
          attributes: wikiAttributes,
          derivedAttributes: wikiDerivedAttributes
        }, mySqlRequester)
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
        )
        .apply(
          'PagesHaving',
          $("wiki").split("$page", 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .filter($('Count').lessThan(30))
            .limit(3)
        );

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
                    "Count": 29,
                    "Page": "User:King Lui"
                  },
                  {
                    "Count": 29,
                    "Page": "The Visit (2015 film)"
                  },
                  {
                    "Count": 29,
                    "Page": "Stargate production discography"
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

    it("works with boolean GROUP BYs", () => {
      let ex = $("wiki").split($("channel").is("en"), 'ChannelIsEn')
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Count', 'descending');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal({
            "attributes": [
              {
                "name": "ChannelIsEn",
                "type": "BOOLEAN"
              },
              {
                "name": "Count",
                "type": "NUMBER"
              }
            ],
            "data": [
              {
                "ChannelIsEn": false,
                "Count": 277732
              },
              {
                "ChannelIsEn": true,
                "Count": 114711
              }
            ],
            "keys": [
              "ChannelIsEn"
            ]
          });
        });
    });

    it("works with multi-dimensional GROUP BYs", () => {
      let ex = $('wiki')
        .filter($("channel").isnt("en"))
        .split({
          'Channel': "$channel",
          'TimeByHour': '$__time.timeBucket(PT1H)'
        })
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Count', 'descending')
        .limit(4);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal({
            "attributes": [
              {
                "name": "Channel",
                "type": "STRING"
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
                "Count": 12443,
                "TimeByHour": {
                  "end": new Date('2015-09-12T07:00:00.000Z'),
                  "start": new Date('2015-09-12T06:00:00.000Z')
                }
              },
              {
                "Channel": "vi",
                "Count": 11833,
                "TimeByHour": {
                  "end": new Date('2015-09-12T08:00:00.000Z'),
                  "start": new Date('2015-09-12T07:00:00.000Z')
                }
              },
              {
                "Channel": "vi",
                "Count": 6411,
                "TimeByHour": {
                  "end": new Date('2015-09-12T18:00:00.000Z'),
                  "start": new Date('2015-09-12T17:00:00.000Z')
                }
              },
              {
                "Channel": "vi",
                "Count": 4943,
                "TimeByHour": {
                  "end": new Date('2015-09-12T16:00:00.000Z'),
                  "start": new Date('2015-09-12T15:00:00.000Z')
                }
              }
            ],
            "keys": [
              "Channel",
              "TimeByHour"
            ]
          });
        });
    });

    it("fallback doesn't happen if not null", () => {
      let ex = ply()
        .apply('added', $('wiki').sum($('added')).fallback(2));

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "added": 97393743
            }
          ]);
        });
    });

    it("works with simple raw mode", () => {
      let ex = $('wiki')
        .filter('$cityName == "El Paso"')
        .select('regionName', 'added', 'page');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal({
            "attributes": [
              {
                "name": "regionName",
                "type": "STRING"
              },
              {
                "name": "added",
                "type": "NUMBER"
              },
              {
                "name": "page",
                "type": "STRING"
              }
            ],
            "data": [
              {
                "added": 0,
                "page": "Clint High School",
                "regionName": "Texas"
              },
              {
                "added": 0,
                "page": "Reggie Williams (linebacker)",
                "regionName": "Texas"
              }
            ]
          });
        });
    });

    it("works with complex raw mode", () => {
      let ex = $('wiki')
        .filter('$cityName == "El Paso"')
        .apply('regionNameLOL', '$regionName.concat(LOL)')
        .apply('addedPlusOne', '$added + 1')
        .select('regionNameLOL', 'addedPlusOne', 'pageInBrackets');

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal({
            "attributes": [
              {
                "name": "regionNameLOL",
                "type": "STRING"
              },
              {
                "name": "addedPlusOne",
                "type": "NUMBER"
              },
              {
                "name": "pageInBrackets",
                "type": "STRING"
              }
            ],
            "data": [
              {
                "addedPlusOne": 1,
                "pageInBrackets": "[Clint High School]",
                "regionNameLOL": "TexasLOL"
              },
              {
                "addedPlusOne": 1,
                "pageInBrackets": "[Reggie Williams (linebacker)]",
                "regionNameLOL": "TexasLOL"
              }
            ]
          });
        });
    });

    it("fallback happens if null", () => {
      let ex = ply()
        .apply("wiki", $('wiki').filter($("page").is('Rallicula')))
        .apply('MetroCode', $('wiki').sum($('metroCode')).fallback(0));

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "MetroCode": 0
            }
          ]);
        });
    });

    it("power of and abs", () => {
      let ex = ply()
        .apply("wiki", $('wiki').filter($("page").is('Kosowo')))
        .apply('Delta', $('wiki').min($('delta')))
        .apply('AbsDelta', $('wiki').min($('delta')).absolute())
        .apply('SquareDelta', $('wiki').sum($('delta')).power(2));

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "AbsDelta": 2,
              "Delta": -2,
              "SquareDelta": 4
            }
          ]);
        });
    });


    it("works string range", () => {
      let ex = $('wiki')
        .filter($('cityName').greaterThan('Kab').and($('cityName').lessThan('Kar')))
        .split('$cityName', 'City')
        .limit(5);
      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "City": "Kadelburg"
            },
            {
              "City": "Kaduwela"
            },
            {
              "City": "Kagoshima"
            },
            {
              "City": "Kailua"
            },
            {
              "City": "Kainan"
            }
          ]);
        });
    });

    it("works string range", () => {
      let ex = $('wiki')
        .filter($('cityName').lessThan('P'))
        .filter('$comment < "zebra"')
        .split('$cityName', 'City')
        .limit(5);
      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "City": "'Ewa Beach"
            },
            {
              "City": "A Coruña"
            },
            {
              "City": "Aachen"
            },
            {
              "City": "Aalborg"
            },
            {
              "City": "Aarhus"
            }
          ]);
        });
    });

  });

  describe("incorrect page", () => {
    let wikiUserCharAsNumber = External.fromJS({
      engine: 'mysql',
      source: 'wikipedia',
      timeAttribute: 'time',
      allowEternity: true,
      attributes: [
        { name: 'time', type: 'TIME' },
        { name: "comment", type: "STRING" },
        { name: 'page', type: 'NUMBER' }, // This is incorrect
        { name: 'count', type: 'NUMBER', unsplitable: true }
      ]
    }, mySqlRequester);

    // Todo: invalid number casts return 0 in mysql. Also, something is happening when page is defined as a number that results in numbers being passed into the cast to date
    it("works with bad casts", () => {
      let ex = $('wiki').split({ 'numberCast': '$comment.cast("NUMBER")', 'dateCast': '$page.cast("TIME")' })
        .apply('Count', '$wiki.sum($count)')
        .sort('$Count', 'descending')
        .limit(3);

      return ex.compute({ wiki: wikiUserCharAsNumber })
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "Count": 382569,
              "dateCast": new Date('1970-01-01T00:00:00.000Z'),
              "numberCast": 0
            },
            {
              "Count": 3347,
              "dateCast": new Date('1970-01-01T00:00:02.015Z'),
              "numberCast": 0
            },
            {
              "Count": 640,
              "dateCast": new Date('1970-01-01T00:00:00.000Z'),
              "numberCast": 1
            }
          ]);
        });
    });
  });


  describe("introspection", () => {
    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'mysql',
          source: 'wikipedia'
        }, mySqlRequester)
      }
    });

    it("introspects", () => {
      return External.fromJS({
        engine: 'mysql',
        source: 'wikipedia'
      }, mySqlRequester).introspect()
        .then((external) => {
          expect(external.version).to.equal(info.mySqlVersion);
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
