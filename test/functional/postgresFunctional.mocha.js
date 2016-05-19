var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var { postgresRequesterFactory } = require('plywood-postgres-requester');

var plywood = require('../../build/plywood');
var { External, PostgresExternal, TimeRange, $, ply, basicExecutorFactory, helper } = plywood;

var info = require('../info');

var postgresRequester = postgresRequesterFactory({
  host: info.postgresHost,
  database: info.postgresDatabase,
  user: info.postgresUser,
  password: info.postgresPassword
});

// postgresRequester = helper.verboseRequesterFactory({
//  requester: postgresRequester
// });

describe("Postgres Functional", function() {
  this.timeout(10000);

  var wikiAttributes = [
    { "name": "time", "type": "TIME" },
    { "name": "sometimeLater", "type": "TIME" },
    { "name": "channel", "type": "STRING" },
    { "name": "cityName", "type": "STRING" },
    { "name": "comment", "type": "STRING" },
    { "name": "commentLength", "type": "NUMBER" },
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
    { "name": "count", "type": "NUMBER" },
    { "name": "added", "type": "NUMBER" },
    { "name": "deleted", "type": "NUMBER" },
    { "name": "delta", "type": "NUMBER" },
    { "name": "min_delta", "type": "NUMBER" },
    { "name": "max_delta", "type": "NUMBER" },
    { "name": "deltaByTen", "type": "NUMBER" }
  ];

  var wikiDerivedAttributes = {
    pageInBrackets: "'[' ++ $page ++ ']'"
  };

  describe("source list", () => {
    it("does a source list", (testComplete) => {
      PostgresExternal.getSourceList(postgresRequester)
        .then((sources) => {
          expect(sources).to.deep.equal(['wikipedia', 'wikipedia_raw']);
          testComplete();
        })
        .done()
    });

  });


  describe("defined attributes in datasource", () => {
    var basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'postgres',
          table: 'wikipedia',
          attributes: wikiAttributes,
          derivedAttributes: wikiDerivedAttributes
        }, postgresRequester)
      }
    });

    it("works in advanced case", (testComplete) => {
      var ex = ply()
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

      basicExecutor(ex)
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
          testComplete();
        })
        .done();
    });

    it("works with boolean GROUP BYs", (testComplete) => {
      var ex = $("wiki").split($("channel").is("en"), 'ChannelIsEn')
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Count', 'descending');

      basicExecutor(ex)
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
          testComplete();
        })
        .done();
    });

  });

  describe("introspection", () => {
    var basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'postgres',
          table: 'wikipedia'
        }, postgresRequester)
      }
    });

    it("introspects", (testComplete) => {
      External.fromJS({
        engine: 'postgres',
        table: 'wikipedia'
      }, postgresRequester).introspect()
        .then((external) => {
          expect(external.toJS().attributes).to.deep.equal(wikiAttributes);
          testComplete();
        })
        .done();
    });

    it("works with introspection", (testComplete) => {
      var ex = ply()
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

      basicExecutor(ex)
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
          testComplete();
        })
        .done();
    });
  });

});
