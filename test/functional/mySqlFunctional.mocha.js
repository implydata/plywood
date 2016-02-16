var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var { mySqlRequesterFactory } = require('plywood-mysql-requester');

var plywood = require('../../build/plywood');
var { Expression, External, TimeRange, $, ply, basicExecutorFactory } = plywood;

var info = require('../info');

var mySqlRequester = mySqlRequesterFactory({
  host: info.mySqlHost,
  database: info.mySqlDatabase,
  user: info.mySqlUser,
  password: info.mySqlPassword
});

const DEFAULT_TABLE = "wikipedia";

describe("MySQL Functional", function() {
  this.timeout(10000);

  describe("defined attributes in datasource", () => {
    var basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'mysql',
          table: DEFAULT_TABLE,
          attributes: [
            { name: 'time', type: 'TIME' },
            { name: 'language', type: 'STRING' },
            { name: 'page', type: 'STRING' },
            { name: 'namespace', type: 'STRING' },
            { name: 'added', type: 'NUMBER' },
            { name: 'regionName', type: 'STRING' },
            { name: 'countryName', type: 'STRING' },
            { name: 'channel', type: 'STRING' },
            { name: 'added', type: 'NUMBER' }
          ],
          requester: mySqlRequester
        })
      }
    });

    it("works in advanced case", (testComplete) => {
      var ex = ply()
        .apply("wiki", $('wiki').filter($("channel").is('en')))
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply('Namespaces',
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
      //      .apply(
      //        'PagesHaving',
      //        $("wiki").split("$page", 'Page')
      //          .apply('Count', '$wiki.sum($count)')
      //          .sort('$Count', 'descending')
      //          .filter($('Count').lessThan(30))
      //          .limit(100)
      //      )

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
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
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("works multi-dimensional GROUP BYs", (testComplete) => {
      var ex = ply()
        .apply("wiki", $('wiki').filter($("channel").isnt("en")))
        .apply(
          'Cuts',
          $("wiki").split({
              'Channel': "$channel",
              'TimeByHour': '$time.timeBucket(PT1H)'
            })
            .apply('Count', $('wiki').count())
            .sort('$Count', 'descending')
            .limit(4)
        );

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "Cuts": [
                {
                  "Channel": "vi",
                  "Count": 12443,
                  "TimeByHour": {
                    "end": new Date('2015-09-12T07:00:00.000Z'),
                    "start": new Date('2015-09-12T06:00:00.000Z'),
                    "type": "TIME_RANGE"
                  }
                },
                {
                  "Channel": "vi",
                  "Count": 11833,
                  "TimeByHour": {
                    "end": new Date('2015-09-12T08:00:00.000Z'),
                    "start": new Date('2015-09-12T07:00:00.000Z'),
                    "type": "TIME_RANGE"
                  }
                },
                {
                  "Channel": "vi",
                  "Count": 6411,
                  "TimeByHour": {
                    "end": new Date('2015-09-12T18:00:00.000Z'),
                    "start": new Date('2015-09-12T17:00:00.000Z'),
                    "type": "TIME_RANGE"
                  }
                },
                {
                  "Channel": "vi",
                  "Count": 4943,
                  "TimeByHour": {
                    "end": new Date('2015-09-12T16:00:00.000Z'),
                    "start": new Date('2015-09-12T15:00:00.000Z'),
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
  });

  describe("introspection", () => {
    var basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'mysql',
          table: DEFAULT_TABLE,
          requester: mySqlRequester
        })
      }
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

  describe("fallback", () => {
    var basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS({
          engine: 'mysql',
          table: DEFAULT_TABLE,
          requester: mySqlRequester
        })
      }
    });


    it("fallback doesn't happen if not null", (testComplete) => {
      var ex = ply()
        .apply("wiki", $('wiki'))
        .apply('added', $('wiki').average($('added')).fallback(2));

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "added": 248.173
            }
          ]);
          testComplete();
        })
        .done();
    });

    it("fallback happens if null", (testComplete) => {
      var ex = ply()
        .apply("wiki", $('wiki').filter($("page").is('Rallicula')))
        .apply('MetroCode', $('wiki').sum($('metroCode')).fallback(0));

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
          {
            "MetroCode": 0
          }
        ]);
        testComplete();
        })
        .done();
    });

    it("power of and abs", (testComplete) => {
      var ex = ply()
        .apply("wiki", $('wiki').filter($("page").is('Kosowo')))
        .apply('Delta', $('wiki').min($('delta')))
        .apply('AbsDelta', $('wiki').min($('delta')).absolute())
        .apply('SquareDelta', $('wiki').sum($('delta')).power(2));

      basicExecutor(ex)
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            {
              "AbsDelta": 2,
              "Delta": -2,
              "SquareDelta": 4
            }
          ]);
          testComplete();
        })
        .done();
    });
  });
});
