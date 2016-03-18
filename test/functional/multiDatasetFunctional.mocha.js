var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var { druidRequesterFactory } = require('plywood-druid-requester');
var { mySqlRequesterFactory } = require('plywood-mysql-requester');

var plywood = require('../../build/plywood');
var { External, TimeRange, $, ply, basicExecutorFactory, helper } = plywood;

var utils = require('../utils');
var info = require('../info');

var druidRequester = druidRequesterFactory({
  host: info.druidHost
});

var mySqlRequester = mySqlRequesterFactory({
  host: info.mySqlHost,
  database: info.mySqlDatabase,
  user: info.mySqlUser,
  password: info.mySqlPassword
});

//druidRequester = helper.verboseRequesterFactory({
//  requester: druidRequester
//});
//mySqlRequester = helper.verboseRequesterFactory({
//  requester: mySqlRequester
//});

var attributes = [
  { name: 'time', type: 'TIME' },
  { name: 'sometimeLater', type: 'TIME' },
  { name: "channel", type: 'STRING' },
  { name: "cityName", type: 'STRING' },
  { name: "comment", type: 'STRING' },
  { name: "commentLength", type: 'NUMBER' },
  { name: "countryIsoCode", type: 'STRING' },
  { name: "countryName", type: 'STRING' },
  { name: "isAnonymous", type: 'BOOLEAN' },
  { name: "isMinor", type: 'BOOLEAN' },
  { name: "isNew", type: 'BOOLEAN' },
  { name: "isRobot", type: 'BOOLEAN' },
  { name: "isUnpatrolled", type: 'BOOLEAN' },
  { name: "metroCode", type: 'STRING' },
  { name: "namespace", type: 'STRING' },
  { name: "page", type: 'STRING' },
  { name: "regionIsoCode", type: 'STRING' },
  { name: "regionName", type: 'STRING' },
  { name: "user", type: 'STRING' },
  //{ name: "userChars", type: 'SET/STRING' },
  { name: 'count', type: 'NUMBER' },
  { name: 'delta', type: 'NUMBER' },
  { name: 'min_delta', type: 'NUMBER' },
  { name: 'max_delta', type: 'NUMBER' },
  { name: 'deltaByTen', type: 'NUMBER' },
  { name: 'added', type: 'NUMBER' },
  { name: 'deleted', type: 'NUMBER' }
];

var mixedExecutor = basicExecutorFactory({
  datasets: {
    wiki_druid: External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      context: {
        timeout: 10000
      },
      attributes,
      filter: $('time').in(TimeRange.fromJS({
        start: new Date("2015-09-12T00:00:00Z"),
        end: new Date("2015-09-13T00:00:00Z")
      })),
      version: info.druidVersion
    }, druidRequester),
    wiki_mysql: External.fromJS({
      engine: 'mysql',
      table: 'wikipedia',
      attributes
    }, mySqlRequester)
  }
});

describe("Multi Dataset Functional", function() {
  this.timeout(10000);

  // ToDo: make this work
  it.skip("works in basic case", (testComplete) => {
    var ex = ply()
      .apply("wiki_druid", $('wiki_druid').filter($("channel").is('en')))
      .apply('TotalAddedDruid', '$wiki_druid.sum($added)')
      .apply("wiki_mysql", $('wiki_mysql').filter($("channel").is('en')))
      .apply('TotalAddedMySQL', '$wiki_mysql.sum($added)');

    mixedExecutor(ex)
      .then((result) => {
        expect(result.toJS()).to.deep.equal([

        ]);
        testComplete();
      })
      .done();
  });

  it("mixed split case", (testComplete) => {
    var ex = $('wiki_mysql').split("$channel", "Channel")
      .apply('TotalAddedMySQL', '$wiki_mysql.sum($added)')
      .sort('$TotalAddedMySQL', 'descending')
      .limit(3)
      .apply('wiki_druid', $('wiki_druid').filter("$channel == $Channel"))
      .apply(
        'Namespaces',
        $('wiki_druid').split('$namespace', "Namespace")
          .apply('TotalAddedDruid', '$wiki_druid.sum($added)')
          .sort('$TotalAddedDruid', 'descending')
          .limit(3)
      );


    mixedExecutor(ex)
      .then((result) => {
        expect(result.toJS()).to.deep.equal([
          {
            "Channel": "en",
            "Namespaces": [
              {
                "Namespace": "Main",
                "TotalAddedDruid": 11594002
              },
              {
                "Namespace": "User talk",
                "TotalAddedDruid": 9210976
              },
              {
                "Namespace": "Wikipedia",
                "TotalAddedDruid": 4720291
              }
            ],
            "TotalAddedMySQL": 32553107
          },
          {
            "Channel": "it",
            "Namespaces": [
              {
                "Namespace": "Discussioni utente",
                "TotalAddedDruid": 5938398
              },
              {
                "Namespace": "Main",
                "TotalAddedDruid": 1545491
              },
              {
                "Namespace": "Utente",
                "TotalAddedDruid": 97907
              }
            ],
            "TotalAddedMySQL": 7852924
          },
          {
            "Channel": "fr",
            "Namespaces": [
              {
                "Namespace": "Main",
                "TotalAddedDruid": 3830175
              },
              {
                "Namespace": "Discussion utilisateur",
                "TotalAddedDruid": 1381838
              },
              {
                "Namespace": "Projet",
                "TotalAddedDruid": 639063
              }
            ],
            "TotalAddedMySQL": 7050247
          }
        ]);
        testComplete();
      })
      .done();
  });

});
