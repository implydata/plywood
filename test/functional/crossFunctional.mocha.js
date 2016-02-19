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
  { name: "commentLength", type: 'Number' },
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
  { name: 'deltaByTen', type: 'NUMBER' },
  { name: 'added', type: 'NUMBER' },
  { name: 'deleted', type: 'NUMBER' }
];

var druidExecutor = basicExecutorFactory({
  datasets: {
    wiki: External.fromJS({
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
      druidVersion: info.druidVersion,
      requester: druidRequester
    })
  }
});

var mysqlExecutor = basicExecutorFactory({
  datasets: {
    wiki: External.fromJS({
      engine: 'mysql',
      table: 'wikipedia',
      attributes,
      requester: mySqlRequester
    })
  }
});

var equalityTest = utils.makeEqualityTest({
  druid: druidExecutor,
  mysql: mysqlExecutor
});

describe("Cross Functional", function() {
  this.timeout(10000);

  describe("filters", () => {
    it('works with a simple filter', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($channel == en)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with == NULL filter', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName == null)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with != NULL filter', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName != null)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with contains filter', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName.contains("San"))')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));
  });

  describe("splits", () => {
    it('works with total', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki') // needed for now
        //.apply('RowCount', '$wiki.count()') // ToDo: make wikipedia data in MySQL rolled up
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with BOOLEAN split (native)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split('$isNew', 'IsNew')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it('works with BOOLEAN split (expression)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split('$channel == de', 'ChannelIsDE')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it('works with STRING split (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .limit(5)
    }));

    it('works with STRING split (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split('$channel', 'Channel')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(5)
    }));

    it('works with TIME split (timeBucket) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TimeByHour', 'ascending')
    }));

    it('works with TIME split (timeBucket) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works with TIME split (timePart) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("time").timePart('HOUR_OF_DAY', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$HourOfDay', 'ascending')
    }));

    it('works with TIME split (timePart) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("time").timePart('HOUR_OF_DAY', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works with secondary TIME split (timeBucket PT1M) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("sometimeLater").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TimeByHour', 'ascending')
    }));

    it('works with secondary TIME split (timeBucket PT1M) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("sometimeLater").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works with secondary TIME split (timePart) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("sometimeLater").timePart('MINUTE_OF_HOUR', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$HourOfDay', 'ascending')
    }));

    it('works with secondary TIME split (timePart) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("sometimeLater").timePart('MINUTE_OF_HOUR', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works with BOOLEAN multi-dim-split', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki')
        .split({
          'isNew': '$isNew',
          'isRobot': '$isRobot',
          'isUnpatrolled': '$isUnpatrolled',
          'ChannelIsDE': '$channel == de'
        })
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it('works with various multi-dimensional split', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki')
        .split({
          'Channel': "$channel",
          'TimeByHour': '$time.timeBucket(PT1H)',
          'IsNew': '$isNew',
          'ChannelIsDE': "$channel == 'de'"
        })
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(4)
    }));

  });

  describe("applies", () => {
    it('works with all sorts of applies', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split('$channel', 'Channel')
        .apply('TotalAdded', '$wiki.sum($added)')
        //.apply('AddedBYDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('TokyoAdded', '$wiki.filter($cityName == Tokyo).sum($added)')
        .sort('$Channel', 'descending')
        .limit(50)
    }));

    it('works with max time (total)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki') // needed for now
        .apply('MaxTime', '$wiki.max($time)')
        .apply('MinTime', '$wiki.min($time)')
    }));

  });
});
