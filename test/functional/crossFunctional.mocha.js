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
  { name: "metroCode", type: 'NUMBER' },
  { name: "namespace", type: 'STRING' },
  { name: "page", type: 'STRING' },
  { name: "regionIsoCode", type: 'STRING' },
  { name: "regionName", type: 'STRING' },
  { name: "user", type: 'STRING' },

  { name: 'count', type: 'NUMBER', unsplitable: true },
  { name: 'delta', type: 'NUMBER', unsplitable: true },
  { name: 'min_delta', type: 'NUMBER', unsplitable: true },
  { name: 'max_delta', type: 'NUMBER', unsplitable: true },
  { name: 'deltaByTen', type: 'NUMBER', unsplitable: true },
  { name: 'added', type: 'NUMBER', unsplitable: true },
  { name: 'deleted', type: 'NUMBER', unsplitable: true }
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
      allowSelectQueries: true,
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
    it('works with == filter', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName == "San Francisco")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it.skip('works with != filter', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName != "San Francisco")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with == NULL filter', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName == null)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with != NULL filter', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName != null)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .in() filter [dimension without NULLs]', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($channel.in(["en", "simple"]))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .in().not() filter [dimension without NULLs]', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($channel.in(["en", "simple"]).not())')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it.skip('works with .in().not() filter [dimension with NULLs]', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName.in(["Moscow", "Tel Aviv", "London", "San Francisco"]).not())')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .overlap() filter [dimension without NULLs]', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($channel.overlap(["en", "simple"]))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with contains filter', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName.contains("San"))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with match filter', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression:  ply()
        .apply('wiki', '$wiki.filter($cityName.match("^S[ab]n .{3,6}$"))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with lessThan filter', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($commentLength < 50)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with numeric range', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter(20 <= $commentLength and $commentLength < 50)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));
  });


  describe("splits", () => {
    it('works with total', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki') // needed for now
        .apply('RowCount', '$wiki.count()')
        .apply('EditsCount', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with BOOLEAN split (native)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split('$isNew', 'IsNew')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it('works with BOOLEAN split (expression, defined)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split('$channel == de', 'ChannelIsDE')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it.skip('works with BOOLEAN split (expression, nullable)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split('$cityName == "San Francisco"', 'CityIsSF')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it('works with STRING split (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .limit(20)
    }));

    it('works with STRING split (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split('$channel', 'Channel')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with STRING split (nullable dimension)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split('$cityName', 'City')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with NUMBER split (numberBucket) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("commentLength").numberBucket(10), 'CommentLength10')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$CommentLength10', 'ascending')
        .limit(20) // To force a topN (for now)
    }));

    it('works with NUMBER split (numberBucket) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("commentLength").numberBucket(10), 'CommentLength10')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with NUMBER split (expression^2) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split("$commentLength^2", 'CommentLengthSq')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$CommentLengthSq', 'descending')
        .limit(20) // To force a topN (for now)
    }));

    it('works with TIME split (timeBucket) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TimeByHour', 'ascending')
    }));

    it('works with TIME split (timeBucket) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works with TIME split (timePart) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("time").timePart('HOUR_OF_DAY', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$HourOfDay', 'ascending')
    }));

    it('works with TIME split (timePart) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("time").timePart('HOUR_OF_DAY', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works with secondary TIME split (timeBucket PT1M) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("sometimeLater").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TimeByHour', 'ascending')
    }));

    it('works with secondary TIME split (timeBucket PT1M) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("sometimeLater").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works with secondary TIME split (timePart) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("sometimeLater").timePart('MINUTE_OF_HOUR', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$HourOfDay', 'ascending')
    }));

    it('works with secondary TIME split (timePart) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split($("sometimeLater").timePart('MINUTE_OF_HOUR', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalEdits', '$wiki.sum($count)')
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
        .apply('TotalEdits', '$wiki.sum($count)')
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
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(4)
    }));

  });


  describe("applies", () => {
    it('works with all sorts of applies', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split('$channel', 'Channel')
        .apply('RowCount', '$wiki.count()')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply('AddedBYDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('TokyoAdded', '$wiki.filter($cityName == Tokyo).sum($added)')
        .apply('To*Added', '$wiki.filter($cityName.contains("to")).sum($added)')
        .apply('MinDelta', '$wiki.min($min_delta)')
        .apply('MaxDelta', '$wiki.max($max_delta)')
        .apply('AbsDeltaX2', '$wiki.sum($delta.absolute()) * 2')
        .apply('SumAdded^0.6', '$wiki.sum($added) ^ 0.6')
        .apply('Sum(Added^0.6)', '$wiki.sum($added ^ 0.6)') // This is meaningless since added is aggregated
        .sort('$Channel', 'descending')
        .limit(50)
    }));

    it.skip('works with (aprox) countDistinct', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki') // needed for now
        .apply('DistPagesWithinLimits', '($wiki.countDistinct($page) - 279893).absolute() < 10')
    }));

    it('works with max time (total)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki') // needed for now
        .apply('MinTime', '$wiki.min($time)')
        .apply('MaxTime', '$wiki.max($time)')
    }));

  });


  describe("having filter", () => {
    it('works with greaterThan', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .apply('TotalEdits', '$wiki.sum($count)')
        .filter('$TotalEdits > 5000')
        .limit(20)
    }));

    it('works with lessThan', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .apply('TotalEdits', '$wiki.sum($count)')
        .filter('$TotalEdits < 5000')
        .limit(20)
    }));
  });

  describe("select", () => {
    it('works with basic select', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki').filter('$cityName == "El Paso"')
    }));

    it.skip('works with extra columns', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki')
        .filter('$cityName == "El Paso"')
        .apply('AddedX2', '$added * 2')
    }));

  });

});
