var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var { druidRequesterFactory } = require('plywood-druid-requester');
var { mySqlRequesterFactory } = require('plywood-mysql-requester');
var { postgresRequesterFactory } = require('plywood-postgres-requester');

var plywood = require('../../build/plywood');
var { External, TimeRange, $, ply, r, basicExecutorFactory, helper } = plywood;

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

var postgresRequester = postgresRequesterFactory({
  host: info.postgresHost,
  database: info.postgresDatabase,
  user: info.postgresUser,
  password: info.postgresPassword
});

// druidRequester = helper.verboseRequesterFactory({
//   requester: druidRequester
// });
// mySqlRequester = helper.verboseRequesterFactory({
//   requester: mySqlRequester
// });
// postgresRequester = helper.verboseRequesterFactory({
//   requester: postgresRequester
// });

var attributes = [
  { name: 'time', type: 'TIME' },
  { name: 'sometimeLater', type: 'TIME' },
  { name: "channel", type: 'STRING' },
  { name: "cityName", type: 'STRING' },
  { name: "comment", type: 'STRING' },
  { name: "commentLength", type: 'NUMBER' },
  { name: "countryIsoCode", type: 'STRING' },
  { name: "countryName", type: 'STRING' },
  { name: "deltaBucket100", type: 'NUMBER' },
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

var derivedAttributes = {
  pageInBrackets: "'[' ++ $page:STRING ++ ']'" // ToDo: remove :STRING
};

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
      derivedAttributes,
      version: info.druidVersion,
      allowSelectQueries: true,
      allowEternity: true
    }, druidRequester)
  }
});

var druidLegacyExecutor = basicExecutorFactory({
  datasets: {
    wiki: External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      context: {
        timeout: 10001 // Put a different timeout here so we can tell queries apart from non-legacy druid
      },
      attributes,
      derivedAttributes,
      version: '0.8.3',
      allowSelectQueries: true,
      allowEternity: true
    }, druidRequester)
  }
});

var mysqlExecutor = basicExecutorFactory({
  datasets: {
    wiki: External.fromJS({
      engine: 'mysql',
      table: 'wikipedia',
      attributes,
      derivedAttributes
    }, mySqlRequester)
  }
});

var postgresExecutor = basicExecutorFactory({
  datasets: {
    wiki: External.fromJS({
      engine: 'postgres',
      table: 'wikipedia',
      attributes,
      derivedAttributes
    }, postgresRequester)
  }
});

var equalityTest = utils.makeEqualityTest({
  druid: druidExecutor,
  druidLegacy: druidLegacyExecutor,
  mysql: mysqlExecutor,
  postgres: postgresExecutor
});

describe("Cross Functional", function() {
  this.timeout(10000);

  describe("filters", () => {
    it('works with empty filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName == "this city does not exist")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with ref filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName == "San Francisco")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with == filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($isAnonymous)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with != filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName != "San Francisco")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with == NULL filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName == null)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with != NULL filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName != null)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .contains() and .is() filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName.contains("San") and $cityName == "San Francisco")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .in() filter [dimension without NULLs]', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($channel.in(["en", "simple"]))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .in().not() filter [dimension without NULLs]', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($channel.in(["en", "simple"]).not())')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it.skip('works with .in().not() filter [dimension with NULLs]', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName.in(["Moscow", "Tel Aviv", "London", "San Francisco"]).not())')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .concat().concat().contains() filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter(("[" ++ $cityName ++ "]").contains("[san", "ignoreCase"))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .overlap() filter [dimension without NULLs]', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($channel.overlap(["en", "simple"]))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with OR filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($channel == "en" or $cityName == "Tel Aviv")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .contains(,normal)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName.contains("San", normal))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .contains(,ignoreCase)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName.contains("San", ignoreCase))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .match()', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression:  ply()
        .apply('wiki', '$wiki.filter($cityName.match("^S[ab]n .{3,6}$"))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with primary time filter (single range)', equalityTest({
      executorNames: ['druid', 'druidLegacy', 'mysql', 'postgres'],
      expression:  ply()
        .apply('wiki', $('wiki').filter($('time').in(new Date("2015-09-12T01:00:00Z"), new Date("2015-09-12T02:30:00Z"))))
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with alt time filter (single range)', equalityTest({
      executorNames: ['druid', 'druidLegacy', 'mysql', 'postgres'],
      expression:  ply()
        .apply('wiki', $('wiki').filter($('sometimeLater').in(new Date("2016-09-12T01:00:00Z"), new Date("2016-09-12T02:30:00Z"))))
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with primary time filter (multi range)', equalityTest({
      executorNames: ['druid', 'druidLegacy', 'mysql', 'postgres'],
      expression:  ply()
        .apply('wiki', $('wiki').filter(
          $('time').in(new Date("2015-09-12T01:00:00Z"), new Date("2015-09-12T02:30:00Z")).or(
            $('time').in(new Date("2015-09-12T03:00:00Z"), new Date("2015-09-12T04:30:00Z")),
            $('time').in(new Date("2015-09-12T05:00:00Z"), new Date("2015-09-12T06:30:00Z")))
        ))
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with alt time filter (multi range)', equalityTest({
      executorNames: ['druid', 'druidLegacy', 'mysql', 'postgres'],
      expression:  ply()
        .apply('wiki', $('wiki').filter(
          $('sometimeLater').in(new Date("2016-09-12T01:00:00Z"), new Date("2016-09-12T02:30:00Z")).or(
            $('sometimeLater').in(new Date("2016-09-12T03:00:00Z"), new Date("2016-09-12T04:30:00Z")),
            $('sometimeLater').in(new Date("2016-09-12T05:00:00Z"), new Date("2016-09-12T06:30:00Z")))
        ))
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it.skip('works with .timePart().in()', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression:  ply()
        .apply('wiki', $('wiki').filter($('time').timePart('HOUR_OF_DAY').in([3, 7])))
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .timePart().in() [alt time column]', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression:  ply()
        .apply('wiki', $('wiki').filter($('sometimeLater').timePart('HOUR_OF_DAY').in([3, 7])))
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .lessThan()', equalityTest({
      executorNames: ['druid', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($commentLength < 50)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .lessThanOrEqual()', equalityTest({
      executorNames: ['druid', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($commentLength <= 50)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with numeric range, bounds: ()', equalityTest({
      executorNames: ['druid', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter(20 < $commentLength and $commentLength < 50)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with numeric range, bounds: [)', equalityTest({
      executorNames: ['druid', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter(20 <= $commentLength and $commentLength < 50)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with numeric range, bounds: (]', equalityTest({
      executorNames: ['druid', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter(20 < $commentLength and $commentLength <= 50)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with numeric range, bounds: []', equalityTest({
      executorNames: ['druid', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter(20 <= $commentLength and $commentLength <= 50)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with negative number in range', equalityTest({
      executorNames: ['druid', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter(-300 <= $deltaBucket100 and $deltaBucket100 <= 300)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with static derived attribute .is()', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($pageInBrackets == "[Deaths_in_2015]")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with dynamic derived attribute .is()', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.apply(city3, $cityName.substr(0, 3)).filter($city3 == "San")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

  });


  describe("splits (single)", () => {
    it('works with empty filter split', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki')
        .filter('$cityName == "this city does not exist"')
        .split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .limit(20)
    }));

    it.skip('works with constant split', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki')
        .split('blah', 'Constant')
        .apply('TotalEdits', '$wiki.sum($count)')
    }));

    it('works with BOOLEAN split (native)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split('$isNew', 'IsNew')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it('works with BOOLEAN split (expression, defined)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split('$channel == de', 'ChannelIsDE')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it('works with BOOLEAN split (expression, nullable)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split('$cityName == "San Francisco"', 'CityIsSF')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it('works with STRING split (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .limit(20)
    }));

    it('works with STRING split (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split('$channel', 'Channel')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with STRING split (nullable dimension)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split('$cityName', 'City')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with dynamic derived column', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').apply('city3', '$cityName.substr(0, 3)').split('$city3', 'City3')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with STRING split .fallback() no match', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split('$cityName.fallback("NoCity")', 'CityFallback')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with STRING split .fallback() with match', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split('$cityName.fallback("Bucharest")', 'CityFallback')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with NUMBER split (numberBucket) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("commentLength").numberBucket(10), 'CommentLength10')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$CommentLength10', 'ascending')
        .limit(20) // To force a topN (for now)
    }));

    it('works with NUMBER split (numberBucket) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("commentLength").numberBucket(10), 'CommentLength10')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with NUMBER split (with negatives) (sort on split)', equalityTest({
      executorNames: ['mysql', 'postgres'], // 'druid',
      expression: $('wiki').split("$deltaBucket100", 'DeltaBucket100')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$DeltaBucket100', 'ascending')
        .limit(20)
    }));

    it('works with NUMBER split (-expression) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split("1000 - $commentLength", 'MinusCommentLength')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$MinusCommentLength', 'descending')
        .limit(20) // To force a topN (for now)
    }));

    it('works with NUMBER split (1 + expression) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split("1 + $commentLength", 'OnePlusCommentLength')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$OnePlusCommentLength', 'descending')
        .limit(20) // To force a topN (for now)
    }));

    it('works with NUMBER split (expression / 10) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql'], //'postgres' # ToDo: postgres truncates results
      expression: $('wiki').split("$commentLength / 10", 'CommentLengthDiv')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalEdits', 'descending')
        .limit(20) // To force a topN (for now)
    }));

    it('works with NUMBER split (expression / 10).numberBucket (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split("($commentLength / 10).numberBucket(2, 0)", 'CommentLengthDivBucket')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalEdits', 'descending')
        .limit(20) // To force a topN (for now)
    }));

    it('works with NUMBER split (expression^2) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split("$commentLength^2", 'CommentLengthSq')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$CommentLengthSq', 'descending')
        .limit(20) // To force a topN (for now)
    }));

    it('works with TIME split (raw) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').filter('$time < "2015-09-12T02Z"').split($("time"), 'TimeRaw')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimeRaw', 'ascending')
        .limit(20)
    }));

    it('works with TIME split (timeFloor) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("time").timeFloor('PT1H', 'Etc/UTC'), 'TimeFloorHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TimeFloorHour', 'ascending')
        .limit(20)
    }));

    it('works with TIME split (timeBucket) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TimeByHour', 'ascending')
        .limit(20)
    }));

    it('works with TIME split (timeBucket, Kathmandu) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'], // , 'postgres'
      expression: $('wiki').split($("time").timeBucket('PT1H', 'Asia/Kathmandu'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TimeByHour', 'ascending')
        .limit(20)
    }));

    it('works with TIME split (timeBucket) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works with TIME split (timePart) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("time").timePart('HOUR_OF_DAY', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$HourOfDay', 'ascending')
    }));

    it('works with TIME split (timePart(YEAR)) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("time").timePart('YEAR'), 'TimePart')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimePart', 'ascending')
    }));

    it('works with TIME split (timePart(DAY_OF_YEAR)) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("time").timePart('DAY_OF_YEAR'), 'TimePart')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimePart', 'ascending')
    }));

    it('works with TIME split (timePart(DAY_OF_MONTH)) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("time").timePart('DAY_OF_MONTH'), 'TimePart')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimePart', 'ascending')
    }));

    it('works with TIME split (timePart(DAY_OF_WEEK)) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("time").timePart('DAY_OF_WEEK'), 'TimePart')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimePart', 'ascending')
    }));

    it('works with TIME split (timePart(MINUTE_OF_DAY)) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("time").timePart('MINUTE_OF_DAY'), 'TimePart')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimePart', 'ascending')
        .limit(200)
    }));

    it('works with TIME split (timePart) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("time").timePart('HOUR_OF_DAY', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works with secondary TIME split (raw) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').filter('$time < "2015-09-12T02Z"').split($("sometimeLater"), 'TimeRaw')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimeRaw', 'ascending')
        .limit(20)
    }));

    it('works with secondary TIME split (timeBucket PT1H) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("sometimeLater").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TimeByHour', 'ascending')
    }));

    it('works with secondary TIME split (timeBucket PT1H) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("sometimeLater").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it.skip('works with secondary TIME split (timeBucket PT3H) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("sometimeLater").timeBucket('PT3H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TimeByHour', 'ascending')
        .limit(10)
    }));

    it('works with secondary TIME split (timePart) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("sometimeLater").timePart('MINUTE_OF_HOUR', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$HourOfDay', 'ascending')
    }));

    it('works with secondary TIME split (timePart) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("sometimeLater").timePart('MINUTE_OF_HOUR', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works with secondary TIME split (timePart, TZ) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split($("sometimeLater").timePart('DAY_OF_YEAR', 'America/New_York'), 'HourOfDay')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

  });

  describe("splits (single, multi-dim)", () => {
    it('works with BOOLEAN multi-dim-split', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
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
      executorNames: ['druid', 'mysql', 'postgres'],
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

    it('works with timeFloor multi-dimensional split', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki')
        .split({
          'Channel': "$channel",
          'TimeByHour': '$time.timeFloor(PT1M)'
        })
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(4)
    }));

  });


  describe("splits (nested)", () => {
    it('works with STRING, STRING', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split('$cityName', 'CityName')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TotalEdits', 'descending')
        .limit(5)
        .apply(
          'ByNamespace',
          $('wiki').split('$namespace', 'Namespace')
            .apply('TotalEdits', '$wiki.sum($count)')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$TotalAdded', 'descending')
            .limit(5)
        )
    }));

    it('works with NUMBER_BUCKET, STRING', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split('$commentLength.numberBucket(10)', 'CommentLengthB10')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$CommentLengthB10', 'ascending')
        .limit(5)
        .apply(
          'ByNamespace',
          $('wiki').split('$namespace', 'Namespace')
            .apply('TotalEdits', '$wiki.sum($count)')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$TotalAdded', 'descending')
            .limit(5)
        )
    }));

  });


  describe("splits (sequential)", () => {
    it('works with nested split', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki')
        .split({ 'isNew': '$isNew', 'isRobot': '$isRobot' })
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .split('$isNew', 'isNew', 'data')
        .apply('SumTotalEdits', '$data.sum($TotalEdits)')
    }));

  });


  describe("applies", () => {
    it('works with all sorts of applies', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split('$channel', 'Channel')
        .apply('RowCount', '$wiki.count()')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply('SixtySix', 66)
        .apply('AddedBYDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('TokyoAdded', '$wiki.filter($cityName == Tokyo).sum($added)')
        .apply('NullCities', '$wiki.filter($cityName == null).sum($added)')
        .apply('To*Added', '$wiki.filter($cityName.contains("to")).sum($added)')
        .apply('MinDelta', '$wiki.min($min_delta)')
        .apply('MaxDelta', '$wiki.max($max_delta)')
        .apply('Anon', '$wiki.filter($isAnonymous).count()')
        .apply('AbsDeltaX2', '$wiki.sum($delta.absolute()) * 2')
        .apply('SumAdded^0.6', '$wiki.sum($added) ^ 0.6')
        //.apply('Sum(Added^0.6)', '$wiki.sum($added ^ 0.6)') // This is meaningless since added is aggregated
        .sort('$Channel', 'descending')
        .limit(50)
    }));

    it('works with all sorts of filtered aggregates == null', equalityTest({
      executorNames: ['druid', 'postgres'], // , 'mysql'
      expression: $('wiki').split('$channel', 'Channel')
        .apply('RowCount', '$wiki.count()')
        .apply('Added_NullCities', '$wiki.filter($cityName == null).sum($added)')
        .apply('Added_NullCities3', '$wiki.filter($cityName.substr(0, 3) == null).sum($added)')
        .apply('Added_NullCity_lol', '$wiki.filter($cityName.concat(_lol) == null).sum($added)')
        .apply('Added_NullCityExtract', '$wiki.filter($cityName.extract("^(...)") == null).sum($added)')
        .sort('$Channel', 'descending')
        .limit(50)
    }));

    it.skip('works with (approx) countDistinct', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('DistPagesWithinLimits', '($wiki.countDistinct($page) - 279893).absolute() < 10')
    }));

    it('works with max time (total)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('MinTime', '$wiki.min($time)')
        .apply('MaxTime', '$wiki.max($time)')
    }));

    it.skip('works with filtered count', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('CrazyCount', "$wiki.filter($time < '2015-09-12T18Z').sum($count) + $wiki.filter('2015-09-12T18Z' <= $time).sum($count)")
    }));

    it('works with min/max numeric dimension', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: ply()
        .apply('MinCommentLength', '$wiki.min($commentLength)')
        .apply('MaxCommentLength', '$wiki.max($commentLength)')
    }));

  });


  describe("having filter", () => {
    it('works with lessThan', equalityTest({
      executorNames: ['druid', 'mysql'], // , 'postgres'
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .apply('TotalEdits', '$wiki.sum($count)')
        .filter('$TotalEdits < 5096') // Channel 'ko' has exactly 5096 edits
        .limit(20)
    }));

    it('works with lessThanOrEqual', equalityTest({
      executorNames: ['druid', 'mysql'], // , 'postgres'
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .apply('TotalEdits', '$wiki.sum($count)')
        .filter('$TotalEdits <= 5096') // Channel 'ko' has exactly 5096 edits
        .limit(20)
    }));

    it('works with greaterThan', equalityTest({
      executorNames: ['druid', 'mysql'], // , 'postgres'
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .apply('TotalEdits', '$wiki.sum($count)')
        .filter('$TotalEdits > 5096') // Channel 'ko' has exactly 5096 edits
        .limit(20)
    }));

    it('works with greaterThanOrEqual', equalityTest({
      executorNames: ['druid', 'mysql'], // , 'postgres'
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .apply('TotalEdits', '$wiki.sum($count)')
        .filter('$TotalEdits >= 5096') // Channel 'ko' has exactly 5096 edits
        .limit(20)
    }));

    it('works with AND', equalityTest({
      executorNames: ['druid', 'mysql'], // , 'postgres'
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .apply('TotalEdits', '$wiki.sum($count)')
        .filter('$TotalEdits >= 5096 and $TotalEdits < 10000')
        .limit(20)
    }));

    it('works with OR', equalityTest({
      executorNames: ['druid', 'mysql'], // , 'postgres'
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .apply('TotalEdits', '$wiki.sum($count)')
        .filter('$TotalEdits == 5096 or $TotalEdits > 10000') // Channel 'ko' has exactly 5096 edits
        .limit(20)
    }));

  });


  describe("raw (select)", () => {
    it('works with empty filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName == "I am pretty sure this city does not exist"')
        .select('page', 'channel', 'comment', 'added')
        .sort('$comment', 'descending')
    }));

    it('works with basic filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName == "El Paso"')
        .select('page', 'channel', 'comment', 'added')
        .sort('$comment', 'descending')
    }));

    it('works with basic select action (no measures)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName == "El Paso"')
        .select('time', 'channel', 'comment')
        .sort('$comment', 'descending')
    }));

    it('works with basic select action (no dimensions)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName == "El Paso"')
        .select('added', 'deleted')
        .sort('$deleted', 'descending')
    }));

    // Pick a city with distinct time values so as not worry about ties
    // SELECT cityName, COUNT(*) as cnt, COUNT(DISTINCT time) - COUNT(*) AS diff FROM wikipedia GROUP BY 1 HAVING diff = 0 AND cnt > 20 ORDER BY cnt DESC
    it('works with sort on time ascending and limit', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName == "Munich"')
        .select('time', 'added', 'deleted')
        .sort('$time', 'ascending')
        .limit(20)
    }));

    it('works with sort on time descending and limit', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName == "Munich"')
        .select('time', 'added', 'deleted')
        .sort('$time', 'descending')
        .limit(20)
    }));

    it.skip('works with sort on something else and limit', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName == "Munich"')
        .select('time', 'added', 'deleted')
        .sort('$added', 'descending')
        .limit(20)
    }));

    it.skip('works with derived dimension columns', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki')
        .filter('$cityName == "El Paso"')
        .apply('regionNameLOL', '$regionName.concat(LOL)')
        .select('regionNameLOL')
    }));

    it.skip('works with derived measure columns', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki')
        .filter('$cityName == "El Paso"')
        .apply('AddedX2', '$added * 2')
    }));

    it("works with raw data inside a split", equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki')
        .filter('$cityName.match("^San")')
        .split('$cityName', 'City')
        .apply('Edits', '$wiki.sum($count)')
        .sort('$Edits', 'descending')
        .limit(2)
        .apply(
          'Latest2Events',
          $('wiki').sort('$time', 'descending')
            .select("time", "channel", "commentLength")
            .limit(3)
        )
    }));

  });


  describe("value", () => {
    it('works with empty filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: `$wiki.filter($cityName == "this city does not exist").count()`
    }));

    it('works with basic value', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: `$wiki.filter($cityName == "El Paso").count()`
    }));

    it('works with complex value', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: `$wiki.filter($cityName == "El Paso").count() + $wiki.sum($count)`
    }));

    it('works with an even more complex value', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: `1000 - $wiki.filter($cityName == "El Paso").count() - $wiki.sum($count)`
    }));

    it('works with chain division', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: `$wiki.count() / $wiki.sum($count) / 0.25`
    }));

  });

});
