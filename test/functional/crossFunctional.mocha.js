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

let { druidRequesterFactory } = require('plywood-druid-requester');
let { mySqlRequesterFactory } = require('plywood-mysql-requester');
let { postgresRequesterFactory } = require('plywood-postgres-requester');

let plywood = require('../plywood');
let { Expression, External, TimeRange, $, ply, r, basicExecutorFactory, verboseRequesterFactory } = plywood;

let utils = require('../utils');
let info = require('../info');

let druidRequester = druidRequesterFactory({
  host: info.druidHost
});

let mySqlRequester = mySqlRequesterFactory({
  host: info.mySqlHost,
  database: info.mySqlDatabase,
  user: info.mySqlUser,
  password: info.mySqlPassword
});

let postgresRequester = postgresRequesterFactory({
  host: info.postgresHost,
  database: info.postgresDatabase,
  user: info.postgresUser,
  password: info.postgresPassword
});

// druidRequester = verboseRequesterFactory({
//   requester: druidRequester,
//   onSuccess: () => {}
// });
// mySqlRequester = verboseRequesterFactory({
//   requester: mySqlRequester
// });
// postgresRequester = verboseRequesterFactory({
//   requester: postgresRequester
// });

let attributes = [
  { name: '__time', type: 'TIME', nativeType: '__time' },
  { name: 'sometimeLater', type: 'TIME', nativeType: 'STRING' },
  { name: "channel", type: 'STRING', nativeType: 'STRING' },
  { name: "cityName", type: 'STRING', nativeType: 'STRING' },
  { name: "comment", type: 'STRING', nativeType: 'STRING' },
  { name: "commentLength", type: 'NUMBER', nativeType: 'STRING' },
  { name: "commentLengthStr", type: 'STRING', nativeType: 'STRING' },
  { name: "countryIsoCode", type: 'STRING', nativeType: 'STRING' },
  { name: "countryName", type: 'STRING', nativeType: 'STRING' },
  { name: "deltaBucket100", type: 'NUMBER', nativeType: 'STRING' },
  { name: "isAnonymous", type: 'BOOLEAN', nativeType: 'STRING' },
  { name: "isMinor", type: 'BOOLEAN', nativeType: 'STRING' },
  { name: "isNew", type: 'BOOLEAN', nativeType: 'STRING' },
  { name: "isRobot", type: 'BOOLEAN', nativeType: 'STRING' },
  { name: "isUnpatrolled", type: 'BOOLEAN', nativeType: 'STRING' },
  { name: "metroCode", type: 'NUMBER', nativeType: 'STRING' },
  { name: "namespace", type: 'STRING', nativeType: 'STRING' },
  { name: "page", type: 'STRING', nativeType: 'STRING' },
  { name: "regionIsoCode", type: 'STRING', nativeType: 'STRING' },
  { name: "regionName", type: 'STRING', nativeType: 'STRING' },
  { name: "user", type: 'STRING', nativeType: 'STRING' },
  { name: "userChars", type: 'SET/STRING', nativeType: 'STRING' },

  { name: 'count', type: 'NUMBER', nativeType: 'LONG', unsplitable: true },
  { name: 'delta', type: 'NUMBER', nativeType: 'LONG', unsplitable: true },
  { name: 'min_delta', type: 'NUMBER', nativeType: 'LONG', unsplitable: true },
  { name: 'max_delta', type: 'NUMBER', nativeType: 'LONG', unsplitable: true },
  { name: 'deltaByTen', type: 'NUMBER', nativeType: 'FLOAT', unsplitable: true },
  { name: 'added', type: 'NUMBER', nativeType: 'LONG', unsplitable: true },
  { name: 'deleted', type: 'NUMBER', nativeType: 'LONG', unsplitable: true }
];

let derivedAttributes = {
  pageInBrackets: "'[' ++ $page:STRING ++ ']'" // ToDo: remove :STRING
};

let druidExecutor = basicExecutorFactory({
  datasets: {
    wiki: External.fromJS({
      engine: 'druid',
      source: 'wikipedia',
      context: info.druidContext,
      attributes,
      derivedAttributes,
      version: info.druidVersion,
      allowSelectQueries: true,
      allowEternity: true
    }, druidRequester)
  }
});

let druidLegacyExecutor = basicExecutorFactory({
  datasets: {
    wiki: External.fromJS({
      engine: 'druid',
      source: 'wikipedia',
      context: Object.assign({}, info.druidContext, {
        timeout: 10001 // Put a different timeout here so we can tell queries apart from non-legacy druid
      }),
      attributes,
      derivedAttributes,
      version: '0.8.3',
      allowSelectQueries: true,
      allowEternity: true
    }, druidRequester)
  }
});

let druidSqlExecutor = basicExecutorFactory({
  datasets: {
    wiki: External.fromJS({
      engine: 'druidsql',
      source: 'wikipedia',
      attributes,
      derivedAttributes
    }, druidRequester)
  }
});

let mysqlExecutor = basicExecutorFactory({
  datasets: {
    wiki: External.fromJS({
      engine: 'mysql',
      source: 'wikipedia',
      attributes,
      derivedAttributes
    }, mySqlRequester)
  }
});

let postgresExecutor = basicExecutorFactory({
  datasets: {
    wiki: External.fromJS({
      engine: 'postgres',
      source: 'wikipedia',
      attributes,
      derivedAttributes
    }, postgresRequester)
  }
});

let equalityTest = utils.makeEqualityTest({
  druid: druidExecutor,
  druidLegacy: druidLegacyExecutor,
  druidSql: druidSqlExecutor,
  mysql: mysqlExecutor,
  postgres: postgresExecutor
});

describe("Cross Functional", function() {
  this.timeout(10000);

  describe("filters", () => {
    it('works with empty filter', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName == "this city does not exist")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with raw boolean column filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: ply()
        .apply('wiki', '$wiki.filter($isAnonymous)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with == filter', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($page == "San Francisco")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with != filter', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName != "San Francisco")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with == NULL filter', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName == null)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with != NULL filter', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName != null)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with == unicode filter', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($page == "Викинги")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .contains() and .is() filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName.contains("San") and $cityName == "San Francisco")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .in() filter [dimension without NULLs]', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($channel.in(["en", "simple"]))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .in().not() filter [dimension without NULLs]', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: ply()
        .apply('wiki', '$wiki.filter($channel.in(["en", "simple"]).not())')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it.skip('works with .in().not() filter [dimension with NULLs]', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName.in(["Moscow", "Tel Aviv", "London", "San Francisco"]).not())')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .overlap() filter [dimension without NULLs]', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($channel.overlap(["en", "simple"]))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .overlap().not() filter [dimension without NULLs]', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: ply()
        .apply('wiki', '$wiki.filter($channel.overlap(["en", "simple"]).not())')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .concat().concat().contains() filter', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: ply()
        .apply('wiki', '$wiki.filter(("[" ++ $cityName ++ "]").contains("[san", "ignoreCase"))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with OR filter', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($channel == "en" or $cityName == "Tel Aviv")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with OR filter of identical match', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName.match("San") or $cityName.match("San"))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .contains(,normal)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName.contains("San", normal))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .contains(,ignoreCase)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName.contains("San", ignoreCase))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .match()', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression:  ply()
        .apply('wiki', '$wiki.filter($cityName.match("^S[ab]n .{3,6}$"))')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with primary time filter (single range)', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy', 'mysql', 'postgres'],
      expression:  ply()
        .apply('wiki', $('wiki').filter($('__time').overlap(new Date("2015-09-12T01:00:00Z"), new Date("2015-09-12T02:30:00Z"))))
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with alt time filter (single range)', equalityTest({
      executorNames: ['druid', 'druidLegacy', 'mysql', 'postgres'], // 'druidSql'
      expression:  ply()
        .apply('wiki', $('wiki').filter($('sometimeLater').overlap(new Date("2016-09-12T01:00:00Z"), new Date("2016-09-12T02:30:00Z"))))
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with primary time filter (multi range)', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy', 'mysql', 'postgres'],
      expression:  ply()
        .apply('wiki', $('wiki').filter(
          $('__time').overlap(new Date("2015-09-12T01:00:00Z"), new Date("2015-09-12T02:30:00Z")).or(
            $('__time').overlap(new Date("2015-09-12T03:00:00Z"), new Date("2015-09-12T04:30:00Z")),
            $('__time').overlap(new Date("2015-09-12T05:00:00Z"), new Date("2015-09-12T06:30:00Z")))
        ))
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with alt time filter (multi range)', equalityTest({
      executorNames: ['druid', 'druidLegacy', 'mysql', 'postgres'], // 'druidSql'
      expression:  ply()
        .apply('wiki', $('wiki').filter(
          $('sometimeLater').overlap(new Date("2016-09-12T01:00:00Z"), new Date("2016-09-12T02:30:00Z")).or(
            $('sometimeLater').overlap(new Date("2016-09-12T03:00:00Z"), new Date("2016-09-12T04:30:00Z")),
            $('sometimeLater').overlap(new Date("2016-09-12T05:00:00Z"), new Date("2016-09-12T06:30:00Z")))
        ))
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it.skip('works with .timePart().in()', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression:  ply()
        .apply('wiki', $('wiki').filter($('__time').timePart('HOUR_OF_DAY').is([3, 7])))
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .timePart().in() [alt time column]', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression:  ply()
        .apply('wiki', $('wiki').filter($('sometimeLater').timePart('HOUR_OF_DAY').is([3, 7])))
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with cast from number to time and primary time filter (single range)', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy', 'mysql', 'postgres'],
      expression:  ply()
        .apply('wiki', $('wiki').filter(($('$deltaBucket100').absolute().cast('TIME')) > new Date('1970-01-01T00:00:02.000Z')))
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with cast from number to string in filter', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy', 'mysql', 'postgres'],
      expression:  ply()
        .apply('wiki', $('wiki').filter($('commentLength').cast('STRING').is(r("15"))))
        .apply('TotalEdits', '$wiki.sum($count)')
    }));

    it('works with cast from string to number in filter', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy', 'mysql', 'postgres'],
      expression:  ply()
        .apply('wiki', $('wiki').filter($('commentLengthStr').cast('NUMBER').is(r(15))))
        .apply('TotalEdits', '$wiki.sum($count)')
    }));

    it('works with .lessThan()', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($commentLength < 50)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with .lessThanOrEqual()', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter($commentLength <= 50)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with numeric range, bounds: ()', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter(20 < $commentLength and $commentLength < 50)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with numeric range, bounds: [)', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter(20 <= $commentLength and $commentLength < 50)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with numeric range, bounds: (]', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter(20 < $commentLength and $commentLength <= 50)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with numeric range, bounds: []', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter(20 <= $commentLength and $commentLength <= 50)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with negative number in range', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter(-300 <= $deltaBucket100 and $deltaBucket100 <= 300)')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with string range, bounds: []', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter("b" >= $channel and $channel <= "z")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with string range, bounds: ()', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.filter("d" > $channel and $channel < "w")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with static derived attribute .is()', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: ply()
        .apply('wiki', '$wiki.filter($pageInBrackets == "[Deaths_in_2015]")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with attribute dynamically derived from substr .is()', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: ply()
        .apply('wiki', '$wiki.apply(city3, $cityName.substr(0, 3)).filter($city3 == "San")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with attribute dynamically derived from transformCase .is()', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: ply()
        .apply('wiki', '$wiki.apply(city3, $cityName.transformCase("upperCase")).filter($city3 == "SAN FRANCISCO")')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
    }));


    it('works with length action on filter', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName.length() > 4')
        .split('$cityName', 'CityName')
        .apply('Count', '$wiki.sum($count)')
        .sort('$Count', 'descending')
        .limit(5)
    }));

    it('works with indexOf action on filter', equalityTest({
      executorNames: ['mysql', 'druidLegacy', 'postgres', 'druid'],
      expression: $('wiki').filter('$cityName.indexOf(x) > 5')
        .split('$cityName', 'CityName')
        .apply('Count', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5)
    }));

    it('works with transformCase action on filter', equalityTest({
      executorNames: ['mysql', 'druidLegacy', 'postgres', 'druid'],
      expression: $('wiki').filter('$cityName.transformCase("lowerCase") == "el paso"')
        .split('$cityName', 'CityName')
        .apply('Count', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5)
    }));

    it('works with sub-query filter', equalityTest({
      executorNames: ['mysql', 'druidLegacy', 'postgres', 'druid'],
      expression: $('wiki').filter('$commentLength > $wiki.average($commentLength)')
        .split('$channel', 'Channel')
        .apply('Count', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5)
    }));

    it('works with sub-query filter', equalityTest({
      executorNames: ['mysql', 'druidLegacy', 'postgres', 'druid'],
      expression: $('wiki').filter('$commentLength > $wiki.average($commentLength)')
        .split('$channel', 'Channel')
        .apply('Count', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5)
    }));

  });


  describe("splits (single)", () => {
    it('works with empty filter split', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki')
        .filter('$cityName == "this city does not exist"')
        .split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .limit(20)
    }));

    it('works with constant split', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
      expression: $('wiki')
        .split('blah', 'Constant')
        .apply('TotalEdits', '$wiki.sum($count)')
    }));

    it('works with plain split', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki')
        .split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
    }));

    it('works with plain with limit split', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki')
        .split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .limit(10000)
    }));

    it('works with BOOLEAN split (native)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').split('$isNew', 'IsNew')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it('works with BOOLEAN split (expression, defined)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
      expression: $('wiki').split('$channel == de', 'ChannelIsDE')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it('works with BOOLEAN split (expression, nullable)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
      expression: $('wiki').split('$cityName == "San Francisco"', 'CityIsSF')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it('works with BOOLEAN split (overlap)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
      expression: $('wiki').split($('channel').overlap(['en', 'simple']), 'ChannelIsEnSimple')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it('works with BOOLEAN split (overlap __time)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
      expression: $('wiki').split($('__time').overlap(new Date("2015-09-12T01:00:00Z"), new Date("2015-09-12T02:30:00Z")), 'TheHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it.skip('works with BOOLEAN split (overlap secondaryTime)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
      expression: $('wiki').split($('sometimeLater').overlap(new Date("2016-09-12T01:00:00Z"), new Date("2016-09-12T02:30:00Z")), 'TheHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
    }));

    it('works with STRING split (sort on split)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').split('$channel', 'Channel') // ToDo: change this to user
        .sort('$Channel', 'ascending')
        .limit(20)
    }));

    it('works with STRING indexOf action', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split('$page.indexOf(b)', 'BLocation')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with STRING transform case action', equalityTest({
      executorNames: ['mysql', 'druidLegacy', 'postgres', 'druid'],
      expression: $('wiki').split('$cityName.transformCase("lowerCase")', 'CityLower')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with STRING split (sort on apply)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').split('$channel', 'Channel')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with STRING split (nullable dimension)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
      expression: $('wiki').split('$cityName', 'City')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with dynamic derived column', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
      expression: $('wiki').apply('city3', '$cityName.substr(0, 3)').split('$city3', 'City3')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with chained number bucket', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki') // null handling varies
        .split('$cityName.length().numberBucket(10)', 'cityNameBucket')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with STRING split .fallback() no match', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split('$cityName.fallback("NoCity")', 'CityFallback')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with STRING split .fallback() with match', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split('$cityName.fallback("Bucharest")', 'CityFallback')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with NUMBER split (numberBucket) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split($("commentLength").numberBucket(10), 'CommentLength10')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$CommentLength10', 'ascending')
        .limit(20) // To force a topN (for now)
    }));

    it('works with NUMBER split (numberBucket) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split($("commentLength").numberBucket(10), 'CommentLength10')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(20)
    }));

    it('works with NUMBER split (with negatives) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki').split("$deltaBucket100", 'DeltaBucket100')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$DeltaBucket100', 'ascending')
        .limit(20)
    }));

    it('works with NUMBER split (-expression) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split("1000 - $commentLength", 'MinusCommentLength')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$MinusCommentLength', 'descending')
        .limit(20) // To force a topN (for now)
    }));

    it('works with NUMBER split (1 + expression) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split("1 + $commentLength", 'OnePlusCommentLength')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$OnePlusCommentLength', 'descending')
        .limit(20) // To force a topN (for now)
    }));

    it('works with NUMBER split (expression / 10) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql'], // 'druidSql', 'postgres'  # ToDo: postgres truncates results
      expression: $('wiki').split("$commentLength / 10", 'CommentLengthDiv')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalEdits', 'descending')
        .limit(20) // To force a topN (for now)
    }));

    it('works with NUMBER split (expression / 10).numberBucket (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split("($commentLength / 10).numberBucket(2, 0)", 'CommentLengthDivBucket')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalEdits', 'descending')
        .limit(20) // To force a topN (for now)
    }));

    it('works with NUMBER split (expression^2) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split("$commentLength^2", 'CommentLengthSq')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$CommentLengthSq', 'descending')
        .limit(20) // To force a topN (for now)
    }));

    it('works with TIME split (raw) (sort on split)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').filter('$__time < "2015-09-12T02Z"').split($('__time'), 'TimeRaw')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimeRaw', 'ascending')
        .limit(20)
    }));

    it('works with TIME split (timeFloor) (sort on split)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').split($('__time').timeFloor('PT1H', 'Etc/UTC'), 'TimeFloorHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TimeFloorHour', 'ascending')
        .limit(20)
    }));

    it('works with TIME split (timeBucket) (sort on split)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').split($('__time').timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TimeByHour', 'ascending')
        .limit(20)
    }));

    it('works with TIME split (timeBucket, Kathmandu) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'], // 'druidSql', 'postgres' // ToDo: should be able to add TZ to DSQL query
      expression: $('wiki').split($('__time').timeBucket('PT1H', 'Asia/Kathmandu'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TimeByHour', 'ascending')
        .limit(20)
    }));

    it('works with TIME split (timeBucket) (sort on apply)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').split($('__time').timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works with TIME split (timePart) (sort on split)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').split($('__time').timePart('HOUR_OF_DAY', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$HourOfDay', 'ascending')
    }));

    it('works with TIME split (timePart(YEAR)) (sort on split)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').split($('__time').timePart('YEAR'), 'TimePart')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimePart', 'ascending')
    }));

    it('works with TIME split (timePart(DAY_OF_YEAR)) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
      expression: $('wiki').split($('__time').timePart('DAY_OF_YEAR'), 'TimePart')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimePart', 'ascending')
    }));

    it('works with TIME split (timePart(DAY_OF_MONTH)) (sort on split)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').split($('__time').timePart('DAY_OF_MONTH'), 'TimePart')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimePart', 'ascending')
    }));

    it('works with TIME split (timePart(DAY_OF_WEEK)) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
      expression: $('wiki').split($('__time').timePart('DAY_OF_WEEK'), 'TimePart')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimePart', 'ascending')
    }));

    it('works with TIME split (timePart(HOUR_OF_DAY)) (sort on split)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').split($('__time').timePart('HOUR_OF_DAY'), 'TimePart')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimePart', 'ascending')
    }));

    it('works with TIME split (timePart(MINUTE_OF_DAY)) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split($('__time').timePart('MINUTE_OF_DAY'), 'TimePart')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimePart', 'ascending')
        .limit(200)
    }));

    it('works with TIME split (timePart) (sort on apply)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').split($('__time').timePart('HOUR_OF_DAY', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works with secondary TIME split (raw) (sort on split)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').filter('$__time < "2015-09-12T02Z"').split($("sometimeLater"), 'TimeRaw')
        .apply('TotalEdits', '$wiki.sum($count)')
        .sort('$TimeRaw', 'ascending')
        .limit(20)
    }));

    it('works with secondary TIME split (timeBucket PT1H) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split($("sometimeLater").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TimeByHour', 'ascending')
    }));

    it('works with secondary TIME split (timeBucket PT1H) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split($("sometimeLater").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it.skip('works with secondary TIME split (timeBucket PT3H) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split($("sometimeLater").timeBucket('PT3H', 'Etc/UTC'), 'TimeByHour')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TimeByHour', 'ascending')
        .limit(10)
    }));

    it('works with secondary TIME split (timePart) (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split($("sometimeLater").timePart('MINUTE_OF_HOUR', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$HourOfDay', 'ascending')
    }));

    it('works with secondary TIME split (timePart) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split($("sometimeLater").timePart('MINUTE_OF_HOUR', 'Etc/UTC'), 'HourOfDay')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works with secondary TIME split (timePart, TZ) (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki').split($("sometimeLater").timePart('DAY_OF_YEAR', 'America/New_York'), 'HourOfDay')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works .then(literal).fallback split', equalityTest({
      executorNames: ['mysql', 'postgres'], // 'druid'
      expression: $('wiki')
        .split(
          $('__time').timePart('HOUR_OF_DAY').lessThan(10).then('Morning')
            .fallback($('__time').timePart('HOUR_OF_DAY').lessThan(20).then('Afternoon'))
            .fallback('Evening'),
          'Greeting'
        )
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works .then(variable).fallback split', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki')
        .split(
          $('channel').overlap(['en', 'es', 'he']).then('$channel').fallback('Other'),
          'Channel'
        )
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works .then(variable).fallback split complex', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'],
      expression: $('wiki')
        .split(
          $('channel').overlap(['en', 'es', 'he']).then('$channel')
            .fallback($('channel').overlap(['fr', 'ru']).then('War'))
            .fallback('Other'),
          'Channel'
        )
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(10)
    }));

    it('works with length action on split', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').split({ 'PageLength': '$page.length()' })
        .apply('Count', '$wiki.sum($count)')
        .sort('$Count', 'descending')
        .limit(5)
    }));

    it('works with cast action from number to time on split', equalityTest({
      executorNames: ['postgres', 'mysql', 'druid'],
      expression: $('wiki').filter('$deltaBucket100.in([1000, 2000, 3000, 8000])') // druid time is precise to seconds
        .split('$deltaBucket100.cast(TIME)', 'deltaBucketToDate')
        .sort('$deltaBucketToDate', 'descending')
        .limit(10)
    }));

    it('works with cast action from time to number on split', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
      expression: $('wiki').split('$__time.cast("NUMBER")', 'time')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(4)
    }));

    it('works with cast action from number to string on split', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').split('$commentLength.cast("STRING")', 'StringifiedCommentLength')
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(4)
    }));

    it('works with cast action from string to number on split', equalityTest({
      executorNames: ['druid', 'druidSql', 'druidLegacy'],
      expression: $('wiki').split('$commentLengthStr.cast("NUMBER")', 'NumberfiedString')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(4)
    }));

  });


  describe("splits (single, multi-dim)", () => {
    it('works with BOOLEAN multi-dim-split', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
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
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki')
        .split({
          'Channel': "$channel",
          'TimeByHour': '$__time.timeBucket(PT1H)',
          'IsNew': '$isNew',
          'ChannelIsDE': "$channel == 'de'"
        })
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(4)
    }));

    it('works with timeFloor multi-dimensional split', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki')
        .split({
          'Channel': "$channel",
          'TimeByHour': '$__time.timeFloor(PT1M)'
        })
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(4)
    }));

  });


  describe("splits (nested)", () => {
    it('works with STRING, STRING', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
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
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
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

    it.skip('works with heatmap like query', equalityTest({ // ToDo: add when rollup is off
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: ply()
        .apply(
          'ys',
          $('wiki').split('$__time.timeBucket(PT1H)', 'v')
            .sort('$v', 'ascending')
            .limit(5)
        )
        .apply(
          'xs',
          $('wiki').split('$channel', 'v')
            .apply('count', '$wiki.count()')
            .sort('$count', 'descending')
            .limit(5)
        )
        .apply('count', '$wiki.count()')
        .apply(
          'cells',
          $('wiki').filter('$channel.in($xs.collect($v)).and($__time.in($ys.collect($v)))')
            .split({ __time: '$__time.timeBucket(PT1M)', channel: '$channel' })
            .apply('count', '$wiki.count()')
            .sort('$__time', 'ascending')
            .limit(25)
        )
    }));

  });


  describe("splits (sequential)", () => {
    it('works with nested split', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
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
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
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
        .apply('SumIndexOf', '$wiki.sum($user.transformCase("upperCase").indexOf("A"))')
        .sort('$Channel', 'descending')
        .limit(50)
    }));

    it('works with all sorts of filtered aggregates == null', equalityTest({
      executorNames: ['druid', 'postgres'], // 'druidSql', 'mysql'
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
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: ply()
        .apply('DistPagesWithinLimits', '($wiki.countDistinct($page) - 279893).absolute() < 10')
    }));

    it('works with max time (total)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: ply()
        .apply('MinTime', '$wiki.min($__time)')
        .apply('MaxTime', '$wiki.max($__time)')
    }));

    it.skip('works with filtered count', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: ply()
        .apply('CrazyCount', "$wiki.filter($__time < '2015-09-12T18Z').sum($count) + $wiki.filter('2015-09-12T18Z' <= $__time).sum($count)")
    }));

    it('works with min/max numeric dimension', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
      expression: ply()
        .apply('MinCommentLength', '$wiki.min($commentLength)')
        .apply('MaxCommentLength', '$wiki.max($commentLength)')
    }));

    it('works with string indexOf in apply', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: ply()
        .apply('BLocation', '$wiki.sum($page.indexOf(b))')
        .sort('$BLocation', 'descending')
        .limit(20)
    }));

    it('works with string indexOf in apply not found', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: ply()
        .apply('BLocation', '$wiki.sum($page.indexOf(thisasdsczxczvdprobablydoesntexist))')
        .sort('$BLocation', 'descending')
        .limit(20)
    }));

    it('works with string length in apply', equalityTest({
      /*
       // 2 entries in zh:
        druid and postgres returns: "Page": "𠊎話" (len 2) and mysql has: "Page": "?話"(len 2)
        druid and postgres returns: 虾子𡎚站 (len 5) and mysql has 虾子?站(len 4)
       */
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: ply()
        .apply('wiki', '$wiki.filter($channel != "zh")')
        .apply('PageLength', '$wiki.sum($page.length())')
        .sort('$PageLength', 'descending')
        .limit(10)
    }));

    it('works with set cardinality in apply', equalityTest({
      executorNames: ['druid', 'postgres'],
      expression: $('wiki').split('$channel', 'Channel')
        .apply('SIZE', ('$wiki.max($userChars.cardinality())'))
        .sort('$Channel', 'descending')
        .limit(5)
    }));

    // min and maxes don't work for stuff that's not primary time column
    it.skip('works with cast from number to time in apply', equalityTest({
      executorNames: ['druid', 'postgres'],
      expression: $('wiki').filter('$cityName == "El Paso"')
        .apply('castValue', '$commentLength.cast("TIME")') // ToDo: move to end
        .select('page', 'commentLength', 'comment', 'added', 'castValue')
        .sort('$comment', 'descending')
    }));

    it.skip('works with cast from time to number in apply', equalityTest({
      executorNames: ['druid', 'postgres'],
      expression: $('wiki').filter('$cityName == "El Paso"') // ToDo: move to end
        .apply('castValue', '$wiki.max($__time.cast("NUMBER"))')
        .select('page', '__time', 'comment', 'added', 'castValue')
        .sort('$comment', 'descending')
    }));

    it.skip('works with cast from number to string in apply', equalityTest({
      executorNames: ['druid', 'postgres'],
      expression: $('wiki').filter('$cityName == "El Paso"')
        .apply('castValue', '$commentLength.cast("STRING")') // ToDo: move to end
        .select('page', 'commentLength', 'comment', 'added', 'castValue')
        .sort('$comment', 'descending')
    }));

    it.skip('works with cast from string to number in apply', equalityTest({
      executorNames: ['druid', 'postgres'],
      expression: $('wiki').filter('$cityName == "El Paso"')
        .apply('castValue', '$wiki.max($commentLengthStr.cast("NUMBER"))') // ToDo: move to end
        .select('page', 'commentLengthStr', 'comment', 'added', 'castValue')
        .sort('$comment', 'descending')
    }));

  });


  describe("having filter", () => {
    it('works with lessThan', equalityTest({
      executorNames: ['druid', 'mysql'], // 'druidSql', 'postgres'
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .apply('TotalEdits', '$wiki.sum($count)')
        .filter('$TotalEdits < 5096') // Channel 'ko' has exactly 5096 edits
        .limit(20)
    }));

    it('works with lessThanOrEqual', equalityTest({
      executorNames: ['druid', 'mysql'], // 'druidSql', 'postgres'
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .apply('TotalEdits', '$wiki.sum($count)')
        .filter('$TotalEdits <= 5096') // Channel 'ko' has exactly 5096 edits
        .limit(20)
    }));

    it('works with greaterThan', equalityTest({
      executorNames: ['druid', 'mysql'], // 'druidSql', 'postgres'
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .apply('TotalEdits', '$wiki.sum($count)')
        .filter('$TotalEdits > 5096') // Channel 'ko' has exactly 5096 edits
        .limit(20)
    }));

    it('works with greaterThanOrEqual', equalityTest({
      executorNames: ['druid', 'mysql'], // 'druidSql', 'postgres'
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .apply('TotalEdits', '$wiki.sum($count)')
        .filter('$TotalEdits >= 5096') // Channel 'ko' has exactly 5096 edits
        .limit(20)
    }));

    it('works with AND', equalityTest({
      executorNames: ['druid', 'mysql'], // 'druidSql', 'postgres'
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .apply('TotalEdits', '$wiki.sum($count)')
        .filter('$TotalEdits >= 5096 and $TotalEdits < 10000')
        .limit(20)
    }));

    it('works with OR', equalityTest({
      executorNames: ['druid', 'mysql'], // 'druidSql', 'postgres'
      expression: $('wiki').split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .apply('TotalEdits', '$wiki.sum($count)')
        .filter('$TotalEdits == 5096 or $TotalEdits > 10000') // Channel 'ko' has exactly 5096 edits
        .limit(20)
    }));

  });


  describe("raw (select)", () => {
    it('works with empty filter', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName == "I am pretty sure this city does not exist"')
        .sort('$__time', 'ascending')
        .select('page', 'channel', 'comment', 'added')
    }));

    it('works with basic filter', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName == "El Paso"')
        .sort('$__time', 'ascending')
        .select('page', 'channel', 'comment', 'added')
    }));

    it('works with basic select action (no metrics)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName == "El Paso"')
        .sort('$__time', 'ascending')
        .select('__time', 'channel', 'comment')
    }));

    it('works with basic select action (no dimensions)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName == "El Paso"')
        .sort('$__time', 'ascending')
        .select('added', 'deleted')
    }));

    // Pick a city with distinct time values so as not worry about ties
    // SELECT cityName, COUNT(*) as cnt, COUNT(DISTINCT time) - COUNT(*) AS diff FROM wikipedia GROUP BY 1 HAVING diff = 0 AND cnt > 20 ORDER BY cnt DESC
    it('works with sort on time ascending and limit', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName == "Munich"')
        .select('__time', 'added', 'deleted')
        .sort('$__time', 'ascending')
        .limit(20)
    }));

    it('works with sort on time descending and limit', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName == "Munich"')
        .select('__time', 'added', 'deleted')
        .sort('$__time', 'descending')
        .limit(20)
    }));

    it.skip('works with sort on something else and limit', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki').filter('$cityName == "Munich"')
        .select('__time', 'added', 'deleted')
        .sort('$added', 'descending')
        .limit(20)
    }));

    it('works with derived dimension columns (SUBSTR)', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki')
        .filter('$cityName == "El Paso"')
        .apply('reg', '$regionName.substr(1, 3)')
        .select('reg')
    }));

    it('works with derived dimension columns (CONCAT)', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // , 'druidSql'
      expression: $('wiki')
        .filter('$cityName == "El Paso"')
        .apply('regionNameLOL', '$regionName.concat(LOL)')
        .select('regionNameLOL')
    }));

    it.skip('works with derived measure columns', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: $('wiki')
        .filter('$cityName == "El Paso"')
        .apply('AddedX2', '$added * 2')
    }));

    it("works with raw data inside a split", equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql'
      expression: $('wiki')
        .filter('$cityName.match("^San")')
        .split('$cityName', 'City')
        .apply('Edits', '$wiki.sum($count)')
        .sort('$Edits', 'descending')
        .limit(2)
        .apply(
          'Latest2Events',
          $('wiki').sort('$__time', 'descending')
            .select('__time', "channel", "commentLength")
            .limit(3)
        )
    }));

    it('works with cardinality in select', equalityTest({
      executorNames: ['druid', 'postgres'],
      expression: $('wiki').filter('$cityName == "El Paso"')
        .select('userChars')
        .apply('Cardinality', '$userChars.cardinality()')
        .select("Cardinality")
        .limit(5)
    }));

    it('works in large result case', equalityTest({
      executorNames: ['druid', 'druidSql'], // 'mysql', 'postgres' when testing on raw data
      expression: $('wiki')
        .sort('$__time', 'ascending')
        .select('__time', 'page', 'channel')
        .limit(60)
    }));

  });


  describe("value", () => {
    it('works with empty filter', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: `$wiki.filter($cityName == "this city does not exist").count()`
    }));

    it('works with basic value', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: `$wiki.filter($cityName == "El Paso").count()`
    }));

    it('works with complex value', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: `$wiki.filter($cityName == "El Paso").count() + $wiki.sum($count)`
    }));

    it('works with an even more complex value', equalityTest({
      executorNames: ['druid', 'druidSql', 'mysql', 'postgres'],
      expression: `1000 - $wiki.filter($cityName == "El Paso").count() - $wiki.sum($count)`
    }));

    it('works with chain division', equalityTest({
      executorNames: ['druid', 'mysql', 'postgres'], // 'druidSql' !!!
      expression: `$wiki.count() / $wiki.sum($count) / 0.25`
    }));

  });

});
