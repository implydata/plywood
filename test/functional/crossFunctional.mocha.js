var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var { druidRequesterFactory } = require('plywood-druid-requester');
var { mySqlRequesterFactory } = require('plywood-mysql-requester');

var plywood = require('../../build/plywood');
var { External, TimeRange, $, ply, basicExecutorFactory } = plywood;

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

var attributes = [
  { name: 'time', type: 'TIME' },
  { name: 'sometimeLater', type: 'TIME' },
  { name: 'channel', type: 'STRING' },
  { name: 'page', type: 'STRING' },
  { name: 'page_unique', special: 'unique' },
  { name: 'user', type: 'STRING' },
  { name: 'isNew', type: 'BOOLEAN' },
  { name: 'isAnonymous', type: 'BOOLEAN' },
  { name: 'commentLength', type: 'NUMBER' },
  { name: 'metroCode', type: 'STRING' },
  { name: 'cityName', type: 'STRING' },
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

    it('works with an IS NULL filter', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: ply()
        .apply('wiki', '$wiki.filter($cityName == null)')
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
        .apply('wiki', '$wiki')
        //.apply('RowCount', '$wiki.count()') // ToDo: make wikipedia data in MySQL rolled up
        .apply('TotalAdded', '$wiki.sum($added)')
    }));

    it('works with STRING split (sort on split)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki')
        .split('$channel', 'Channel')
        .sort('$Channel', 'ascending')
        .limit(5)
    }));

    it('works with STRING split (sort on apply)', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki')
        .split('$channel', 'Channel')
        .apply('TotalAdded', '$wiki.sum($added)')
        .sort('$TotalAdded', 'descending')
        .limit(5)
    }));

    it('works with TIME split (timeBucket) (sort on split)');

    it('works with TIME split (timeBucket) (sort on apply)');

    it('works with TIME split (timePart) (sort on split)');

    it('works with TIME split (timePart) (sort on apply)');

    it('works with multi-dimensional split');
  });

  describe("applies", () => {
    it('works with all sorts of applies', equalityTest({
      executorNames: ['druid', 'mysql'],
      expression: $('wiki')
        .split('$channel', 'Channel')
          .apply('TotalAdded', '$wiki.sum($added)')
          .apply('TokyoAdded', '$wiki.filter($cityName == Tokyo).sum($added)')
          .sort('$Channel', 'descending')
          .limit(50)
    }));
  });
});
