var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { Expression, External, TimeRange, $, ply, r } = plywood;

var context = {
  rankings: External.fromJS({
    engine: 'druid',
    dataSource: 'rankings',
    timeAttribute: 'time',
    allowSelectQueries: true,
    allowEternity: true,
    context: null,
    attributes: [
      { name: 'pageURL', type: 'STRING' }, // VARCHAR(300)
      { name: 'pageRank', type: 'NUMBER' }, // INT
      { name: 'avgDuration', type: 'NUMBER' } // INT
    ],
    druidVersion: '0.9.0'
  }),
  uservisits: External.fromJS({
    engine: 'druid',
    dataSource: 'uservisits',
    timeAttribute: 'visitDate',
    allowSelectQueries: true,
    allowEternity: true,
    context: null,
    attributes: [
      { name: 'sourceIP', type: 'STRING' }, // VARCHAR(116)
      { name: 'destURL', type: 'STRING' }, // VARCHAR(100)
      { name: 'visitDate', type: 'TIME' }, // DATE
      { name: 'adRevenue', type: 'NUMBER' }, // FLOAT
      { name: 'userAgent', type: 'STRING' }, // VARCHAR(256)
      { name: 'countryCode', type: 'STRING' }, // CHAR(3)
      { name: 'languageCode', type: 'STRING' }, // CHAR(6)
      { name: 'searchWord', type: 'STRING' }, // VARCHAR(32)
      { name: 'duration', type: 'NUMBER' } // INT
    ],
    druidVersion: '0.9.0'
  })
};

// https://amplab.cs.berkeley.edu/benchmark/
describe("simulate Druid for amplab benchmark", () => {
  it("works for Query1", () => {
    //         SELECT pageURL, pageRank FROM rankings WHERE pageRank > X
    var sql = 'SELECT pageURL, pageRank FROM rankings WHERE pageRank > 5';
    var ex = Expression.parseSQL(sql).expression;

    expect(ex.toJS()).to.deep.equal(
      $('rankings')
        .filter('$pageRank > 5')
        .apply('pageURL', '$pageURL')
        .apply('pageRank', '$pageRank')
        .select('pageURL', 'pageRank')
        .toJS()
    );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "dataSource": "rankings",
        "dimensions": [
          "pageURL",
          "pageRank"
        ],
        "filter": {
          "alphaNumeric": true,
          "dimension": "pageRank",
          "lower": 5,
          "lowerStrict": true,
          "type": "bound"
        },
        "granularity": "all",
        "intervals": "1000-01-01/3000-01-01",
        "metrics": [
          "!DUMMY"
        ],
        "pagingSpec": {
          "pagingIdentifiers": {},
          "threshold": 10000
        },
        "queryType": "select"
      }
    ]);
  });

  it("works for Query1 (modified to be GROUP BY)", () => {
    //         SELECT pageURL, sum(pageRank) AS pageRank FROM rankings GROUP BY pageURL HAVING pageRank > X
    var sql = 'SELECT pageURL, sum(pageRank) AS pageRank FROM rankings GROUP BY pageURL HAVING pageRank > 5';
    var ex = Expression.parseSQL(sql).expression;

    expect(ex.toJS()).to.deep.equal(
      $('rankings').split('$pageURL', 'pageURL', 'data')
        .apply('pageRank', '$data.sum($pageRank)')
        .filter('$pageRank > 5')
        .toJS()
    );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "pageRank",
            "name": "pageRank",
            "type": "doubleSum"
          }
        ],
        "dataSource": "rankings",
        "dimensions": [
          {
            "dimension": "pageURL",
            "outputName": "pageURL",
            "type": "default"
          }
        ],
        "granularity": "all",
        "having": {
          "aggregation": "pageRank",
          "type": "greaterThan",
          "value": 5
        },
        "intervals": "1000-01-01/3000-01-01",
        "limitSpec": {
          "columns": [
            "pageURL"
          ],
          "limit": 500000,
          "type": "default"
        },
        "queryType": "groupBy"
      }
    ]);
  });

  it("works for Query2", () => {
    //         SELECT SUBSTR(sourceIP, 1, X), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, X)
    var sql = 'SELECT SUBSTR(sourceIP, 1, 5), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, 5)';
    var ex = Expression.parseSQL(sql).expression;

    expect(ex.toJS()).to.deep.equal(
      $('uservisits').split('$sourceIP.substr(1, 5)', 'SUBSTR_sourceIP_1_5', 'data')
        .apply('SUM_adRevenue', '$data.sum($adRevenue)')
        .toJS()
    );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "adRevenue",
            "name": "SUM_adRevenue",
            "type": "doubleSum"
          }
        ],
        "dataSource": "uservisits",
        "dimensions": [
          {
            "dimension": "sourceIP",
            "extractionFn": {
              "type": "substring",
              "index": 1,
              "length": 5
            },
            "outputName": "SUBSTR_sourceIP_1_5",
            "type": "extraction"
          }
        ],
        "granularity": "all",
        "intervals": "1000-01-01/3000-01-01",
        "limitSpec": {
          "columns": [
            "SUBSTR_sourceIP_1_5"
          ],
          "limit": 500000,
          "type": "default"
        },
        "queryType": "groupBy"
      }
    ]);
  });
});
