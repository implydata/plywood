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
    version: '0.9.0',
    dataSource: 'rankings',
    timeAttribute: 'time',
    allowSelectQueries: true,
    allowEternity: true,
    attributes: [
      { name: 'pageURL', type: 'STRING' }, // VARCHAR(300)
      { name: 'pageRank', type: 'NUMBER' }, // INT
      { name: 'avgDuration', type: 'NUMBER' } // INT
    ]
  }),
  uservisits: External.fromJS({
    engine: 'druid',
    version: '0.9.0',
    dataSource: 'uservisits',
    timeAttribute: 'visitDate',
    allowSelectQueries: true,
    allowEternity: true,
    attributes: [
      { name: 'sourceIP', type: 'STRING' }, // VARCHAR(116)
      { name: 'destURL', type: 'STRING' }, // VARCHAR(100)
      { name: 'visitDate', type: 'TIME' }, // DATE
      { name: 'adRevenue', type: 'NUMBER', unsplitable: true }, // FLOAT
      { name: 'userAgent', type: 'STRING' }, // VARCHAR(256)
      { name: 'countryCode', type: 'STRING' }, // CHAR(3)
      { name: 'languageCode', type: 'STRING' }, // CHAR(6)
      { name: 'searchWord', type: 'STRING' }, // VARCHAR(32)
      { name: 'duration', type: 'NUMBER', unsplitable: true } // INT
    ]
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
        "intervals": "1000/3000",
        "metrics": [
          "!DUMMY"
        ],
        "pagingSpec": {
          "pagingIdentifiers": {},
          "threshold": 50
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
            "fieldNames": [
              "pageRank"
            ],
            "fnAggregate": "function(_c,pageRank) { return _c+(+pageRank); }",
            "fnCombine": "function(a,b) { return a+b; }",
            "fnReset": "function() { return 0; }",
            "name": "pageRank",
            "type": "javascript"
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
        "intervals": "1000/3000",
        "limitSpec": {
          "columns": [
            { "dimension": "pageURL" }
          ],
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
      $('uservisits').split('$sourceIP.substr(1, 5)', 'SUBSTR(sourceIP, 1, 5)', 'data')
        .apply('SUM(adRevenue)', '$data.sum($adRevenue)')
        .toJS()
    );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "adRevenue",
            "name": "SUM(adRevenue)",
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
            "outputName": "SUBSTR(sourceIP, 1, 5)",
            "type": "extraction"
          }
        ],
        "granularity": "all",
        "intervals": "1000/3000",
        "limitSpec": {
          "columns": [
            { "dimension": "SUBSTR(sourceIP, 1, 5)" }
          ],
          "type": "default"
        },
        "queryType": "groupBy"
      }
    ]);
  });
});
