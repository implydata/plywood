var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { Expression, External, TimeRange, $, ply, r } = plywood;

describe("DruidExternal Rollup", () => {

  var context = {
    wiki: External.fromJS({
      engine: 'druid',
      version: '0.9.1',
      dataSource: 'diamonds',
      rollup: true,
      timeAttribute: 'time',
      attributes: [
        {
          "name": "time",
          "type": "TIME"
        },
        {
          "makerAction": {
            "action": "sum",
            "expression": {
              "name": "added",
              "op": "ref"
            }
          },
          "name": "added",
          "type": "NUMBER",
          "unsplitable": true
        },
        {
          "name": "anonymous",
          "type": "STRING"
        },
        {
          "makerAction": {
            "action": "count"
          },
          "name": "count",
          "type": "NUMBER",
          "unsplitable": true
        },
        {
          "name": "delta_hist",
          "special": "histogram",
          "type": "NUMBER"
        },
        {
          "name": "language",
          "type": "STRING"
        },
        {
          "name": "namespace",
          "type": "SET/STRING"
        },
        {
          "name": "page",
          "type": "STRING"
        },
        {
          "name": "user_unique",
          "special": "unique",
          "type": "STRING"
        }
      ],
      allowSelectQueries: true,
      filter: $("time").in({
        start: new Date('2015-03-12T00:00:00'),
        end: new Date('2015-03-19T00:00:00')
      })
    })
  };

  it("works in basic case", () => {
    var ex = ply()
      .apply('Count', '$wiki.count()')
      .apply('AvgPrice', '$wiki.average($added)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "count",
            "name": "Count",
            "type": "doubleSum"
          },
          {
            "fieldName": "added",
            "name": "!T_0",
            "type": "doubleSum"
          }
        ],
        "dataSource": "diamonds",
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "postAggregations": [
          {
            "fields": [
              {
                "fieldName": "!T_0",
                "type": "fieldAccess"
              },
              {
                "fieldName": "Count",
                "type": "fieldAccess"
              }
            ],
            "fn": "/",
            "name": "AvgPrice",
            "type": "arithmetic"
          }
        ],
        "queryType": "timeseries"
      }
    ]);
  });

});
