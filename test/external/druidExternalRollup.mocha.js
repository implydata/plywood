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
          "makerAction": {
            "action": "sum",
            "expression": {
              "name": "deleted",
              "op": "ref"
            }
          },
          "name": "deleted",
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
          "name": "channel",
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
      .apply('AvgAdded', '$wiki.average($added)');

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
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
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
            "name": "AvgAdded",
            "type": "arithmetic"
          }
        ],
        "queryType": "timeseries"
      }
    ]);
  });

  it("works in filtered average case", () => {
    var ex = ply()
      .apply('AvgEnAdded', '$wiki.filter($channel == "en").average($added)')
      .apply('AvgHeDeleted', '$wiki.filter($channel == "he").average($deleted)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "aggregator": {
              "fieldName": "added",
              "name": "!T_0",
              "type": "doubleSum"
            },
            "filter": {
              "dimension": "channel",
              "type": "selector",
              "value": "en"
            },
            "name": "!T_0",
            "type": "filtered"
          },
          {
            "aggregator": {
              "fieldName": "count",
              "name": "!T_1",
              "type": "doubleSum"
            },
            "filter": {
              "dimension": "channel",
              "type": "selector",
              "value": "en"
            },
            "name": "!T_1",
            "type": "filtered"
          },
          {
            "aggregator": {
              "fieldName": "deleted",
              "name": "!T_2",
              "type": "doubleSum"
            },
            "filter": {
              "dimension": "channel",
              "type": "selector",
              "value": "he"
            },
            "name": "!T_2",
            "type": "filtered"
          },
          {
            "aggregator": {
              "fieldName": "count",
              "name": "!T_3",
              "type": "doubleSum"
            },
            "filter": {
              "dimension": "channel",
              "type": "selector",
              "value": "he"
            },
            "name": "!T_3",
            "type": "filtered"
          }
        ],
        "dataSource": "diamonds",
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "postAggregations": [
          {
            "fields": [
              {
                "fieldName": "!T_0",
                "type": "fieldAccess"
              },
              {
                "fieldName": "!T_1",
                "type": "fieldAccess"
              }
            ],
            "fn": "/",
            "name": "AvgEnAdded",
            "type": "arithmetic"
          },
          {
            "fields": [
              {
                "fieldName": "!T_2",
                "type": "fieldAccess"
              },
              {
                "fieldName": "!T_3",
                "type": "fieldAccess"
              }
            ],
            "fn": "/",
            "name": "AvgHeDeleted",
            "type": "arithmetic"
          }
        ],
        "queryType": "timeseries"
      }
    ]);
  });

});
