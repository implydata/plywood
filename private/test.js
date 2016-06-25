var plywood = require('../build/plywood');
var ply = plywood.ply;
var $ = plywood.$;
var External = plywood.External;
var Expression = plywood.Expression;

var ex = Expression.fromJS({
  "op": "chain",
  "expression": {
    "op": "literal",
    "value": [
      {}
    ],
    "type": "DATASET"
  },
  "actions": [
    {
      "action": "apply",
      "expression": {
        "op": "chain",
        "expression": {
          "op": "ref",
          "name": "demoexchange"
        },
        "action": {
          "action": "filter",
          "expression": {
            "op": "chain",
            "expression": {
              "op": "ref",
              "name": "time"
            },
            "actions": [
              {
                "action": "in",
                "expression": {
                  "op": "literal",
                  "value": {
                    "start": "2016-04-30T00:00:00.000Z",
                    "end": "2016-05-01T00:00:00.000Z"
                  },
                  "type": "TIME_RANGE"
                }
              },
              {
                "action": "and",
                "expression": {
                  "op": "chain",
                  "expression": {
                    "op": "ref",
                    "name": "country_id"
                  },
                  "action": {
                    "action": "in",
                    "expression": {
                      "op": "literal",
                      "value": {
                        "setType": "STRING",
                        "elements": [
                          "USA",
                          "FIN"
                        ]
                      },
                      "type": "SET"
                    }
                  }
                }
              }
            ]
          }
        }
      },
      "name": "timeseries"
    },
    {
      "action": "apply",
      "expression": {
        "op": "chain",
        "expression": {
          "op": "ref",
          "name": "timeseries"
        },
        "actions": [
          {
            "action": "split",
            "name": "timerange",
            "expression": {
              "op": "chain",
              "expression": {
                "op": "ref",
                "name": "time"
              },
              "action": {
                "action": "timeBucket",
                "duration": "PT1H",
                "timezone": "Etc/UTC"
              }
            },
            "dataName": "timeseries"
          },
          {
            "action": "apply",
            "expression": {
              "op": "chain",
              "expression": {
                "op": "ref",
                "name": "timeseries"
              },
              "actions": [
                {
                  "action": "sum",
                  "expression": {
                    "op": "ref",
                    "name": "clear_price"
                  }
                }
              ]
            },
            "name": "ecpm"
          }
        ]
      },
      "name": "Time"
    }
  ]
});

var demoexchange = External.fromJS({
  engine: 'druid',
  source: 'demoexchange',
  timeAttribute: 'time',
  attributes: [
    { name: 'time', type: 'TIME' },
    { name: 'country_id', type: 'STRING' },
    { name: 'clear_price', type: 'NUMBER' },
    { name: 'imp_cnt', type: 'NUMBER' }
  ] 
});


console.log('ex.toString(2)', ex.toString(2));

console.log(ex.simulateQueryPlan({ demoexchange: demoexchange }));
  
