var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { Expression, External, Dataset, TimeRange, $, ply, r } = plywood;

var attributes = [
  { name: 'time', type: 'TIME' },
  { name: 'color', type: 'STRING' },
  { name: 'cut', type: 'STRING' },
  { name: 'tags', type: 'SET/STRING' },
  { name: 'carat', type: 'NUMBER' },
  { name: 'height_bucket', special: 'range', separator: ';', rangeSize: 0.05, digitsAfterDecimal: 2 },
  { name: 'price', type: 'NUMBER', unsplitable: true },
  { name: 'tax', type: 'NUMBER', unsplitable: true },
  { name: 'vendor_id', special: 'unique', unsplitable: true }
];

var context = {
  'diamonds': External.fromJS({
    engine: 'druid',
    dataSource: 'diamonds',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true,
    filter: $("time").in({
      start: new Date('2015-03-12T00:00:00'),
      end: new Date('2015-03-19T00:00:00')
    }),
    druidVersion: '0.9.1'
  }),
  'diamonds-alt:;<>': External.fromJS({
    engine: 'druid',
    dataSource: 'diamonds-alt:;<>',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true,
    filter: $("time").in({
      start: new Date('2015-03-12T00:00:00'),
      end: new Date('2015-03-19T00:00:00')
    }),
    druidVersion: '0.9.1'
  })
};

var contextUnfiltered = {
  'diamonds': External.fromJS({
    engine: 'druid',
    dataSource: 'diamonds',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true
  })
};

describe("simulate Druid", function() {
  it("works in basic case", function() {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").is('D')))
      .apply('Count', '$diamonds.count()')
      .apply('TotalPrice', '$diamonds.sum($price)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          },
          {
            "fieldName": "price",
            "name": "TotalPrice",
            "type": "doubleSum"
          }
        ],
        "dataSource": "diamonds",
        "filter": {
          "dimension": "color",
          "type": "selector",
          "value": "D"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "queryType": "timeseries"
      }
    ]);
  });

  it.skip("works on initial dataset", function() {
    var dataset = Dataset.fromJS([
      { col: 'D' },
      { col: 'E' }
    ]);

    var ex = ply(dataset)
      .apply("diamonds", $('diamonds').filter($("color").is('$col')))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([]);
  });

  it.skip("works in advanced case", function() {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").is('D').and($('cut').in(['Good', 'Bad', 'Ugly']))))
      .apply('Count', '$diamonds.count()')
      .apply('TotalPrice', '$diamonds.sum($price)')
      .apply('PriceTimes2', '$diamonds.sum($price) * 2')
      .apply('PriceMinusTax', '$diamonds.sum($price) - $diamonds.sum($tax)')
      .apply('PriceDiff', '$diamonds.sum($price - $tax)')
      .apply('Crazy', '$diamonds.sum($price) - $diamonds.sum($tax) + 10 - $diamonds.sum($carat)')
      .apply('PriceAndTax', '$diamonds.sum($price) * $diamonds.sum($tax)')
      .apply('PriceGoodCut', $('diamonds').filter($('cut').is('good')).sum('$price'))
      .apply('AvgPrice', '$diamonds.average($price)')
      .apply(
        'Cuts',
        $("diamonds").split("$cut", 'Cut')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(2)
          .apply(
            'Time',
            $("diamonds").split($("time").timeBucket('P1D', 'America/Los_Angeles'), 'Timestamp')
              .apply('TotalPrice', $('diamonds').sum('$price'))
              .sort('$Timestamp', 'ascending')
              //.limit(10)
              .apply(
                'Carats',
                $("diamonds").split($("carat").numberBucket(0.25), 'Carat')
                  .apply('Count', $('diamonds').count())
                  .sort('$Count', 'descending')
                  .limit(3)
              )
          )
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "tax",
            "name": "_sd_0",
            "type": "doubleSum"
          },
          {
            "fieldName": "carat",
            "name": "_sd_1",
            "type": "doubleSum"
          },
          {
            "name": "Count",
            "type": "count"
          },
          {
            "fieldName": "price",
            "name": "TotalPrice",
            "type": "doubleSum"
          },
          {
            "aggregator": {
              "fieldName": "price",
              "name": "PriceGoodCut",
              "type": "doubleSum"
            },
            "filter": {
              "dimension": "cut",
              "type": "selector",
              "value": "good"
            },
            "name": "PriceGoodCut",
            "type": "filtered"
          }
        ],
        "dataSource": "diamonds",
        "filter": {
          "dimension": "color",
          "type": "selector",
          "value": "D"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "postAggregations": [
          {
            "fields": [
              {
                "fieldName": "TotalPrice",
                "type": "fieldAccess"
              },
              {
                "type": "constant",
                "value": 2
              }
            ],
            "fn": "*",
            "name": "PriceTimes2",
            "type": "arithmetic"
          },
          {
            "fields": [
              {
                "fieldName": "TotalPrice",
                "type": "fieldAccess"
              },
              {
                "fieldName": "_sd_0",
                "type": "fieldAccess"
              }
            ],
            "fn": "-",
            "name": "PriceMinusTax",
            "type": "arithmetic"
          },
          {
            "fields": [
              {
                "fieldName": "TotalPrice",
                "type": "fieldAccess"
              },
              {
                "fieldName": "_sd_0",
                "type": "fieldAccess"
              }
            ],
            "fn": "-",
            "name": "PriceDiff",
            "type": "arithmetic"
          },
          {
            "fields": [
              {
                "fields": [
                  {
                    "fieldName": "TotalPrice",
                    "type": "fieldAccess"
                  },
                  {
                    "type": "constant",
                    "value": 10
                  }
                ],
                "fn": "+",
                "type": "arithmetic"
              },
              {
                "fields": [
                  {
                    "fieldName": "_sd_0",
                    "type": "fieldAccess"
                  },
                  {
                    "fieldName": "_sd_1",
                    "type": "fieldAccess"
                  }
                ],
                "fn": "+",
                "type": "arithmetic"
              }
            ],
            "fn": "-",
            "name": "Crazy",
            "type": "arithmetic"
          },
          {
            "fields": [
              {
                "fieldName": "TotalPrice",
                "type": "fieldAccess"
              },
              {
                "fieldName": "_sd_0",
                "type": "fieldAccess"
              }
            ],
            "fn": "*",
            "name": "PriceAndTax",
            "type": "arithmetic"
          },
          {
            "fields": [
              {
                "fieldName": "TotalPrice",
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
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "cut",
          "outputName": "Cut",
          "type": "default"
        },
        "filter": {
          "dimension": "color",
          "type": "selector",
          "value": "D"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 2
      },
      {
        "aggregations": [
          {
            "fieldName": "price",
            "name": "TotalPrice",
            "type": "doubleSum"
          }
        ],
        "dataSource": "diamonds",
        "filter": {
          "fields": [
            {
              "dimension": "color",
              "type": "selector",
              "value": "D"
            },
            {
              "dimension": "cut",
              "type": "selector",
              "value": "some_cut"
            }
          ],
          "type": "and"
        },
        "granularity": {
          "period": "P1D",
          "timeZone": "America/Los_Angeles",
          "type": "period"
        },
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "queryType": "timeseries"
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "extractionFn": {
            "function": "function(d){d=Number(d); if(isNaN(d)) return 'null'; return Math.floor(d / 0.25) * 0.25;}",
            "type": "javascript"
          },
          "dimension": "carat",
          "outputName": "Carat",
          "type": "extraction"
        },
        "filter": {
          "fields": [
            {
              "dimension": "color",
              "type": "selector",
              "value": "D"
            },
            {
              "dimension": "cut",
              "type": "selector",
              "value": "some_cut"
            }
          ],
          "type": "and"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-13T07/2015-03-14T07"
        ],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 3
      }
    ]);
  });

  it("works on fancy filter dataset (EXTRACT / IS)", function() {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.extract('^(.)') == 'D'"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "expr": "^(.)",
        "replaceMissingValue": true,
        "type": "regex"
      },
      "type": "extraction",
      "value": "D"
    });
  });

  it("works on fancy filter (SUBSET / IS)", function() {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.substr(0, 1) == 'D'"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "type": "substring",
        "index": 0,
        "length": 1
      },
      "type": "extraction",
      "value": "D"
    });
  });

  it("works on fancy filter (SUBSET / IN)", function() {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.substr(0, 1).in(['D', 'C'])"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "type": "or",
      "fields": [
        {
          "dimension": "color",
          "extractionFn": {
            "type": "substring",
            "index": 0,
            "length": 1
          },
          "type": "extraction",
          "value": "D"
        },
        {
          "dimension": "color",
          "extractionFn": {
            "type": "substring",
            "index": 0,
            "length": 1
          },
          "type": "extraction",
          "value": "C"
        }
      ]
    });
  });

  it("works on fancy filter (LOOKUP / IN)", function() {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.lookup('some_lookup').in(['D', 'C'])"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "fields": [
        {
          "dimension": "color",
          "extractionFn": {
            "injective": false,
            "lookup": {
              "namespace": 'some_lookup',
              "type": "namespace"
            },
            "type": "lookup"
          },
          "type": "extraction",
          "value": "D"
        },
        {
          "dimension": "color",
          "extractionFn": {
            "injective": false,
            "lookup": {
              "namespace": 'some_lookup',
              "type": "namespace"
            },
            "type": "lookup"
          },
          "type": "extraction",
          "value": "C"
        }
      ],
      "type": "or"
    });
  });

  it("works with basic timePart", function() {
    var ex = ply()
      .apply(
        'HoursOfDay',
        $("diamonds").split("$time.timePart(HOUR_OF_DAY, 'Etc/UTC')", 'HourOfDay')
          .apply('TotalPrice', '$diamonds.sum($price)')
          .sort('$TotalPrice', 'descending')
          .limit(3)
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "price",
            "name": "TotalPrice",
            "type": "doubleSum"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "__time",
          "extractionFn": {
            "format": "H",
            "locale": "en-US",
            "timeZone": "Etc/UTC",
            "type": "timeFormat"
          },
          "outputName": "HourOfDay",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "metric": "TotalPrice",
        "queryType": "topN",
        "threshold": 3
      }
    ]);
  });

  it("works with basic concat", function() {
    var ex = ply()
      .apply(
        'Colors',
        $("diamonds").split("'!!!<' ++ $color ++ '>!!!'", 'Colors')
          .apply('TotalPrice', '$diamonds.sum($price)')
          .sort('$TotalPrice', 'descending')
          .limit(3)
      );

    expect(ex.simulateQueryPlan(context)[0].dimension).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "function": "function(d){return ((\"!!!<\"+d)+\">!!!\");}",
        "type": "javascript",
        "injective": true
      },
      "outputName": "Colors",
      "type": "extraction"
    });
  });

  it("works with basic substr", function() {
    var ex = ply()
      .apply(
        'Colors',
        $("diamonds").split("$color.substr(1, 2)", 'Colors')
          .apply('TotalPrice', '$diamonds.sum($price)')
          .sort('$TotalPrice', 'descending')
          .limit(3)
      );

    expect(ex.simulateQueryPlan(context)[0].dimension).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "type": "substring",
        "index": 1,
        "length": 2
      },
      "outputName": "Colors",
      "type": "extraction"
    });
  });

  it.skip("works with basic boolean split", function() {
    var ex = ply()
      .apply(
        'Colors',
        $("diamonds").split("$color == $cut", 'IsA')
          .apply('TotalPrice', '$diamonds.sum($price)')
          .sort('$TotalPrice', 'descending')
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([]);
  });

  it("works with having filter", function() {
    var ex = $("diamonds").split("$cut", 'Cut')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending')
      .filter($('Count').greaterThan(100))
      .limit(10);

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimensions": [
          {
            "dimension": "cut",
            "outputName": "Cut",
            "type": "default"
          }
        ],
        "granularity": "all",
        "having": {
          "aggregation": "Count",
          "type": "greaterThan",
          "value": 100
        },
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "limitSpec": {
          "columns": [
            {
              "dimension": "Count",
              "direction": "descending"
            }
          ],
          "limit": 10,
          "type": "default"
        },
        "queryType": "groupBy"
      }
    ]);
  });

  it("works with lower bound only time filter", function() {
    var ex = ply()
      .apply('diamonds', $("diamonds").filter($("time").in({ start: new Date('2015-03-12T00:00:00'), end: null })))
      .apply('Count', $('diamonds').count());

    expect(ex.simulateQueryPlan(contextUnfiltered)[0].intervals).to.deep.equal([
      "2015-03-12/3000-01-01"
    ]);
  });

  it("works with upper bound only time filter", function() {
    var ex = ply()
      .apply('diamonds', $("diamonds").filter($("time").in({ start: null, end: new Date('2015-03-12T00:00:00') })))
      .apply('Count', $('diamonds').count());

    expect(ex.simulateQueryPlan(contextUnfiltered)[0].intervals).to.deep.equal([
      "1000-01-01/2015-03-12"
    ]);
  });

  it("works with range bucket", function() {
    var ex = ply()
      .apply(
        'HeightBuckets',
        $("diamonds").split("$height_bucket", 'HeightBucket')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(10)
      )
      .apply(
        'HeightUpBuckets',
        $("diamonds").split($('height_bucket').numberBucket(2, 0.5), 'HeightBucket')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(10)
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "extractionFn": {
            "function": "function(d) {\nvar m = d.match(/^((?:-?[1-9]\\d*|0)\\.\\d{2});((?:-?[1-9]\\d*|0)\\.\\d{2})$/);\nif(!m) return 'null';\nvar s = +m[1];\nif(!(Math.abs(+m[2] - s - 0.05) < 1e-6)) return 'null'; \nvar parts = String(Math.abs(s)).split('.');\nparts[0] = ('000000000' + parts[0]).substr(-10);\nreturn (start < 0 ?'-':'') + parts.join('.');\n}",
            "type": "javascript"
          },
          "dimension": "height_bucket",
          "outputName": "HeightBucket",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      },
// ---------------------
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "extractionFn": {
            "function": "function(d) {\nvar m = d.match(/^((?:-?[1-9]\\d*|0)\\.\\d{2});((?:-?[1-9]\\d*|0)\\.\\d{2})$/);\nif(!m) return 'null';\nvar s = +m[1];\nif(!(Math.abs(+m[2] - s - 0.05) < 1e-6)) return 'null'; s=Math.floor((s - 0.5) / 2) * 2 + 0.5;\nvar parts = String(Math.abs(s)).split('.');\nparts[0] = ('000000000' + parts[0]).substr(-10);\nreturn (start < 0 ?'-':'') + parts.join('.');\n}",
            "type": "javascript"
          },
          "dimension": "height_bucket",
          "outputName": "HeightBucket",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("makes a timeBoundary query", function() {
    var ex = ply()
      .apply('maximumTime', '$diamonds.max($time)')
      .apply('minimumTime', '$diamonds.min($time)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "dataSource": "diamonds",
        "queryType": "timeBoundary"
      }
    ]);
  });

  it("makes a timeBoundary query (maxTime only)", function() {
    var ex = ply()
      .apply('maximumTime', '$diamonds.max($time)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "dataSource": "diamonds",
        "queryType": "timeBoundary",
        "bound": "maxTime"
      }
    ]);
  });

  it("makes a timeBoundary query (minTime only)", function() {
    var ex = ply()
      .apply('minimumTime', '$diamonds.min($time)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "dataSource": "diamonds",
        "queryType": "timeBoundary",
        "bound": "minTime"
      }
    ]);
  });

  it("makes a topN with a timePart dim extraction fn", function() {
    var ex = $("diamonds").split($("time").timePart('SECOND_OF_DAY', 'Etc/UTC'), 'Time')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending')
      .limit(10);

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "__time",
          "extractionFn": {
            "format": "H'*60+'m'*60+'s",
            "locale": "en-US",
            "timeZone": "Etc/UTC",
            "type": "timeFormat"
          },
          "outputName": "Time",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("makes a filtered aggregate query", function() {
    var ex = ply()
      .apply(
        'BySegment',
        $('diamonds').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeSegment')
          .apply('Total', $('diamonds').sum('$price'))
          .apply('GoodPrice', $('diamonds').filter($('cut').is('Good')).sum('$price'))
          .apply('NotBadPrice', $('diamonds').filter($('cut').isnt('Bad')).sum('$price'))
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "price",
            "name": "Total",
            "type": "doubleSum"
          },
          {
            "aggregator": {
              "fieldName": "price",
              "name": "GoodPrice",
              "type": "doubleSum"
            },
            "filter": {
              "dimension": "cut",
              "type": "selector",
              "value": "Good"
            },
            "name": "GoodPrice",
            "type": "filtered"
          },
          {
            "aggregator": {
              "fieldName": "price",
              "name": "NotBadPrice",
              "type": "doubleSum"
            },
            "filter": {
              "field": {
                "dimension": "cut",
                "type": "selector",
                "value": "Bad"
              },
              "type": "not"
            },
            "name": "NotBadPrice",
            "type": "filtered"
          }
        ],
        "dataSource": "diamonds",
        "granularity": {
          "period": "PT1H",
          "timeZone": "Etc/UTC",
          "type": "period"
        },
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "queryType": "timeseries"
      }
    ]);
  });

  it.skip("makes a filter on timePart", function() {
    var ex = $("diamonds").filter(
      $("time").timePart('HOUR_OF_DAY', 'Etc/UTC').in([3, 4, 10]).and($("time").in([
        TimeRange.fromJS({ start: new Date('2015-03-12T00:00:00'), end: new Date('2015-03-15T00:00:00') }),
        TimeRange.fromJS({ start: new Date('2015-03-16T00:00:00'), end: new Date('2015-03-18T00:00:00') })
      ]))
    ).split("$color", 'Color')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending')
      .limit(10);

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "color",
          "outputName": "Color",
          "type": "default"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12T03/2015-03-12T05",
          "2015-03-12T10/2015-03-12T11",
          "2015-03-13T03/2015-03-13T05",
          "2015-03-13T10/2015-03-13T11",
          "2015-03-14T03/2015-03-14T05",
          "2015-03-14T10/2015-03-14T11",

          "2015-03-16T03/2015-03-16T05",
          "2015-03-16T10/2015-03-16T11",
          "2015-03-17T03/2015-03-17T05",
          "2015-03-17T10/2015-03-17T11"
        ],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it.skip("splits on timePart with sub split", function() {
    var ex = $("diamonds").split($("time").timePart('HOUR_OF_DAY', 'Etc/UTC'), 'hourOfDay')
      .apply('Count', '$diamonds.count()')
      .sort('$Count', 'descending')
      .limit(3)
      .apply(
        'Colors',
        $("diamonds").split("$color", 'Color')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(10)
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "__time",
          "extractionFn": {
            "format": "H",
            "locale": "en-US",
            "timeZone": "Etc/UTC",
            "type": "timeFormat"
          },
          "outputName": "hourOfDay",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 3
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "color",
          "outputName": "Color",
          "type": "default"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12T04/2015-03-12T05",
          "2015-03-13T04/2015-03-13T05",
          "2015-03-14T04/2015-03-14T05",
          "2015-03-15T04/2015-03-15T05",
          "2015-03-16T04/2015-03-16T05",
          "2015-03-17T04/2015-03-17T05",
          "2015-03-18T04/2015-03-18T05"
        ],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("works without a sort defined", function() {
    var ex = ply()
      .apply(
        'topN',
        $("diamonds").split("$color", 'Color')
          .apply('Count', $('diamonds').count())
          .limit(10)
      )
      .apply(
        'timeseries',
        $("diamonds").split($("time").timeBucket('P1D', 'America/Los_Angeles'), 'Timestamp')
          .apply('Count', $('diamonds').count())
          .limit(10)
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "color",
          "outputName": "Color",
          "type": "default"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "metric": {
          "type": "lexicographic"
        },
        "queryType": "topN",
        "threshold": 10
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "granularity": {
          "period": "P1D",
          "timeZone": "America/Los_Angeles",
          "type": "period"
        },
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "queryType": "timeseries"
      }
    ]);
  });

  it("works with no attributes in dimension split dataset", function() {
    var ex = ply()
      .apply(
        'Cuts',
        $('diamonds').split("$cut", 'Cut')
          .sort('$Cut', 'ascending')
          .limit(5)
          .apply(
            'Colors',
            $('diamonds').split('$color', 'Color')
              .apply('Count', $('diamonds').count())
              .sort('$Count', 'descending')
              .limit(3)
          )
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "cut",
          "outputName": "Cut",
          "type": "default"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "metric": {
          "metric": {
            "type": "lexicographic"
          },
          "type": "inverted"
        },
        "queryType": "topN",
        "threshold": 5
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "color",
          "outputName": "Color",
          "type": "default"
        },
        "filter": {
          "dimension": "cut",
          "type": "selector",
          "value": "some_cut"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 3
      }
    ]);
  });

  it("works with no attributes in time split dataset", function() {
    var ex = ply()
      .apply(
        'ByHour',
        $('diamonds').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
          .sort('$TimeByHour', 'ascending')
          .apply(
            'Colors',
            $('diamonds').split('$color', 'Color')
              .apply('Count', $('diamonds').count())
              .sort('$Count', 'descending')
              .limit(3)
          )
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "granularity": {
          "period": "PT1H",
          "timeZone": "Etc/UTC",
          "type": "period"
        },
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "queryType": "timeseries"
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "color",
          "outputName": "Color",
          "type": "default"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-14/2015-03-14T01"
        ],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 3
      }
    ]);
  });

  it.skip("inlines a defined derived attribute", function() {
    var ex = ply()
      .apply("diamonds", $('diamonds').apply('sale_price', '$price + $tax'))
      .apply(
        'ByTime',
        $('diamonds').split($("time").timeBucket('P1D', 'Etc/UTC'), 'Time')
          .apply('TotalSalePrice', $('diamonds').sum('$sale_price'))
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "price",
            "name": "_sd_0",
            "type": "doubleSum"
          },
          {
            "fieldName": "tax",
            "name": "_sd_1",
            "type": "doubleSum"
          }
        ],
        "dataSource": "diamonds",
        "granularity": {
          "period": "P1D",
          "timeZone": "Etc/UTC",
          "type": "period"
        },
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "postAggregations": [
          {
            "fields": [
              {
                "fieldName": "_sd_0",
                "type": "fieldAccess"
              },
              {
                "fieldName": "_sd_1",
                "type": "fieldAccess"
              }
            ],
            "fn": "+",
            "name": "TotalSalePrice",
            "type": "arithmetic"
          }
        ],
        "queryType": "timeseries"
      }
    ]);
  });

  it("makes a query on a dataset with a fancy name", function() {
    var ex = ply()
      .apply('maximumTime', '${diamonds-alt:;<>}.max($time)')
      .apply('minimumTime', '${diamonds-alt:;<>}.min($time)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "dataSource": "diamonds-alt:;<>",
        "queryType": "timeBoundary"
      }
    ]);
  });

  // In the future, druid will support finalize field access
  // ( and potentially finalization of javascript post aggs.) In the meantime, it shouldnt be something that we try to implement
  // in the plywood layer
  // https://github.com/druid-io/druid/issues/2433

  it.skip("makes a query with countDistinct", function() {
    var ex = ply()
      .apply('NumColors', '$diamonds.countDistinct($color)')
      .apply('NumVendors', '$diamonds.countDistinct($vendor_id)')
      .apply('VendorsByColors', '$NumVendors / $NumColors');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "byRow": true,
            "fieldNames": [
              "color"
            ],
            "name": "NumColors",
            "type": "cardinality"
          },
          {
            "fieldName": "vendor_id",
            "name": "NumVendors",
            "type": "hyperUnique"
          }
        ],
        "dataSource": "diamonds",
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "postAggregations": [
          {
            "fields": [
              {
                "fieldName": "NumVendors",
                "type": "hyperUniqueCardinality"
              },
              {
                "fieldName": "NumColors",
                "type": "hyperUniqueCardinality"
              }
            ],
            "fn": "/",
            "name": "VendorsByColors",
            "type": "arithmetic"
          }
        ],
        "queryType": "timeseries"
      }
    ]);
  });

  it("works with duplicate aggregates", function() {
    var ex = ply()
      .apply('Price', '$diamonds.sum($price)')
      .apply('Price', '$diamonds.sum($price)')
      .apply('M', '$diamonds.max($price)')
      .apply('M', '$diamonds.min($price)')
      .apply('Post', '$diamonds.count() * 2')
      .apply('Post', '$diamonds.count() * 3');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "price",
            "name": "Price",
            "type": "doubleSum"
          },
          {
            "fieldName": "price",
            "name": "M",
            "type": "doubleMin"
          },
          {
            "name": "_sd_0",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "postAggregations": [
          {
            "fields": [
              {
                "fieldName": "_sd_0",
                "type": "fieldAccess"
              },
              {
                "type": "constant",
                "value": 3
              }
            ],
            "fn": "*",
            "name": "Post",
            "type": "arithmetic"
          }
        ],
        "queryType": "timeseries"
      }
    ]);
  });

  it("works on exact time filter (is)", function() {
    var ex = ply()
      .apply('diamonds', $('diamonds').filter($('time').is(new Date('2015-03-12T01:00:00.123Z'))))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].intervals).to.deep.equal([
      "2015-03-12T01:00:00.123/2015-03-12T01:00:00.124"
    ]);
  });

  it("works on exact time filter (in interval)", function() {
    var ex = ply()
      .apply('diamonds', $('diamonds').filter($('time').in(new Date('2015-03-12T01:00:00.123Z'), new Date('2015-03-12T01:00:00.124Z'))))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].intervals).to.deep.equal([
      "2015-03-12T01:00:00.123/2015-03-12T01:00:00.124"
    ]);
  });

  it("works contains filter (case sensitive)", function() {
    var ex = ply()
      .apply('diamonds', $('diamonds').filter($('color').contains(r('sup"yo'))))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "function": "function(d){return (''+d).indexOf(\"sup\\\"yo\")>-1;}",
      "type": "javascript"
    });
  });

  it("works contains filter (case insensitive)", function() {
    var ex = ply()
      .apply('diamonds', $('diamonds').filter($('color').contains(r('sup"yo'), 'ignoreCase')))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "query": {
        "type": "fragment",
        "values": ['sup"yo']
      },
      "type": "search"
    });
  });

  it("works with SELECT query", function() {
    var ex = $('diamonds')
      .filter('$color == "D"')
      .limit(10);

    var queryPlan = ex.simulateQueryPlan(context);

    expect(queryPlan).to.deep.equal([
      {
        "dataSource": "diamonds",
        "dimensions": [],
        "filter": {
          "dimension": "color",
          "type": "selector",
          "value": "D"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "metrics": [],
        "pagingSpec": {
          "pagingIdentifiers": {},
          "threshold": 10
        },
        "queryType": "select"
      }
    ]);
  });

  it("works multi-dimensional GROUP BYs", function() {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").in(['A', 'B', 'some_color'])))
      .apply(
        'Cuts',
        $("diamonds").split({
            'Cut': "$cut",
            'Color': '$color',
            'TimeByHour': '$time.timeBucket(PT1H, "Etc/UTC")'
          })
          .apply('Count', $('diamonds').count())
          .limit(3)
          .apply(
            'Carats',
            $("diamonds").split($("carat").numberBucket(0.25), 'Carat')
              .apply('Count', $('diamonds').count())
              .sort('$Count', 'descending')
              .limit(3)
          )
      );

    var queryPlan = ex.simulateQueryPlan(context);

    expect(queryPlan).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimensions": [
          {
            "dimension": "color",
            "outputName": "Color",
            "type": "default"
          },
          {
            "dimension": "cut",
            "outputName": "Cut",
            "type": "default"
          },
          {
            "dimension": "__time",
            "extractionFn": {
              "format": "yyyy-MM-dd'T'HH':00Z",
              "locale": "en-US",
              "timeZone": "Etc/UTC",
              "type": "timeFormat"
            },
            "outputName": "TimeByHour",
            "type": "extraction"
          }
        ],
        "filter": {
          "fields": [
            {
              "dimension": "color",
              "type": "selector",
              "value": "A"
            },
            {
              "dimension": "color",
              "type": "selector",
              "value": "B"
            },
            {
              "dimension": "color",
              "type": "selector",
              "value": "some_color"
            }
          ],
          "type": "or"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "limitSpec": {
          "columns": [
            "Color"
          ],
          "limit": 3,
          "type": "default"
        },
        "queryType": "groupBy"
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "carat",
          "extractionFn": {
            "function": "function(d){d=Number(d); if(isNaN(d)) return 'null'; return Math.floor(d / 0.25) * 0.25;}",
            "type": "javascript"
          },
          "outputName": "Carat",
          "type": "extraction"
        },
        "filter": {
          "fields": [
            {
              "dimension": "color",
              "type": "selector",
              "value": "some_color"
            },
            {
              "dimension": "cut",
              "type": "selector",
              "value": "some_cut"
            }
          ],
          "type": "and"
        },
        "granularity": "all",
        "intervals": ["2015-03-14/2015-03-14T01"],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 3
      }
    ]);
  });

  it("works multi-dimensional GROUP BYs (no limit)", function() {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").in(['A', 'B', 'some_color'])))
      .apply(
        'Cuts',
        $("diamonds").split({
            'Cut': "$cut",
            'Color': '$color',
            'TimeByHour': '$time.timeBucket(PT1H, "Etc/UTC")'
          })
          .apply('Count', $('diamonds').count())
      );

    var queryPlan = ex.simulateQueryPlan(context);

    expect(queryPlan).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimensions": [
          {
            "dimension": "color",
            "outputName": "Color",
            "type": "default"
          },
          {
            "dimension": "cut",
            "outputName": "Cut",
            "type": "default"
          }
        ],
        "filter": {
          "fields": [
            {
              "dimension": "color",
              "type": "selector",
              "value": "A"
            },
            {
              "dimension": "color",
              "type": "selector",
              "value": "B"
            },
            {
              "dimension": "color",
              "type": "selector",
              "value": "some_color"
            }
          ],
          "type": "or"
        },
        "granularity": {
          "period": "PT1H",
          "timeZone": "Etc/UTC",
          "type": "period"
        },
        "intervals": [
          "2015-03-12/2015-03-19"
        ],
        "limitSpec": {
          "columns": [
            "Color"
          ],
          "limit": 500000,
          "type": "default"
        },
        "queryType": "groupBy"
      }
    ]);
  });

  it("adds context to query if set on External", function(testComplete) {
    var ds = External.fromJS({
      engine: 'druid',
      dataSource: 'diamonds',
      timeAttribute: 'time',
      attributes,
      allowSelectQueries: true,
      filter: $("time").in({
        start: new Date('2015-03-12T00:00:00'),
        end: new Date('2015-03-19T00:00:00')
      }),
      context: {
        priority: -1,
        queryId: 'test'
      }
    });

    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").is('D')))
      .apply('Count', '$diamonds.count()')
      .apply('TotalPrice', '$diamonds.sum($price)');

    expect(ex.simulateQueryPlan({ diamonds: ds })[0].context).to.deep.equal({ priority: -1, queryId: 'test' });

    return testComplete();
  });
});
