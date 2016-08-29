/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
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
  { name: 'isNice', type: 'BOOLEAN' },
  { name: 'tags', type: 'SET/STRING' },
  { name: 'carat', type: 'NUMBER' },
  { name: 'height_bucket', type: 'NUMBER' },
  { name: 'price', type: 'NUMBER', unsplitable: true },
  { name: 'tax', type: 'NUMBER', unsplitable: true },
  { name: 'vendor_id', special: 'unique', unsplitable: true },

  { name: 'try', type: 'NUMBER' }, // Added here because 'try' is a JS keyword
  { name: 'a+b', type: 'NUMBER' } // Added here because it is invalid JS without escaping
];

var customTransforms = {
  sliceLastChar: {
    extractionFn: {
      "type" : "javascript",
      "function" : "function(x) { return x.slice(-1) }"
    }
  },
  getLastChar: {
    extractionFn: {
      "type" : "javascript",
      "function" : "function(x) { return x.charAt(x.length - 1) }"
    }
  },
  timesTwo: {
    extractionFn: {
      "type" : "javascript",
      "function" : "function(x) { return x * 2 }"
    }
  },
  concatWithConcat: {
    extractionFn: {
      "type" : "javascript",
      "function" : "function(x) { return x.concat('concat') }"
    }
  }
};

var diamondsCompact = External.fromJS({
  engine: 'druid',
  version: '0.9.2',
  source: 'diamonds-compact',
  timeAttribute: 'time',
  attributes: [
    { name: 'time', type: 'TIME', makerAction: { action: 'timeFloor', duration: 'P1D', timezone: 'Etc/UTC' } },
    { name: 'color', type: 'STRING' },
    { name: 'cut', type: 'STRING' },
    { name: 'price', type: 'NUMBER', unsplitable: true }
  ],
  customTransforms,
  concealBuckets: true,
  allowSelectQueries: true,
  filter: $("time").in({
    start: new Date('2015-03-12T00:00:00'),
    end: new Date('2015-03-19T00:00:00')
  })
});

var context = {
  'diamonds': External.fromJS({
    engine: 'druid',
    version: '0.9.2',
    source: 'diamonds',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true,
    filter: $("time").in({
      start: new Date('2015-03-12T00:00:00'),
      end: new Date('2015-03-19T00:00:00')
    })
  }).addDelegate(diamondsCompact),
  'diamonds-alt:;<>': External.fromJS({
    engine: 'druid',
    version: '0.9.2',
    source: 'diamonds-alt:;<>',
    timeAttribute: 'time',
    attributes,
    customTransforms,
    allowSelectQueries: true,
    filter: $("time").in({
      start: new Date('2015-03-12T00:00:00'),
      end: new Date('2015-03-19T00:00:00')
    })
  })
};

var contextUnfiltered = {
  'diamonds': External.fromJS({
    engine: 'druid',
    source: 'diamonds',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true
  })
};

describe("simulate Druid", () => {
  it("works in basic case", () => {
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
        "dataSource": "diamonds-compact",
        "filter": {
          "dimension": "color",
          "type": "selector",
          "value": "D"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "queryType": "timeseries"
      }
    ]);
  });

  it("works on initial dataset", () => {
    var dataset = Dataset.fromJS([
      { col: 'D' },
      { col: 'E' }
    ]);

    var ex = ply(dataset)
      .apply("diamonds", $('diamonds').filter($("color").is('$col')))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "__VALUE__",
            "type": "count"
          }
        ],
        "dataSource": "diamonds-compact",
        "filter": {
          "dimension": "color",
          "type": "selector",
          "value": "D"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "queryType": "timeseries"
      },
      {
        "aggregations": [
          {
            "name": "__VALUE__",
            "type": "count"
          }
        ],
        "dataSource": "diamonds-compact",
        "filter": {
          "dimension": "color",
          "type": "selector",
          "value": "E"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "queryType": "timeseries"
      }
    ]);
  });

  it("works in advanced case", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").is('D').and($('tags').overlap(['Good', 'Bad', 'Ugly']))))
      .apply('Count', '$diamonds.count()')
      .apply('TotalPrice', '$diamonds.sum($price)')
      .apply('PriceTimes2', '$diamonds.sum($price) * 2')
      .apply('PriceMinusTax', '$diamonds.sum($price) - $diamonds.sum($tax)')
      .apply('PriceDiff', '$diamonds.sum($price - $tax)')
      .apply('Crazy', '$diamonds.sum($price) - $diamonds.sum($tax) + 10 - $diamonds.sum($carat)')
      .apply('PriceAndTax', '$diamonds.sum($price) * $diamonds.sum($tax)')
      .apply('SixtySix', 66)
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
          },
          {
            "fieldName": "tax",
            "name": "!T_0",
            "type": "doubleSum"
          },
          {
            "fieldNames": [
              "carat"
            ],
            "fnAggregate": "function($$,_carat) { return $$+(+_carat); }",
            "fnCombine": "function(a,b) { return a+b; }",
            "fnReset": "function() { return 0; }",
            "name": "!T_1",
            "type": "javascript"
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
              "values": ["Good", "Bad", "Ugly"],
              "type": "in",
              "dimension": "tags"
            }
          ],
          "type": "and"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "postAggregations": [
          {
            "fields": [
              {
                "fieldName": "TotalPrice",
                "type": "fieldAccess"
              },
              {
                "fieldName": "!T_0",
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
          },
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
                "fieldName": "!T_0",
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
                "fields": [
                  {
                    "fields": [
                      {
                        "fieldName": "TotalPrice",
                        "type": "fieldAccess"
                      },
                      {
                        "fieldName": "!T_0",
                        "type": "fieldAccess"
                      }
                    ],
                    "fn": "-",
                    "type": "arithmetic"
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
                "fieldName": "!T_1",
                "type": "fieldAccess"
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
                "fieldName": "!T_0",
                "type": "fieldAccess"
              }
            ],
            "fn": "*",
            "name": "PriceAndTax",
            "type": "arithmetic"
          },
          {
            "name": "SixtySix",
            "type": "constant",
            "value": 66
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
          "fields": [
            {
              "dimension": "color",
              "type": "selector",
              "value": "D"
            },
            {
              "values": ["Good", "Bad", "Ugly"],
              "type": "in",
              "dimension": "tags"
            }
          ],
          "type": "and"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
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
              "values": ["Good", "Bad", "Ugly"],
              "type": "in",
              "dimension": "tags"
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
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "queryType": "timeseries",
        "context": {
          "skipEmptyBuckets": "true"
        }
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
            "function": "function(d){var _,_2;_=(_=(+d),(_==null?null:Math.floor(_ / 0.25) * 0.25));return isNaN(_)?null:_}",
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
              "value": "D"
            },
            {
              "values": ["Good", "Bad", "Ugly"],
              "type": "in",
              "dimension": "tags"
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
        "intervals": "2015-03-13T07Z/2015-03-14T07Z",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 3
      }
    ]);
  });

  it("works on OVERLAP (single value) filter", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.overlap(['D'])"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "type": "selector",
      "value": "D"
    });
  });

  it("works on OVERLAP (multi value) filter", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.overlap(['C', 'D'])"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "values": ["C", "D"],
      "type": "in",
      "dimension": "color"
    });
  });

  it("works on fancy filter .concat().match()", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("('A' ++ $color ++ 'Z').match('AB+')"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "format": "A%sZ",
        "nullHandling": "returnNull",
        "type": "stringFormat"
      },
      "pattern": "AB+",
      "type": "regex"
    });
  });

  it("works on fancy filter .concat().contains()", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("('A' ++ $color ++ 'Z').contains('AB')"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "format": "A%sZ",
        "nullHandling": "returnNull",
        "type": "stringFormat"
      },
      "query": {
        "caseSensitive": true,
        "type": "contains",
        "value": "AB"
      },
      "type": "search"
    });
  });

  it("works on fancy filter .fallback().is() [impossible]", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.fallback('NoColor') == 'D'"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "type": "selector",
      "value": "D"
    });
  });

  it("works on fancy filter .fallback().is() [possible]", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.fallback('D') == 'D'"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "lookup": {
          "map": {
            "": "D"
          },
          "type": "map"
        },
        "retainMissingValue": true,
        "type": "lookup"
      },
      "type": "selector",
      "value": "D"
    });
  });

  it("works on fancy filter .extract().is()", () => {
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
      "type": "selector",
      "value": "D"
    });
  });

  it("works on fancy filter .extract().fallback().is()", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.extract('^(.)').fallback('D') == 'D'"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "expr": "^(.)",
        "replaceMissingValue": true,
        "replaceMissingValueWith": "D",
        "type": "regex"
      },
      "type": "selector",
      "value": "D"
    });
  });

  it("works on fancy filter .substr().is()", () => {
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
      "type": "selector",
      "value": "D"
    });
  });

  it("works on fancy filter .substr().in()", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.substr(0, 1).in(['D', 'C'])"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "index": 0,
        "length": 1,
        "type": "substring"
      },
      "type": "in",
      "values": [
        "D",
        "C"
      ]
    });
  });

  it("works on fancy filter .lookup().in()", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.lookup('some_lookup').in(['D', 'C'])"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "lookup": "some_lookup",
        "type": "registeredLookup"
      },
      "type": "in",
      "values": [
        "D",
        "C"
      ]
    });
  });

  it("works on fancy filter .lookup().contains()", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.lookup('some_lookup').contains('hello')"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "lookup": "some_lookup",
        "type": "registeredLookup"
      },
      "query": {
        "caseSensitive": true,
        "type": "contains",
        "value": "hello"
      },
      "type": "search"
    });
  });

  it("works on fancy filter [.in(...).not()]", () => {
    var ex = $('diamonds').filter("$color.in(['D', 'C']).not()");

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "field": {
        "values": ["D", "C"],
        "type": "in",
        "dimension": "color"
      },
      "type": "not"
    });
  });

  it("works on fancy filter .in().is()", () => {
    var ex = $('diamonds').filter("$color.in(['D', 'C']) == true");

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "function": "function(d){var _,_2;return [\"D\",\"C\"].indexOf(d)>-1;}",
        "type": "javascript"
      },
      "type": "selector",
      "value": true
    });
  });

  it("works on cast number to string in filter", () => {
    var ex = $('diamonds').filter($('height_bucket').cast('STRING').is(r("15")));

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "height_bucket",
      "type": "selector",
      "value": "15"
    });
  });

  it("works on cast string to number in filter", () => {
    var ex = $('diamonds').filter($('height_bucket').absolute().cast('STRING').cast('NUMBER').is(r(555)));

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "height_bucket",
      "extractionFn": {
        "function": "function(d){var _,_2;_=+(('' + Math.abs((+d))));return isNaN(_)?null:_}",
        "type": "javascript"
      },
      "type": "selector",
      "value": 555
    });
  });

  it("works on cast number to time in split", () => {
    var ex = $('diamonds').split('$height_bucket.absolute().cast("STRING").cast("NUMBER").cast("TIME")', 'TaxCode');

    expect(ex.simulateQueryPlan(context)[0]).to.deep.equal({
      "aggregations": [
        {
          "name": "!DUMMY",
          "type": "count"
        }
      ],
      "dataSource": "diamonds",
      "dimensions": [
        {
          "dimension": "height_bucket",
          "extractionFn": {
            "function": "function(d){var _,_2;_=new Date(+(('' + Math.abs((+d)))));return isNaN(_)?null:_}",
            "type": "javascript"
          },
          "outputName": "TaxCode",
          "type": "extraction"
        }
      ],
      "granularity": "all",
      "intervals": "2015-03-12T00Z/2015-03-19T00Z",
      "limitSpec": {
        "columns": [
          {
            "dimension": "TaxCode"
          }
        ],
        "type": "default"
      },
      "queryType": "groupBy"
    });
  });

  it("works with timePart (with limit)", () => {
    var ex = ply()
      .apply(
        'HoursOfDay',
        $("diamonds").split("$time.timePart(HOUR_OF_DAY, 'Etc/UTC')", 'HourOfDay')
          .apply('TotalPrice', '$diamonds.sum($price)')
          .sort('$HourOfDay', 'ascending')
          .limit(20)
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
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": {
          "type": "alphaNumeric"
        },
        "queryType": "topN",
        "threshold": 20
      }
    ]);
  });

  it("works with timePart (no limit)", () => {
    var ex = ply()
      .apply(
        'HoursOfDay',
        $("diamonds").split("$time.timePart(HOUR_OF_DAY)", 'HourOfDay')
          .sort('$HourOfDay', 'ascending')
      )
      .apply(
        'SecondOfDay',
        $("diamonds").split("$time.timePart(SECOND_OF_DAY, 'Etc/UTC')", 'HourOfDay')
          .sort('$HourOfDay', 'ascending')
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
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": {
          "type": "alphaNumeric"
        },
        "queryType": "topN",
        "threshold": 1000
      },
      {
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimensions": [
          {
            "dimension": "__time",
            "extractionFn": {
              "function": "function(s){try{\nvar d = new org.joda.time.DateTime(s);\nd = d.getSecondOfDay();\nreturn d;\n}catch(e){return null;}}",
              "type": "javascript"
            },
            "outputName": "HourOfDay",
            "type": "extraction"
          }
        ],
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "limitSpec": {
          "columns": [
            {
              "dimension": "HourOfDay",
              "dimensionOrder": "alphanumeric",
              "direction": "ascending"
            }
          ],
          "type": "default"
        },
        "queryType": "groupBy"
      }
    ]);
  });

  it("works with basic concat", () => {
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
        "format": "!!!<%s>!!!",
        "nullHandling": "returnNull",
        "type": "stringFormat"
      },
      "outputName": "Colors",
      "type": "extraction"
    });
  });

  it("works with basic substr", () => {
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

  it("works with basic index of in split", () => {
    var ex = ply()
      .apply(
        'Colors',
        $("diamonds").split("$color.indexOf('p') != -1", 'Colors')
          .apply('TotalPrice', '$diamonds.sum($price)')
          .sort('$TotalPrice', 'descending')
          .limit(3)
      );

    expect(ex.simulateQueryPlan(context)[0].dimension).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "function": "function(d){var _,_2;return (_=d,(_==null)?null:((''+_).indexOf(\"p\")>-1));}",
        "type": "javascript"
      },
      "outputName": "Colors",
      "type": "extraction"
    });
  });

  it("works with basic index of in filter", () => {
    var ex = $("diamonds").filter("$color.indexOf('blah') != -1");

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "query": {
        "caseSensitive": true,
        "type": "contains",
        "value": "blah"
      },
      "type": "search"
    });
  });

  it("works with basic boolean split", () => {
    var ex = ply()
      .apply(
        'Colors',
        $("diamonds").split("$color == A", 'IsA')
          .apply('TotalPrice', '$diamonds.sum($price)')
          .sort('$TotalPrice', 'descending')
          .limit(10)
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
        "dataSource": "diamonds-compact",
        "dimension": {
          "dimension": "color",
          "extractionFn": {
            "function": "function(d){var _,_2;return (d===\"A\");}",
            "type": "javascript"
          },
          "outputName": "IsA",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": "TotalPrice",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("works with simple having filter", () => {
    var ex = $("diamonds").split("$cut", 'Cut')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending')
      .filter('$Count > 100')
      .limit(10);

    expect(ex.simulateQueryPlan(context)[0].having).to.deep.equal({
      "aggregation": "Count",
      "type": "greaterThan",
      "value": 100
    });
  });

  it("works with AND having filter", () => {
    var ex = $("diamonds").split("$cut", 'Cut')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending')
      .filter('$Count > 100 and $Count <= 200')
      .limit(10);

    expect(ex.simulateQueryPlan(context)[0].having).to.deep.equal({
      "havingSpecs": [
        {
          "aggregation": "Count",
          "type": "greaterThan",
          "value": 100
        },
        {
          "havingSpec": {
            "aggregation": "Count",
            "type": "greaterThan",
            "value": 200
          },
          "type": "not"
        }
      ],
      "type": "and"
    });
  });


  it("works with transform case", () => {
    var ex = $("diamonds").split("$cut.transformCase('upperCase')", 'Cut')
      .limit(10);

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "diamonds-compact",
        "dimension": {
          "dimension": "cut",
          "extractionFn": {
            "type": "upper"
          },
          "outputName": "Cut",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": {
          "type": "lexicographic"
        },
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("works with custom transform in filter and split", () => {
    var ex = $("diamonds")
      .filter($("cut").customTransform('sliceLastChar').is('z'))
      .split($("cut").customTransform('sliceLastChar').is('z'), 'lastChar')
      .limit(10);

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "diamonds-compact",
        "dimension": {
          "dimension": "cut",
          "extractionFn": {
            "function": "function(x) { return x.slice(-1) }",
            "type": "javascript"
          },
          "outputName": "lastChar",
          "type": "extraction"
        },
        "filter": {
          "dimension": "cut",
          "extractionFn": {
            "function": "function(x) { return x.slice(-1) }",
            "type": "javascript"
          },
          "type": "selector",
          "value": "z"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": {
          "type": "lexicographic"
        },
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("works with lower bound only time filter", () => {
    var ex = ply()
      .apply('diamonds', $("diamonds").filter($("time").in({ start: new Date('2015-03-12T00:00:00'), end: null })))
      .apply('Count', $('diamonds').count());

    expect(ex.simulateQueryPlan(contextUnfiltered)[0].intervals).to.equal("2015-03-12T00Z/3000");
  });

  it("works with upper bound only time filter", () => {
    var ex = ply()
      .apply('diamonds', $("diamonds").filter($("time").in({ start: null, end: new Date('2015-03-12T00:00:00') })))
      .apply('Count', $('diamonds').count());

    expect(ex.simulateQueryPlan(contextUnfiltered)[0].intervals).to.equal("1000/2015-03-12T00Z");
  });

  it("from parse should work with <= < of string and number", () => {
    var ex = Expression.parseSQL(`
        SELECT
        SUM(price) AS 'TotalPrice'
        FROM \`diamonds\`
        WHERE '2015-01-02T12:30:00' <= \`cut\` 
        AND '2015-01-01T10:30:00' > \`color\` 
        AND '2015-01-01T10:30:00' <= \`time\` AND \`time\` < '2015-01-02T12:30:00'
      `);

    expect(ex.expression.simulateQueryPlan(contextUnfiltered)).to.deep.equal([
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
              "dimension": "cut",
              "lower": "2015-01-02T12:30:00",
              "type": "bound"
            },
            {
              "dimension": "color",
              "type": "bound",
              "upper": "2015-01-01T10:30:00",
              "upperStrict": true
            }
          ],
          "type": "and"
        },
        "granularity": "all",
        "intervals": "2015-01-01T10:30Z/2015-01-02T12:30Z",
        "queryType": "timeseries"
      }
    ]);

  });

  it("Should be able to find column name case insensitively", () => {
    var ex = Expression.parseSQL(`
        SELECT
        SUM(prIcE) AS 'TotalPrice'
        FROM \`diamonds\`
        WHERE '2015-01-02T12:30:00' <= \`cut\` 
        AND '2015-01-01T10:30:00' > \`color\` 
        AND '2015-01-01T10:30:00' <= \`time\` AND \`time\` < '2015-01-02T12:30:00'
      `);


    expect(ex.expression.simulateQueryPlan(contextUnfiltered)).to.deep.equal([
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
              "dimension": "cut",
              "lower": "2015-01-02T12:30:00",
              "type": "bound"
            },
            {
              "dimension": "color",
              "type": "bound",
              "upper": "2015-01-01T10:30:00",
              "upperStrict": true
            }
          ],
          "type": "and"
        },
        "granularity": "all",
        "intervals": "2015-01-01T10:30Z/2015-01-02T12:30Z",
        "queryType": "timeseries"
      }
    ]);

  });

  it("works with numeric split", () => {
    var ex = ply()
      .apply(
        'CaratSplit',
        $("diamonds").split("$carat", 'Carat')
          .sort('$Carat', 'descending')
          .limit(10)
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
          "dimension": "carat",
          "outputName": "Carat",
          "type": "default"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": {
          "metric": {
            "type": "alphaNumeric"
          },
          "type": "inverted"
        },
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("works with set filter and split (and subsplit)", () => {
    var ex = ply()
      .apply("diamonds", $("diamonds").filter('$tags.overlap(["tagA", "tagB"])'))
      .apply(
        'Tags',
        $("diamonds").split("$tags", 'Tag')
          .sort('$Tag', 'descending')
          .limit(10)
          .apply(
            'Cuts',
            $("diamonds").split("$cut", 'Cut')
              .apply('Count', $('diamonds').count())
              .sort('$Count', 'descending')
              .limit(10)
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
          "dimension": "tags",
          "outputName": "Tag",
          "type": "default"
        },
        "filter": {
          "values": ["tagA", "tagB"],
          "type": "in",
          "dimension": "tags"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": {
          "metric": {
            "type": "lexicographic"
          },
          "type": "inverted"
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
        "dimension": {
          "dimension": "cut",
          "outputName": "Cut",
          "type": "default"
        },
        "filter": {
          "fields": [
            {
              "values": ["tagA", "tagB"],
              "type": "in",
              "dimension": "tags"
            },
            {
              "dimension": "tags",
              "type": "selector",
              "value": "some_tags"
            }
          ],
          "type": "and"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("works with BOOLEAN split", () => {
    var ex = $("diamonds").split("$isNice", 'IsNice')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending');

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
          "dimension": "isNice",
          "extractionFn": {
            "lookup": {
              "map": {
                "0": "false",
                "1": "true",
                "false": "false",
                "true": "true"
              },
              "type": "map"
            },
            "type": "lookup"
          },
          "outputName": "IsNice",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 1000
      }
    ]);
  });

  it("works with lookup split (and subsplit)", () => {
    var ex = $("diamonds").split("$tags.lookup(tag_lookup)", 'Tag')
      .sort('$Tag', 'descending')
      .limit(10)
      .apply(
        'Cuts',
        $("diamonds").split("$cut", 'Cut')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(10)
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
          "dimension": "tags",
          "extractionFn": {
            "lookup": "tag_lookup",
            "type": "registeredLookup"
          },
          "outputName": "Tag",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": {
          "metric": {
            "type": "lexicographic"
          },
          "type": "inverted"
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
        "dimension": {
          "dimension": "cut",
          "outputName": "Cut",
          "type": "default"
        },
        "filter": {
          "dimension": "tags",
          "extractionFn": {
            "lookup": "tag_lookup",
            "type": "registeredLookup"
          },
          "type": "selector",
          "value": "something"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("works with range bucket", () => {
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
          "dimension": "height_bucket",
          "outputName": "HeightBucket",
          "type": "default"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
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
            "function": "function(d){var _,_2;_=(_=(+d),(_==null?null:Math.floor((_ - 0.5) / 2) * 2 + 0.5));return isNaN(_)?null:_}",
            "type": "javascript"
          },
          "dimension": "height_bucket",
          "outputName": "HeightBucket",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("makes a timeBoundary query", () => {
    var ex = ply()
      .apply('maximumTime', '$diamonds.max($time)')
      .apply('minimumTime', '$diamonds.min($time)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "dataSource": "diamonds-compact",
        "queryType": "timeBoundary"
      }
    ]);
  });

  it("makes a timeBoundary query (maxTime only)", () => {
    var ex = ply()
      .apply('maximumTime', '$diamonds.max($time)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "dataSource": "diamonds-compact",
        "queryType": "timeBoundary",
        "bound": "maxTime"
      }
    ]);
  });

  it("makes a timeBoundary query (minTime only)", () => {
    var ex = ply()
      .apply('minimumTime', '$diamonds.min($time)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "dataSource": "diamonds-compact",
        "queryType": "timeBoundary",
        "bound": "minTime"
      }
    ]);
  });

  it("makes a topN with a timePart dim extraction fn", () => {
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
            "function": "function(s){try{\nvar d = new org.joda.time.DateTime(s);\nd = d.getSecondOfDay();\nreturn d;\n}catch(e){return null;}}",
            "type": "javascript"
          },
          "outputName": "Time",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("makes a min/max query on a numeric dimension", () => {
    var ex = ply()
      .apply('maxCarat', '$diamonds.max($carat)')
      .apply('minCarat', '$diamonds.min($carat)');

    expect(ex.simulateQueryPlan(context)[0].aggregations).to.deep.equal([
      {
        "fieldNames": [
          "carat"
        ],
        "fnAggregate": "function($$,_carat) { return Math.max($$,(+_carat)); }",
        "fnCombine": "function(a,b) { return Math.max(a,b); }",
        "fnReset": "function() { return -Infinity; }",
        "name": "maxCarat",
        "type": "javascript"
      },
      {
        "fieldNames": [
          "carat"
        ],
        "fnAggregate": "function($$,_carat) { return Math.min($$,(+_carat)); }",
        "fnCombine": "function(a,b) { return Math.min(a,b); }",
        "fnReset": "function() { return Infinity; }",
        "name": "minCarat",
        "type": "javascript"
      }
    ]);
  });

  it("makes a query on a reserved JS word", () => {
    var ex = ply()
      .apply('maxTry', '$diamonds.max($try)')
      .apply('maxA+B', '$diamonds.max(${a+b})');

    expect(ex.simulateQueryPlan(context)[0].aggregations).to.deep.equal([
      {
        "fieldNames": [
          "try"
        ],
        "fnAggregate": "function($$,_try) { return Math.max($$,(+_try)); }",
        "fnCombine": "function(a,b) { return Math.max(a,b); }",
        "fnReset": "function() { return -Infinity; }",
        "name": "maxTry",
        "type": "javascript"
      },
      {
        "fieldNames": [
          "a+b"
        ],
        "fnAggregate": "function($$,_a$43b) { return Math.max($$,(+_a$43b)); }",
        "fnCombine": "function(a,b) { return Math.max(a,b); }",
        "fnReset": "function() { return -Infinity; }",
        "name": "maxA+B",
        "type": "javascript"
      }
    ]);
  });

  it("makes a filtered aggregate query", () => {
    var ex = ply()
      .apply(
        'BySegment',
        $('diamonds').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeSegment')
          .apply('Total', $('diamonds').sum('$price'))
          .apply('GoodPrice', $('diamonds').filter($('cut').is('Good')).sum('$price'))
          .apply('GoodPrice2', $('diamonds').filter($('cut').is('Good')).sum('$price.power(2)'))
          .apply('GoodishPrice', $('diamonds').filter($('cut').contains('Good')).sum('$price'))
          .apply('NotBadColors', $('diamonds').filter($('cut').isnt('Bad')).countDistinct('$color'))
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
              "fieldNames": [
                "price"
              ],
              "fnAggregate": "function($$,_price) { return $$+Math.pow((+_price),2); }",
              "fnCombine": "function(a,b) { return a+b; }",
              "fnReset": "function() { return 0; }",
              "name": "GoodPrice2",
              "type": "javascript"
            },
            "filter": {
              "dimension": "cut",
              "type": "selector",
              "value": "Good"
            },
            "name": "GoodPrice2",
            "type": "filtered"
          },
          {
            "aggregator": {
              "fieldName": "price",
              "name": "GoodishPrice",
              "type": "doubleSum"
            },
            "filter": {
              "dimension": "cut",
              "query": {
                "caseSensitive": true,
                "type": "contains",
                "value": "Good"
              },
              "type": "search"
            },
            "name": "GoodishPrice",
            "type": "filtered"
          },
          {
            "aggregator": {
              "byRow": true,
              "fieldNames": [
                "color"
              ],
              "name": "NotBadColors",
              "type": "cardinality"
            },
            "filter": {
              "field": {
                "dimension": "cut",
                "type": "selector",
                "value": "Bad"
              },
              "type": "not"
            },
            "name": "NotBadColors",
            "type": "filtered"
          }
        ],
        "dataSource": "diamonds",
        "granularity": {
          "period": "PT1H",
          "timeZone": "Etc/UTC",
          "type": "period"
        },
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "queryType": "timeseries",
        "context": {
          "skipEmptyBuckets": "true"
        }
      }
    ]);
  });

  it("makes a filtered aggregate query", () => {
    var ex = ply()
      .apply(
        'BySegment',
        $('diamonds').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeSegment')
          .apply('Total', $('diamonds').sum('$price'))
          .apply('GoodPrice', $('diamonds').filter($('cut').is('Good')).sum(1))
      );

    expect(ex.simulateQueryPlan(context)[0].aggregations).to.deep.equal([
      {
        "fieldName": "price",
        "name": "Total",
        "type": "doubleSum"
      },
      {
        "aggregator": {
          "name": "GoodPrice",
          "type": "count"
        },
        "filter": {
          "dimension": "cut",
          "type": "selector",
          "value": "Good"
        },
        "name": "GoodPrice",
        "type": "filtered"
      }
    ]);
  });

  it("makes a filter on timePart", () => {
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
        "filter": {
          "dimension": "__time",
          "extractionFn": {
            "format": "H",
            "locale": "en-US",
            "timeZone": "Etc/UTC",
            "type": "timeFormat"
          },
          "type": "in",
          "values": [
            3,
            4,
            10
          ]
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12T00Z/2015-03-15T00Z",
          "2015-03-16T00Z/2015-03-18T00Z"
        ],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("splits on timePart with sub split", () => {
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
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
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
        "filter": {
          "dimension": "__time",
          "extractionFn": {
            "format": "H",
            "locale": "en-US",
            "timeZone": "Etc/UTC",
            "type": "timeFormat"
          },
          "type": "selector",
          "value": 4
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("splits on numberBucket with sub split", () => {
    var ex = $("diamonds").split($("carat").numberBucket(10), 'CaratB10')
      .apply('Count', '$diamonds.count()')
      .sort('$CaratB10', 'ascending')
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
          "dimension": "carat",
          "extractionFn": {
            "function": "function(d){var _,_2;_=(_=(+d),(_==null?null:Math.floor(_ / 10) * 10));return isNaN(_)?null:_}",
            "type": "javascript"
          },
          "outputName": "CaratB10",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": {
          "type": "alphaNumeric"
        },
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
        "filter": {
          "alphaNumeric": true,
          "dimension": "carat",
          "lower": 0,
          "type": "bound",
          "upper": 10,
          "upperStrict": true
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("works without a sort defined", () => {
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
        "dataSource": "diamonds-compact",
        "dimension": {
          "dimension": "color",
          "outputName": "Color",
          "type": "default"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
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
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "queryType": "timeseries",
        "context": {
          "skipEmptyBuckets": "true"
        }
      }
    ]);
  });

  it("works with no attributes in dimension split dataset", () => {
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
        "dataSource": "diamonds-compact",
        "dimension": {
          "dimension": "cut",
          "outputName": "Cut",
          "type": "default"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": {
          "type": "lexicographic"
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
        "dataSource": "diamonds-compact",
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
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 3
      }
    ]);
  });

  it("works with no attributes in time split dataset", () => {
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
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "queryType": "timeseries",
        "context": {
          "skipEmptyBuckets": "true"
        }
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
        "intervals": "2015-03-14T00Z/2015-03-14T01Z",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 3
      }
    ]);
  });

  it("inlines a defined derived attribute", () => {
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
            "name": "!T_0",
            "type": "doubleSum"
          },
          {
            "fieldName": "tax",
            "name": "!T_1",
            "type": "doubleSum"
          }
        ],
        "dataSource": "diamonds",
        "granularity": {
          "period": "P1D",
          "timeZone": "Etc/UTC",
          "type": "period"
        },
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
            "fn": "+",
            "name": "TotalSalePrice",
            "type": "arithmetic"
          }
        ],
        "queryType": "timeseries",
        "context": {
          "skipEmptyBuckets": "true"
        }
      }
    ]);
  });

  it("makes a query on a dataset with a fancy name", () => {
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

  it("makes a query with countDistinct", () => {
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
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
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

  it("works with duplicate aggregates", () => {
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
            "name": "!T_0",
            "type": "count"
          }
        ],
        "dataSource": "diamonds-compact",
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

  it("works with binary JS aggregate", () => {
    var ex = ply()
      .apply('Thing', '$diamonds.sum($price.absolute() * $carat.power(2))');

    expect(ex.simulateQueryPlan(context)[0].aggregations[0]).to.deep.equal({
      "fieldNames": [
        "carat",
        "price"
      ],
      "fnAggregate": "function($$,_carat,_price) { return $$+(Math.abs((+_price))*Math.pow((+_carat),2)); }",
      "fnCombine": "function(a,b) { return a+b; }",
      "fnReset": "function() { return 0; }",
      "name": "Thing",
      "type": "javascript"
    });
  });

  it("works on exact time filter (is)", () => {
    var ex = ply()
      .apply('diamonds', $('diamonds').filter($('time').is(new Date('2015-03-12T01:00:00.123Z'))))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].intervals).to.equal(
      "2015-03-12T01:00:00.123Z/2015-03-12T01:00:00.124Z"
    );
  });

  it("works on exact time filter (in interval)", () => {
    var ex = ply()
      .apply('diamonds', $('diamonds').filter($('time').in(new Date('2015-03-12T01:00:00.123Z'), new Date('2015-03-12T01:00:00.124Z'))))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].intervals).to.equal(
      "2015-03-12T01:00:00.123Z/2015-03-12T01:00:00.124Z"
    );
  });

  it("works contains filter (case sensitive)", () => {
    var ex = ply()
      .apply('diamonds', $('diamonds').filter($('color').contains(r('sup"yo'))))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "query": {
        "caseSensitive": true,
        "type": "contains",
        "value": 'sup"yo'
      },
      "type": "search"
    });
  });

  it("works contains filter (case insensitive)", () => {
    var ex = ply()
      .apply('diamonds', $('diamonds').filter($('color').contains(r('sup"yo'), 'ignoreCase')))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "query": {
        "caseSensitive": false,
        "type": "contains",
        "value": 'sup"yo'
      },
      "type": "search"
    });
  });

  it("works with SELECT query", () => {
    var ex = $('diamonds')
      .filter('$color == "D"')
      .limit(10);

    var queryPlan = ex.simulateQueryPlan(context);

    expect(queryPlan).to.deep.equal([
      {
        "dataSource": "diamonds",
        "dimensions": [
          "color",
          "cut",
          "isNice",
          "tags",
          "carat",
          "height_bucket",
          "try",
          "a+b"
        ],
        "filter": {
          "dimension": "color",
          "type": "selector",
          "value": "D"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "metrics": [
          "price",
          "tax",
          "vendor_id"
        ],
        "pagingSpec": {
          "pagingIdentifiers": {},
          "threshold": 10
        },
        "queryType": "select"
      }
    ]);
  });

  it("works with single split expression", () => {
    var ex = $("diamonds").split("$cut", 'Cut');

    var queryPlan = ex.simulateQueryPlan(context);

    expect(queryPlan).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "diamonds-compact",
        "dimensions": [
          {
            "dimension": "cut",
            "outputName": "Cut",
            "type": "default"
          }
        ],
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "limitSpec": {
          "columns": [
            { "dimension": "Cut" }
          ],
          "type": "default"
        },
        "queryType": "groupBy"
      }
    ]);
  });

  it("works multi-dimensional GROUP BYs", () => {
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
          "values": ["A", "B", "some_color"],
          "type": "in",
          "dimension": "color"
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "limitSpec": {
          "columns": [
            { "dimension": "Color" }
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
            "function": "function(d){var _,_2;_=(_=(+d),(_==null?null:Math.floor(_ / 0.25) * 0.25));return isNaN(_)?null:_}",
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
        "intervals": "2015-03-14T00Z/2015-03-14T01Z",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 3
      }
    ]);
  });

  it("works multi-dimensional GROUP BYs (no limit)", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").in(['A', 'B', 'some_color'])))
      .apply(
        'Cuts',
        $("diamonds").split({
            'Cut': "$cut",
            'Color': '$color',
            'ATimeByHour': '$time.timeBucket(PT1H, "Etc/UTC")'
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
          "values": ["A", "B", "some_color"],
          "type": "in",
          "dimension": "color"
        },
        "granularity": {
          "period": "PT1H",
          "timeZone": "Etc/UTC",
          "type": "period"
        },
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "limitSpec": {
          "columns": [
            { "dimension": "Color" }
          ],
          "type": "default"
        },
        "queryType": "groupBy"
      }
    ]);
  });

  it("works nested GROUP BYs", () => {
    var ex = $("diamonds")
      .filter($("color").in(['A', 'B', 'some_color']))
      .split({ 'Cut': "$cut", 'Color': '$color' })
      .apply('Count', $('diamonds').count())
      .split('$Cut', 'Cut', 'data')
      .apply('MaxCount', '$data.max($Count)');

    var queryPlan = ex.simulateQueryPlan(context);

    expect(queryPlan).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds-compact",
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
          "dimension": "color",
          "type": "in",
          "values": [
            "A",
            "B",
            "some_color"
          ]
        },
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "limitSpec": {
          "columns": [
            {
              "dimension": "Color"
            }
          ],
          "type": "default"
        },
        "queryType": "groupBy"
      }
    ]);
  });

  it("adds context to query if set on External", (testComplete) => {
    var ds = External.fromJS({
      engine: 'druid',
      source: 'diamonds',
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

    testComplete();
  });

});
