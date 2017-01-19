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

const { expect } = require("chai");

let plywood = require('../plywood');
let { Expression, External, Dataset, TimeRange, $, ply, r } = plywood;

let attributes = [
  { name: 'time', type: 'TIME' },
  { name: 'color', type: 'STRING' },
  { name: 'cut', type: 'STRING' },
  { name: 'isNice', type: 'BOOLEAN' },
  { name: 'tags', type: 'SET/STRING' },
  { name: 'carat', type: 'NUMBER' },
  { name: 'height_bucket', type: 'NUMBER' },
  { name: 'price', type: 'NUMBER', unsplitable: true },
  { name: 'tax', type: 'NUMBER', unsplitable: true },
  { name: 'vendor_id', special: 'unique', unsplitable: true }
];

let context = {
  'diamonds': External.fromJS({
    engine: 'druid',
    version: '0.9.0',
    source: 'diamonds',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true,
    filter: $("time").in({
      start: new Date('2015-03-12T00:00:00'),
      end: new Date('2015-03-19T00:00:00')
    })
  })
};


describe("simulate Druid 0.9.0", () => {

  it("works on fancy filter .concat().match()", () => {
    let ex = ply()
      .apply("diamonds", $('diamonds').filter("('A' ++ $color ++ 'Z').match('AB+')"))
      .apply('Count', '$diamonds.count()');

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "function": "function(d){var _,_2;return /AB+/.test((_=(_=d,(_==null)?null:(\"A\"+_)),(_==null)?null:(_+\"Z\")));}",
        "type": "javascript"
      },
      "type": "extraction",
      "value": "true"
    });
  });

  it("works on fancy filter .concat().contains()", () => {
    let ex = ply()
      .apply("diamonds", $('diamonds').filter("('A' ++ $color ++ 'Z').contains('AB')"))
      .apply('Count', '$diamonds.count()');

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "function": "function(d){var _,_2;return (_=(_=(_=d,(_==null)?null:(\"A\"+_)),(_==null)?null:(_+\"Z\")),(_==null)?null:((''+_).indexOf(\"AB\")>-1));}",
        "type": "javascript"
      },
      "type": "extraction",
      "value": "true"
    });
  });

  it("works on fancy filter .fallback().is() [impossible]", () => {
    let ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.fallback('NoColor') == 'D'"))
      .apply('Count', '$diamonds.count()');

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].filter).to.deep.equal({
      "dimension": "color",
      "type": "selector",
      "value": "D"
    });
  });

  it("works on fancy filter .fallback().is() [possible]", () => {
    let ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.fallback('D') == 'D'"))
      .apply('Count', '$diamonds.count()');

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].filter).to.deep.equal({
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
      "type": "extraction",
      "value": "D"
    });
  });

  it("works on fancy filter .extract().is()", () => {
    let ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.extract('^(.)') == 'D'"))
      .apply('Count', '$diamonds.count()');

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].filter).to.deep.equal({
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

  it("works on fancy filter .extract().fallback().is()", () => {
    let ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.extract('^(.)').fallback('D') == 'D'"))
      .apply('Count', '$diamonds.count()');

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "expr": "^(.)",
        "replaceMissingValue": true,
        "replaceMissingValueWith": "D",
        "type": "regex"
      },
      "type": "extraction",
      "value": "D"
    });
  });

  it("works on fancy filter .substr().is()", () => {
    let ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.substr(0, 1) == 'D'"))
      .apply('Count', '$diamonds.count()');

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].filter).to.deep.equal({
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

  it("works on fancy filter .substr().in()", () => {
    let ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.substr(0, 1).in(['D', 'C'])"))
      .apply('Count', '$diamonds.count()');

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].filter).to.deep.equal({
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

  it("works on fancy filter .lookup().in()", () => {
    let ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.lookup('some_lookup').in(['D', 'C'])"))
      .apply('Count', '$diamonds.count()');

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].filter).to.deep.equal({
      "fields": [
        {
          "dimension": "color",
          "extractionFn": {
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

  it("works on fancy filter .lookup().contains()", () => {
    let ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.lookup('some_lookup').contains('hello')"))
      .apply('Count', '$diamonds.count()');

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "extractionFns": [
          {
            "lookup": {
              "namespace": "some_lookup",
              "type": "namespace"
            },
            "type": "lookup"
          },
          {
            "function": "function(d){var _,_2;return (_=d,(_==null)?null:((''+_).indexOf(\"hello\")>-1));}",
            "type": "javascript"
          }
        ],
        "type": "cascade"
      },
      "type": "extraction",
      "value": "true"
    });
  });

  it("works on fancy filter [.in(...).not()]", () => {
    let ex = $('diamonds').filter("$color.in(['D', 'C']).not()");

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].filter).to.deep.equal({
      "field": {
        "values": ["D", "C"],
        "type": "in",
        "dimension": "color"
      },
      "type": "not"
    });
  });

  it("works on fancy filter .in().is()", () => {
    let ex = $('diamonds').filter("$color.in(['D', 'C']) == true");

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "function": "function(d){var _,_2;return [\"D\",\"C\"].indexOf(d)>-1;}",
        "type": "javascript"
      },
      "type": "extraction",
      "value": true
    });
  });

  it("works with lookup split (and subsplit)", () => {
    let ex = $("diamonds").split("$tags.lookup(tag_lookup)", 'Tag')
      .sort('$Tag', 'descending')
      .limit(10)
      .apply(
        'Cuts',
        $("diamonds").split("$cut", 'Cut')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(10)
      );

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(2);
    expect(queryPlan[0]).to.deep.equal([
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
            "lookup": {
              "namespace": "tag_lookup",
              "type": "namespace"
            },
            "type": "lookup"
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
      }
    ]);

    expect(queryPlan[1]).to.deep.equal([
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
            "lookup": {
              "namespace": "tag_lookup",
              "type": "namespace"
            },
            "type": "lookup"
          },
          "type": "extraction",
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

  it("makes a filtered aggregate query", () => {
    let ex = ply()
      .apply(
        'BySegment',
        $('diamonds').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeSegment')
          .apply('Total', $('diamonds').sum('$price'))
          .apply('GoodPrice', $('diamonds').filter($('cut').is('Good')).sum('$price'))
          .apply('GoodPrice2', $('diamonds').filter($('cut').is('Good')).sum('$price.power(2)'))
          .apply('GoodishPrice', $('diamonds').filter($('cut').contains('Good')).sum('$price'))
          .apply('NotBadColors', $('diamonds').filter($('cut').isnt('Bad')).countDistinct('$color'))
      );

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0]).to.deep.equal([
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
              "fnAggregate": "function($$,_price) { return $$+Math.pow(parseFloat(_price),2); }",
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
              "extractionFn": {
                "function": "function(d){var _,_2;return (_=d,(_==null)?null:((''+_).indexOf(\"Good\")>-1));}",
                "type": "javascript"
              },
              "type": "extraction",
              "value": "true"
            },
            "name": "GoodishPrice",
            "type": "filtered"
          },
          {
            "aggregator": {
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

  it("makes a filtered aggregate query 2", () => {
    let ex = ply()
      .apply(
        'BySegment',
        $('diamonds').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeSegment')
          .apply('Total', $('diamonds').sum('$price'))
          .apply('GoodPrice', $('diamonds').filter($('cut').is('Good')).sum(1))
      );

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].aggregations).to.deep.equal([
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

});
