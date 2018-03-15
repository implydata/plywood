/*
 * Copyright 2015-2018 Imply Data, Inc.
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
  { name: 'some_other_time', type: 'TIME' },
  { name: 'color', type: 'STRING' },
  { name: 'cut', type: 'STRING' },
  { name: 'isNice', type: 'BOOLEAN' },
  { name: 'tags', type: 'SET/STRING' },
  { name: 'carat', type: 'NUMBER', nativeType: 'STRING' },
  { name: 'height_bucket', type: 'NUMBER' },
  { name: 'price', type: 'NUMBER', unsplitable: true },
  { name: 'tax', type: 'NUMBER', unsplitable: true },
  { name: 'vendor_id', type: 'NULL', nativeType: 'hyperUnique', unsplitable: true },
];

let context = {
  'diamonds': External.fromJS({
    engine: 'druid',
    version: '0.9.1',
    source: 'diamonds',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true,
    filter: $("time").overlap({
      start: new Date('2015-03-12T00:00:00Z'),
      end: new Date('2015-03-19T00:00:00Z')
    })
  })
};

describe("simulate Druid 0.9.1", () => {

  it("makes a filter on timePart", () => {
    let ex = $("diamonds").filter(
      $("time").timePart('HOUR_OF_DAY', 'Etc/UTC').is([3, 4, 10]).and($("time").overlap([
        TimeRange.fromJS({ start: new Date('2015-03-12T00:00:00Z'), end: new Date('2015-03-15T00:00:00Z') }),
        TimeRange.fromJS({ start: new Date('2015-03-16T00:00:00Z'), end: new Date('2015-03-18T00:00:00Z') })
      ]))
    ).split("$color", 'Color')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending')
      .limit(10);

    expect(() => {
      ex.simulateQueryPlan(context)
    }).to.throw('can not do secondary filtering on primary time dimension (https://github.com/druid-io/druid/issues/2816)');
  });

  it("splits on timePart with sub split", () => {
    let ex = $("diamonds").split($("time").timePart('HOUR_OF_DAY', 'Etc/UTC'), 'hourOfDay')
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

    expect(() => {
      ex.simulateQueryPlan(context)
    }).to.throw('can not do secondary filtering on primary time dimension (https://github.com/druid-io/druid/issues/2816)');
  });

  it("works with range bucket", () => {
    let ex = ply()
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

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0]).to.deep.equal([
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
            "function": "function(d){var _,_2;_=(_=parseFloat(d),(_==null?null:Math.floor((_ - 0.5) / 2) * 2 + 0.5));return isNaN(_)?null:_}",
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

  it("works with numeric split", () => {
    let ex = ply()
      .apply(
        'CaratSplit',
        $("diamonds").split("$carat", 'Carat')
          .sort('$Carat', 'descending')
          .limit(10)
      );

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
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

  it("works with timePart (no limit)", () => {
    let ex = ply()
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

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
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

  it("works on negative range", () => {
    let ex = ply()
      .apply('diamonds', $('diamonds').filter('-10 <= $carat'))
      .apply('Count', '$diamonds.count()');

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0][0].filter).to.deep.equal({
      "dimension": "carat",
      "function": "function(d){var _,_2;return ((_=parseFloat(d)),-10<=_);}",
      "type": "javascript"
    });
  });

  it("works multi-dimensional GROUP BYs", () => {
    let ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").overlap(['A', 'B', 'some_color'])))
      .apply(
        'Cuts',
        $("diamonds").split({
          'Cut': "$cut",
          'Color': '$color',
          'TimeByHour': '$time.timeBucket(PT1H, "Etc/UTC")'
        })
          .apply('Count', $('diamonds').count())
          .limit(3)
      );

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0]).to.deep.equal([
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
              "format": "yyyy-MM-dd'T'HH':00'Z",
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
    ]);
  });

  it("works with multi time column split", () => {
    let ex = ply()
      .apply(
        'SecondOfDay',
        $("diamonds").split({ t1: "$time.timeFloor('P1D')", t2: "$some_other_time.timeFloor('P1D')" })
          .apply('TotalPrice', '$diamonds.sum($price)')
          .sort('$TotalPrice', 'descending')
          .limit(3)
      );

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0]).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "price",
            "name": "TotalPrice",
            "type": "doubleSum"
          }
        ],
        "dataSource": "diamonds",
        "dimensions": [
          {
            "dimension": "__time",
            "extractionFn": {
              "format": "yyyy-MM-dd'T00:00'Z",
              "locale": "en-US",
              "timeZone": "Etc/UTC",
              "type": "timeFormat"
            },
            "outputName": "t1",
            "type": "extraction"
          },
          {
            "dimension": "some_other_time",
            "extractionFn": {
              "format": "yyyy-MM-dd'T00:00'Z",
              "locale": "en-US",
              "timeZone": "Etc/UTC",
              "type": "timeFormat"
            },
            "outputName": "t2",
            "type": "extraction"
          }
        ],
        "granularity": "all",
        "intervals": "2015-03-12T00Z/2015-03-19T00Z",
        "limitSpec": {
          "columns": [
            {
              "dimension": "TotalPrice",
              "direction": "descending"
            }
          ],
          "limit": 3,
          "type": "default"
        },
        "queryType": "groupBy"
      }
    ]);
  });

});
