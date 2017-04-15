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
    version: '0.9.2',
    source: 'diamonds',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true,
    filter: $("time").overlap({
      start: new Date('2015-03-12T00:00:00'),
      end: new Date('2015-03-19T00:00:00')
    })
  })
};

describe("simulate Druid 0.9.2", () => {
  it("works with simple having filter", () => {
    let ex = $("diamonds").split("$cut", 'Cut')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending')
      .filter('$Count > 100')
      .limit(10);

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].having).to.deep.equal({
      "aggregation": "Count",
      "type": "greaterThan",
      "value": 100
    });
  });

  it("works with AND having filter", () => {
    let ex = $("diamonds").split("$cut", 'Cut')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending')
      .filter('$Count > 100 and $Count <= 200')
      .limit(10);

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0][0].having).to.deep.equal({
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

  it("works with multi-value, multi-dim dimension regexp having filter and extra having filters", () => {
    let ex = $("diamonds")
      .filter('$tags.match("[ab]+")')
      .split({ Tag: "$tags", Cut: "$cut" })
      .apply('Count', '$diamonds.count()')
      .filter('$Tag.match("a+") and $Count > 100')
      .limit(10);

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0][0]).to.deep.equal({
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
        },
        {
          "delegate": {
            "dimension": "tags",
            "outputName": "Tag",
            "type": "default"
          },
          "pattern": "a+",
          "type": "regexFiltered"
        }
      ],
      "filter": {
        "dimension": "tags",
        "pattern": "[ab]+",
        "type": "regex"
      },
      "granularity": "all",
      "having": {
        "aggregation": "Count",
        "type": "greaterThan",
        "value": 100
      },
      "intervals": "2015-03-12T00Z/2015-03-19T00Z",
      "limitSpec": {
        "columns": [
          {
            "dimension": "Cut"
          }
        ],
        "limit": 10,
        "type": "default"
      },
      "queryType": "groupBy"
    });
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
              "format": "yyyy-MM-dd'T'HH:mm:ss'Z",
              "granularity": {
                "period": "P1D",
                "timeZone": "Etc/UTC",
                "type": "period"
              },
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
              "format": "yyyy-MM-dd'T'HH:mm:ss'Z",
              "granularity": {
                "period": "P1D",
                "timeZone": "Etc/UTC",
                "type": "period"
              },
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
