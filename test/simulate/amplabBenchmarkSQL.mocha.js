/*
 * Copyright 2012-2015 Metamarkets Group Inc.
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
let { Expression, External, TimeRange, $, i$, ply, r } = plywood;

let context = {
  rankings: External.fromJS({
    engine: 'druid',
    version: '0.10.0',
    source: 'rankings',
    timeAttribute: 'time',
    allowSelectQueries: true,
    allowEternity: true,
    attributes: [
      { name: 'pageURL', type: 'STRING', nativeType: 'STRING' }, // VARCHAR(300)
      { name: 'pageRank', type: 'NUMBER', nativeType: 'LONG' }, // INT
      { name: 'avgDuration', type: 'NUMBER', nativeType: 'LONG' } // INT
    ]
  }),
  uservisits: External.fromJS({
    engine: 'druid',
    version: '0.10.0',
    source: 'uservisits',
    timeAttribute: 'visitDate',
    allowSelectQueries: true,
    allowEternity: true,
    attributes: [
      { name: 'sourceIP', type: 'STRING', nativeType: 'STRING' }, // VARCHAR(116)
      { name: 'destURL', type: 'STRING', nativeType: 'STRING' }, // VARCHAR(100)
      { name: 'visitDate', type: 'TIME', nativeType: 'LONG' }, // DATE
      { name: 'adRevenue', type: 'NUMBER', nativeType: 'FLOAT' }, // FLOAT
      { name: 'userAgent', type: 'STRING', nativeType: 'STRING' }, // VARCHAR(256)
      { name: 'countryCode', type: 'STRING', nativeType: 'STRING' }, // CHAR(3)
      { name: 'languageCode', type: 'STRING', nativeType: 'STRING' }, // CHAR(6)
      { name: 'searchWord', type: 'STRING', nativeType: 'STRING' }, // VARCHAR(32)
      { name: 'duration', type: 'NUMBER', nativeType: 'FLOAT' } // INT
    ]
  })
};

// https://amplab.cs.berkeley.edu/benchmark/
describe("simulate Druid for amplab benchmark", () => {
  it("works for Query1", () => {
    //         SELECT pageURL, pageRank FROM rankings WHERE pageRank > X
    let sql = 'SELECT pageURL, pageRank FROM rankings WHERE pageRank > 5';
    let ex = Expression.parseSQL(sql).expression;

    expect(ex.toJS()).to.deep.equal(
      $('rankings')
        .filter('i$pageRank > 5')
        .apply('pageURL', 'i$pageURL')
        .apply('pageRank', 'i$pageRank')
        .select('pageURL', 'pageRank')
        .toJS()
    );

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0]).to.deep.equal([
      {
        "dataSource": "rankings",
        "dimensions": [
          "pageURL"
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
          "pageRank"
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
    let sql = 'SELECT pageURL, sum(pageRank) AS pageRank FROM rankings GROUP BY pageURL HAVING pageRank > 5';
    let ex = Expression.parseSQL(sql).expression;

    expect(ex.toJS()).to.deep.equal(
      $('rankings').split('i$pageURL', 'pageURL', 'data')
        .apply('pageRank', '$data.sum(i$pageRank)')
        .select('pageURL', 'pageRank')
        .filter('i$pageRank > 5')
        .toJS()
    );

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0]).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "pageRank",
            "name": "pageRank",
            "type": "longSum"
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
    let sql = 'SELECT SUBSTR(sourceIP, 1, 5), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, 5)';
    let ex = Expression.parseSQL(sql).expression;

    expect(ex.toJS()).to.deep.equal(
      $('uservisits').split('i$sourceIP.substr(1, 5)', 'SUBSTR(sourceIP, 1, 5)', 'data')
        .apply('SUM(adRevenue)', '$data.sum(i$adRevenue)')
        .select('SUBSTR(sourceIP, 1, 5)', 'SUM(adRevenue)')
        .toJS()
    );

    let queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan[0]).to.deep.equal([
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
