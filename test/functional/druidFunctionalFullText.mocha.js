/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2020 Imply Data, Inc.
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
const { Duration } = require("chronoshift");
let { sane } = require('../utils');

let { druidRequesterFactory } = require('plywood-druid-requester');

let plywood = require('../plywood');
let { External, DruidExternal, TimeRange, $, i$, ply, r, basicExecutorFactory, verboseRequesterFactory, Expression } = plywood;

let info = require('../info');

let druidRequester = druidRequesterFactory({
  host: info.druidHost
});

// druidRequester = verboseRequesterFactory({
//   requester: druidRequester
// });

describe("Druid Functional (Full Text)", function() {
  this.timeout(10000);

  let wikiAttributes = [
    {
      "name": "time",
      "nativeType": "__time",
      "range": {
        "bounds": "[]",
        "end": new Date('2015-09-12T23:59:00.000Z'),
        "start": new Date('2015-09-12T00:46:00.000Z')
      },
      "type": "TIME"
    },
    {
      "maker": {
        "expression": {
          "name": "added",
          "op": "ref"
        },
        "op": "sum"
      },
      "name": "added",
      "nativeType": "LONG",
      "type": "NUMBER",
      "unsplitable": true
    },
    {
      "cardinality": 52,
      "name": "channel",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "zh",
        "start": "ar"
      },
      "type": "STRING"
    },
    {
      "cardinality": 3719,
      "name": "cityName",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "Ōita",
        "start": ""
      },
      "type": "STRING"
    },
    {
      "cardinality": 138678,
      "name": "comment",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "ｒｖ",
        "start": "!"
      },
      "type": "STRING",
      "termsDelegate": "commentTerms"
    },
    {
      "name": "commentLength",
      "nativeType": "LONG",
      "type": "NUMBER"
    },
    {
      "cardinality": 255,
      "name": "commentLengthStr",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "99",
        "start": "1"
      },
      "type": "STRING"
    },
    {
      "maker": {
        "op": "count"
      },
      "name": "count",
      "nativeType": "LONG",
      "type": "NUMBER",
      "unsplitable": true
    },
    {
      "cardinality": 157,
      "name": "countryIsoCode",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "ZW",
        "start": ""
      },
      "type": "STRING"
    },
    {
      "cardinality": 157,
      "name": "countryName",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "Zimbabwe",
        "start": ""
      },
      "type": "STRING"
    },
    {
      "maker": {
        "expression": {
          "name": "deleted",
          "op": "ref"
        },
        "op": "sum"
      },
      "name": "deleted",
      "nativeType": "LONG",
      "type": "NUMBER",
      "unsplitable": true
    },
    {
      "maker": {
        "expression": {
          "name": "delta",
          "op": "ref"
        },
        "op": "sum"
      },
      "name": "delta",
      "nativeType": "LONG",
      "type": "NUMBER",
      "unsplitable": true
    },
    {
      "name": "deltaBucket100",
      "nativeType": "FLOAT",
      "type": "NUMBER"
    },
    {
      "maker": {
        "expression": {
          "name": "deltaByTen",
          "op": "ref"
        },
        "op": "sum"
      },
      "name": "deltaByTen",
      "nativeType": "DOUBLE",
      "type": "NUMBER",
      "unsplitable": true
    },
    {
      "name": "delta_hist",
      "nativeType": "approximateHistogram",
      "type": "NULL",
      "unsplitable": true
    },
    {
      "name": "isAnonymous",
      "type": "BOOLEAN"
    },
    {
      "name": "isMinor",
      "type": "BOOLEAN"
    },
    {
      "name": "isNew",
      "type": "BOOLEAN"
    },
    {
      "name": "isRobot",
      "type": "BOOLEAN"
    },
    {
      "name": "isUnpatrolled",
      "type": "BOOLEAN"
    },
    {
      "maker": {
        "expression": {
          "name": "max_delta",
          "op": "ref"
        },
        "op": "max"
      },
      "name": "max_delta",
      "nativeType": "LONG",
      "type": "NUMBER",
      "unsplitable": true
    },
    {
      "cardinality": 167,
      "name": "metroCode",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "881",
        "start": ""
      },
      "type": "STRING"
    },
    {
      "maker": {
        "expression": {
          "name": "min_delta",
          "op": "ref"
        },
        "op": "min"
      },
      "name": "min_delta",
      "nativeType": "LONG",
      "type": "NUMBER",
      "unsplitable": true
    },
    {
      "cardinality": 416,
      "name": "namespace",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "파일",
        "start": "2"
      },
      "type": "STRING"
    },
    {
      "cardinality": 279893,
      "name": "page",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "［Alexandros］",
        "start": "!T.O.O.H.!"
      },
      "type": "STRING"
    },
    {
      "name": "page_unique",
      "nativeType": "hyperUnique",
      "type": "NULL",
      "unsplitable": true
    },
    {
      "cardinality": 671,
      "name": "regionIsoCode",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "null",
        "start": ""
      },
      "type": "STRING"
    },
    {
      "cardinality": 1068,
      "name": "regionName",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "Świętokrzyskie",
        "start": ""
      },
      "type": "STRING"
    },
    {
      "name": "sometimeLater",
      "type": "TIME"
    },
    {
      "name": "sometimeLaterMs",
      "nativeType": "LONG",
      "type": "NUMBER"
    },
    {
      "cardinality": 38234,
      "name": "user",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "ＫＡＺＵ",
        "start": "! Bikkit !"
      },
      "type": "STRING"
    },
    {
      "cardinality": 1401,
      "name": "userChars",
      "nativeType": "STRING",
      "range": {
        "bounds": "[]",
        "end": "～",
        "start": " "
      },
      "type": "SET/STRING"
    },
    {
      "name": "user_theta",
      "nativeType": "thetaSketch",
      "type": "NULL",
      "unsplitable": true
    },
    {
      "name": "user_unique",
      "nativeType": "hyperUnique",
      "type": "NULL",
      "unsplitable": true
    }
  ];

  describe("defined attributes in datasource", () => {
    let wiki = External.fromJS({
      engine: 'druid',
      source: 'wikipedia',
      timeAttribute: 'time',
      context: info.druidContext,
      attributes: wikiAttributes,
      filter: $('time').overlap(new Date("2015-09-12T00:00:00Z"), new Date("2015-09-13T00:00:00Z")),
      version: info.druidVersion,
      allowSelectQueries: true
    }, druidRequester);

    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: wiki
      }
    });

    it("works basic case", () => {
      let ex = $('wiki')
        .filter($("comment").contains(r('adding employer')))
        .split($("comment"), "Comment")
        .apply('Count', '$wiki.sum($count)')
        .sort("$Count", 'descending')
        .limit(7);

      return basicExecutor(ex)
        .then((result) => {
          expect(result.toJS().data).to.deep.equal([
            {
              "Comment": "/* Adding parameters: employer, client, affiliation */",
              "Count": 18
            },
            {
              "Comment": "/* Adding parameters: employer, client, affiliation */ re",
              "Count": 3
            },
            {
              "Comment": "/* Adding parameters: employer, client, affiliation */ c",
              "Count": 2
            },
            {
              "Comment": "/* Adding parameters: employer, client, affiliation */ Fixing style/layout errors",
              "Count": 1
            },
            {
              "Comment": "/* Adding parameters: employer, client, affiliation */ new section",
              "Count": 1
            },
            {
              "Comment": "/* Adding parameters: employer, client, affiliation */ subst",
              "Count": 1
            },
            {
              "Comment": "/* Adding parameters: employer, client, affiliation */ t",
              "Count": 1
            }
          ]);
        });
    });

  });

});
