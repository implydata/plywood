/*
 * Copyright 2017-2017 Imply Data, Inc.
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
const toArray = require('stream-to-array');
let { sane } = require('../utils');

let { druidRequesterFactory } = require('plywood-druid-requester');

let plywood = require('../plywood');
let { External, DruidExternal, TimeRange, $, i$, ply, basicExecutorFactory, verboseRequesterFactory, Expression } = plywood;

let info = require('../info');

let druidRequester = druidRequesterFactory({
  host: info.druidHost
});

// druidRequester = verboseRequesterFactory({
//   requester: druidRequester
// });

describe("Streaming Functional", function() {
  this.timeout(10000);

  let wikiAttributes = [
    { "name": "time", "type": "TIME" },
    { "name": "added", "maker":{"op": "sum", "expression":{"name": "added", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "channel", "type": "STRING" },
    { "name": "cityName", "type": "STRING" },
    { "name": "comment", "type": "STRING" },
    { "name": "commentLength", "type": "NUMBER" },
    { "name": "commentLengthStr", "type": "STRING" },
    { "name": "count", "maker":{"op": "count"}, "type": "NUMBER", "unsplitable":true },
    { "name": "countryIsoCode", "type": "STRING" },
    { "name": "countryName", "type": "STRING" },
    { "name": "deleted", "maker":{"op": "sum", "expression":{"name": "deleted", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "delta", "maker":{"op": "sum", "expression":{"name": "delta", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "deltaBucket100", "type": "NUMBER" },
    { "name": "deltaByTen", "maker":{"op": "sum", "expression":{"name": "deltaByTen", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "delta_hist", "special": "histogram", "type": "NUMBER" },
    { "name": "isAnonymous", "type": "BOOLEAN" },
    { "name": "isMinor", "type": "BOOLEAN" },
    { "name": "isNew", "type": "BOOLEAN" },
    { "name": "isRobot", "type": "BOOLEAN" },
    { "name": "isUnpatrolled", "type": "BOOLEAN" },
    { "name": "max_delta", "maker":{"op": "max", "expression":{"name": "max_delta", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "metroCode", "type": "STRING" },
    { "name": "min_delta", "maker":{"op": "min", "expression":{"name": "min_delta", "op": "ref"}},"type": "NUMBER", "unsplitable":true },
    { "name": "namespace", "type": "STRING" },
    { "name": "page", "type": "STRING" },
    { "name": "page_unique", "special": "unique", "type": "STRING" },
    { "name": "regionIsoCode", "type": "STRING" },
    { "name": "regionName", "type": "STRING" },
    { "name": "sometimeLater", "type": "TIME" },
    { "name": "user", "type": "STRING" },
    { "name": "userChars", "type": "SET/STRING" },
    { "name": "user_theta", "special": "theta", "type": "STRING" },
    { "name": "user_unique", "special": "unique", "type": "STRING" }
  ];

  describe("defined attributes in datasource", () => {
    let wiki = External.fromJS({
      engine: 'druid',
      source: 'wikipedia',
      timeAttribute: 'time',
      context: info.druidContext,
      attributes: wikiAttributes,
      filter: $('time').in(TimeRange.fromJS({
        start: new Date("2015-09-12T00:00:00Z"),
        end: new Date("2015-09-13T00:00:00Z")
      })),
      version: info.druidVersion,
      allowSelectQueries: true
    }, druidRequester);

    it("aggregate and splits plus select work with ordering last split first", () => {
      let ex = $('wiki')
        .split({ 'isNew': '$isNew', 'isRobot': '$isRobot' })
        .apply('Count', $('wiki').sum('$count'))
        .limit(3);

      return toArray(ex.computeStream({ wiki }))
        .then((result) => {
          expect(JSON.parse(JSON.stringify(result))).to.deep.equal([
            {
              "__$$type": "init",
              "attributes": [
                {
                  "name": "isNew",
                  "type": "BOOLEAN"
                },
                {
                  "name": "isRobot",
                  "type": "BOOLEAN"
                },
                {
                  "name": "Count",
                  "type": "NUMBER"
                }
              ],
              "keys": null
            },
            {
              "Count": 217394,
              "isNew": false,
              "isRobot": false
            },
            {
              "Count": 151447,
              "isNew": false,
              "isRobot": true
            },
            {
              "Count": 20168,
              "isNew": true,
              "isRobot": false
            }
          ]);
        });
    });

  });

});
