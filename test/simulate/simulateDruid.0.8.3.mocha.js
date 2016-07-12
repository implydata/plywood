/*
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
  { name: 'vendor_id', special: 'unique', unsplitable: true }
];

var context = {
  'diamonds': External.fromJS({
    engine: 'druid',
    version: '0.8.3',
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


describe("simulate Druid 0.8.3", () => {
  it("works contains filter (case sensitive)", () => {
    var ex = ply()
      .apply('diamonds', $('diamonds').filter($('color').contains(r('sup"yo'))))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "function": "function(d){var _,_2;return (_=d,(_==null)?null:((''+_).indexOf(\"sup\\\"yo\")>-1));}",
      "type": "javascript"
    });
  });

  it("works contains filter (case insensitive)", () => {
    var ex = ply()
      .apply('diamonds', $('diamonds').filter($('color').contains(r('sup"yo'), 'ignoreCase')))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "query": {
        "type": "insensitive_contains",
        "value": 'sup"yo'
      },
      "type": "search"
    });
  });

});
