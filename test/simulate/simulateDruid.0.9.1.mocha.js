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

let { expect } = require("chai");

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
  { name: 'vendor_id', special: 'unique', unsplitable: true },
];

let context = {
  'diamonds': External.fromJS({
    engine: 'druid',
    version: '0.9.1',
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

describe("simulate Druid 0.9.1", () => {

  it("makes a filter on timePart", () => {
    let ex = $("diamonds").filter(
      $("time").timePart('HOUR_OF_DAY', 'Etc/UTC').in([3, 4, 10]).and($("time").in([
        TimeRange.fromJS({ start: new Date('2015-03-12T00:00:00'), end: new Date('2015-03-15T00:00:00') }),
        TimeRange.fromJS({ start: new Date('2015-03-16T00:00:00'), end: new Date('2015-03-18T00:00:00') })
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

});
