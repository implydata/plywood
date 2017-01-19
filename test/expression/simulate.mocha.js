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
let { Expression, $, r, ply, Set, Dataset, External, ExternalExpression, fillExpressionExternalAlteration } = plywood;

let diamonds = External.fromJS({
  engine: 'druid',
  source: 'diamonds',
  timeAttribute: 'time',
  attributes: [
    { name: 'time', type: 'TIME' },
    { name: 'color', type: 'STRING' },
    { name: 'cut', type: 'STRING' },
    { name: 'isNice', type: 'BOOLEAN' },
    { name: 'tags', type: 'SET/STRING' },
    { name: 'pugs', type: 'SET/STRING' },
    { name: 'carat', type: 'NUMBER' },
    { name: 'height_bucket', type: 'NUMBER' },
    { name: 'price', type: 'NUMBER', unsplitable: true },
    { name: 'tax', type: 'NUMBER', unsplitable: true },
    { name: 'vendor_id', special: 'unique', unsplitable: true }
  ],
  allowEternity: true,
  allowSelectQueries: true
});

describe("evaluate step", () => {

  it('works in basic case', () => {
    let ex = ply()
      .apply('Total', '$diamonds.count()')
      .apply('TotalX2', '$Total * 2')
      .apply('SomeSplit', $('diamonds').split('$cut:STRING', 'Cut').limit(10))
      .apply('SomeNestedSplit',
        $('diamonds').split('$color:STRING', 'Color')
          .limit(10)
          .apply('SubSplit', $('diamonds').split('$cut:STRING', 'SubCut').limit(5))
      );

    expect(ex.simulate({ diamonds: diamonds }).toJS()).to.deep.equal([
      {
        "SomeNestedSplit": [
          {
            "Color": "some_color",
            "SubSplit": [
              {
                "SubCut": "some_cut"
              }
            ]
          }
        ],
        "SomeSplit": [
          {
            "Cut": "some_cut"
          }
        ],
        "Total": 4,
        "TotalX2": 4
      }
    ]);

  });

});
