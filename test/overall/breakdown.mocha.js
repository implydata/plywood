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
let { Expression, External, $, ply, r } = plywood;

describe.skip("breakdown", () => {
  let context = {
    x: 1,
    y: 2,
    diamonds: External.fromJS({
      engine: 'druid',
      source: 'diamonds',
      timeAttribute: 'time',
      context: null,
      attributes: [
        { name: 'time', type: 'TIME' },
        { name: 'color', type: 'STRING' },
        { name: 'cut', type: 'STRING' },
        { name: 'carat', type: 'NUMBER' }
      ]
    }),
    diamonds2: External.fromJS({
      engine: 'druid',
      source: 'diamonds2',
      timeAttribute: 'time',
      context: null,
      attributes: [
        { name: 'time', type: 'TIME' },
        { name: 'color', type: 'STRING' },
        { name: 'cut', type: 'STRING' },
        { name: 'carat', type: 'NUMBER' }
      ]
    })
  };

  it("errors on breakdown zero datasets", () => {
    let ex = Expression.parse('$x * $y + 2');

    ex = ex.referenceCheck(context);
    expect(() => {
      ex.breakdownByDataset('b');
    }).to.throw();
  });

  it("errors on breakdown one datasets", () => {
    let ex = Expression.parse('$diamonds.count() * 2');

    ex = ex.referenceCheck(context);
    expect(() => {
      ex.breakdownByDataset('b');
    }).to.throw();
  });


  it("breakdown two datasets correctly", () => {
    let ex = Expression.parse('$diamonds.count() * $diamonds2.count() + $diamonds.sum($carat)');

    ex = ex.referenceCheck(context);
    let breakdown = ex.breakdownByDataset('b');
    expect(breakdown.singleDatasetActions.join(' | ')).to.equal(
      '.apply(b0, $diamonds:DATASET.count()) | .apply(b1, $diamonds2:DATASET.count()) | .apply(b2, $diamonds:DATASET.sum($carat:NUMBER))'
    );
    expect(breakdown.combineExpression.toString()).to.equal('(($b0 * $b1) + $b2)');
  });

  it("breakdown two datasets correctly (and de-duplicates expression)", () => {
    let ex = Expression.parse('$diamonds.count() * $diamonds2.sum($carat) + $diamonds.count()');

    ex = ex.referenceCheck(context);
    let breakdown = ex.breakdownByDataset('b');
    expect(breakdown.singleDatasetActions.join(' | ')).to.equal(
      '.apply(b0, $diamonds:DATASET.count()) | .apply(b1, $diamonds2:DATASET.sum($carat:NUMBER))'
    );
    expect(breakdown.combineExpression.toString()).to.equal('(($b0 * $b1) + $b0)');
  });
});
