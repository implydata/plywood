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
let { Expression, Action, Dataset, $, ply, r } = plywood;

describe("free references", () => {
  let context = {
    diamonds: Dataset.fromJS([
      { color: 'A', cut: 'great', carat: 1.1, price: 300 }
    ])
  };

  describe("works as expected", () => {
    it("works with basics", () => {
      let ex = $('x').add('$^y');
      expect(ex.getFreeReferences()).to.deep.equal(['^y', 'x']);
    });

    it("works when there are no free references", () => {
      let ex = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$x * 2')
        );

      ex = ex.referenceCheck({});
      expect(ex.getFreeReferences()).to.deep.equal([]);
    });

    it("works in a basic case", () => {
      let ex = Expression.parse('$x + $y * $z + $data.sum($revenue)');

      expect(ex.getFreeReferences()).to.deep.equal(['data', 'x', 'y', 'z']);
    });

    it("works in an actions case", () => {
      let ex = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$x * 2')
            .apply('z', '$diamonds.sum($price)')
        );

      ex = ex.referenceCheck(context);
      expect(ex.getFreeReferences()).to.deep.equal(['diamonds']);

      expect(ex.expression.getFreeReferences()).to.deep.equal(['^diamonds', 'num']);
    });

    it("works in a consecutive actions case", () => {
      let ex = ply()
        .apply('one', 1)
        .apply('two', '$one + 1')
        .apply('three', '$two + 1')
        .apply('four', '$three + 1');


      ex = ex.referenceCheck({});
      expect(ex.getFreeReferences()).to.deep.equal([]);
    });
  });

});
