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
let { Expression, $, ply, r } = plywood;

describe("every", () => {
  it("has sequential indexes", () => {
    let ex = ply()
      .apply('num', 5)
      .apply(
        'subData',
        ply()
          .apply('x', '$num + 1')
          .apply('y', '$foo * 2')
          .apply('z', ply().sum('$a + 3'))
          .apply('w', ply().sum('$a + 4 + $b'))
      );

    let indexes = [];
    let everyFn = (ex, index) => {
      indexes.push(index);
      return null;
    };

    let expressionCount = ex.expressionCount();
    ex.every(everyFn);
    expect(expressionCount).to.equal(27);
    expect(indexes).to.deep.equal(((() => {
      let result = [];
      let i = 0;
      while (i < expressionCount) {
        result.push(i++);
      }
      return result;
    })()));
  });

});
