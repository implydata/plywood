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
let { Expression, Dataset, $, ply, r } = plywood;

describe("traversal", () => {
  let subs = (ex, index, depth, nestDiff) => {
    let repNum = index * 1e6 + depth * 1e3 + nestDiff;
    if (ex.op === 'literal' && ex.type === 'NUMBER') {
      expect(ex.value).to.equal(repNum);
    }
    return null;
  };

  describe("total basics", () => {
    let ex = $('x').add(2001000);

    it("on substitute", () => {
      return ex.substitute(subs);
    });

    it("on every", () => {
      return ex.every(subs);
    });
  });


  describe("has the right parameters", () => {
    let ex = ply()
      .apply('num', 3002001)
      .apply(
        'subData',
        ply()
          .apply('x', '$num +  16011002')
          .apply('y', '$foo * 19010002')
          .apply('z', ply().sum(23010003).add(24009002))
          .apply('w', ply().sum('$a + 30010003 + $b'))
          .split('$x', 'X', 'data')
          .apply('x', '$num + 35006002')
          .apply('y', '$data:DATASET.sum(39006003) + 40005002')
          .apply('z', ply().sum(44005003).add(45004002))
          .apply('w', '47003002 + $data:DATASET.sum(50004003)')
      );

    it("on substitute", () => {
      return ex.substitute(subs);
    });

    it("on every", () => {
      return ex.every(subs);
    });
  });


  describe("has the right parameters with dataset", () => {
    let data = [
      { cut: 'Good', price: 400 },
      { cut: 'Good', price: 300 },
      { cut: 'Great', price: 124 },
      { cut: 'Wow', price: 160 },
      { cut: 'Wow', price: 100 }
    ];

    let ex = ply()
      .apply('Data', Dataset.fromJS(data))
      .apply('FooPlusCount', '6003001 + $Data.count()')
      .apply('CountPlusBar', '$Data.count() + 12002001');

    it("on substitute", () => {
      return ex.substitute(subs);
    });

    it("on every", () => {
      return ex.every(subs);
    });
  });
});
