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

let { testImmutableClass } = require("immutable-class-tester");

let plywood = require('../plywood');
let { AttributeInfo, $, ply, r } = plywood;

describe("AttributeInfo", () => {
  it("is immutable class", () => {
    testImmutableClass(AttributeInfo, [
      { name: 'time', type: 'TIME' },
      { name: 'color', type: 'STRING' },
      { name: 'cut', type: 'STRING' },
      { name: 'tags', type: 'SET/STRING' },
      { name: 'carat', type: 'NUMBER' },
      { name: 'count', type: 'NUMBER', unsplitable: true, maker: { op: 'count' } },
      {
        name: 'price',
        type: 'NUMBER',
        unsplitable: true,
        maker: { op: 'sum', expression: { op: 'ref', name: 'price' } }
      },
      { name: 'tax', type: 'NUMBER', unsplitable: true },
      { name: 'vendor_id', special: 'unique', type: "STRING" },
      { name: 'vendor_id', special: 'theta', type: "STRING" },
      { name: 'vendor_hist', special: 'histogram', type: "NUMBER" }
    ]);
  });
});
