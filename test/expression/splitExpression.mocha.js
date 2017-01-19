/*
 * Copyright 2016-2016 Imply Data, Inc.
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
let { $, ply, r, Expression } = plywood;

describe("SplitExpression", () => {

  describe("#maxBucketNumber", () => {
    it("works with boolean ref case", () => {
      let splitExpression = Expression._.split({
        bool: $('bool', 'BOOLEAN')
      });

      expect(splitExpression.maxBucketNumber()).to.equal(3);
    });

    it("works with boolean expression case", () => {
      let splitExpression = Expression._.split({
        isBlah: $('x').is('blah')
      });

      expect(splitExpression.maxBucketNumber()).to.equal(3);
    });

    it("works in multi-split case", () => {
      let splitExpression = Expression._.split({
        timePart: $('time').timePart('HOUR_OF_DAY'),
        isBlah: $('x').is('blah')
      });

      expect(splitExpression.maxBucketNumber()).to.equal(81);
    });

    it("works in unknown", () => {
      let splitExpression = Expression._.split({
        isBlah: $('x')
      });

      expect(splitExpression.maxBucketNumber()).to.equal(Infinity);
    });

  });

});
