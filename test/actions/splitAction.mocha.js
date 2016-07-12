/*
 * Copyright 2012-2015 Metamarkets Group Inc.
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

var plywood = require('../../build/plywood');
var { $, ply, r, SplitAction } = plywood;

describe("SplitAction", () => {

  describe("#maxBucketNumber", () => {
    it("works with boolean ref case", () => {
      var splitAction = new SplitAction({
        splits: {
          bool: $('bool', 'BOOLEAN')
        }
      });

      expect(splitAction.maxBucketNumber()).to.equal(3);
    });

    it("works with boolean expression case", () => {
      var splitAction = new SplitAction({
        splits: {
          isBlah: $('x').is('blah')
        }
      });

      expect(splitAction.maxBucketNumber()).to.equal(3);
    });

    it("works in multi-split case", () => {
      var splitAction = new SplitAction({
        splits: {
          timePart: $('time').timePart('HOUR_OF_DAY'),
          isBlah: $('x').is('blah')
        }
      });

      expect(splitAction.maxBucketNumber()).to.equal(81);
    });

    it("works in unknown", () => {
      var splitAction = new SplitAction({
        splits: {
          isBlah: $('x')
        }
      });

      expect(splitAction.maxBucketNumber()).to.equal(Infinity);
    });

  });

});
