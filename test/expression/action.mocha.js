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

var { testImmutableClass } = require("immutable-class-tester");

var plywood = require('../plywood');
var { LimitAction } = plywood;

describe("Action", () => {

  describe("has action back compat", () => {
    it("works with constructor", () => {
      expect(new LimitAction({
        value: 5
      }).toJS()).to.deep.equal({
        op: 'limit',
        operand: { op: 'ref', name: '_' },
        value: 5
      });
    });

    it("works with fromJS", () => {
      expect(LimitAction.fromJS({
        value: 5
      }).toJS()).to.deep.equal({
        op: 'limit',
        operand: { op: 'ref', name: '_' },
        value: 5
      });
    });

  });
});
