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

describe("utils", () => {
  describe("safeAdd", () => {
    it("works on 0.2 + 0.1", () => {
      expect(plywood.safeAdd(0.2, 0.1)).to.equal(0.3);
      expect(plywood.safeAdd(0.2, 0.1)).to.not.equal(0.2 + 0.1);
    });

    it("works on 0.7 + 0.1", () => {
      expect(plywood.safeAdd(0.7, 0.1)).to.equal(0.8);
      expect(plywood.safeAdd(0.7, 0.1)).to.not.equal(0.7 + 0.1);
    });

    it("works on unrepresentable", () => {
      expect(plywood.safeAdd(1, 1 / 3)).to.equal(1 + 1 / 3);
    });
  });


  describe("continuousFloorExpression", () => {
    it("should be minimalistic (no size / no offset)", () => {
      expect(plywood.continuousFloorExpression("x", "Math.floor", 1, 0)).to.equal('Math.floor(x)');
    });

    it("should be minimalistic (no size)", () => {
      expect(plywood.continuousFloorExpression("x", "Math.floor", 1, 0.3)).to.equal('Math.floor(x - 0.3) + 0.3');
    });

    it("should be minimalistic (no offset)", () => {
      expect(plywood.continuousFloorExpression("x", "Math.floor", 5, 0)).to.equal('Math.floor(x / 5) * 5');
    });

    it("should be work in general", () => {
      expect(plywood.continuousFloorExpression("x", "Math.floor", 5, 3)).to.equal('Math.floor((x - 3) / 5) * 5 + 3');
    });
  });
});
