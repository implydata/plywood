/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2019 Imply Data, Inc.
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
let { Expression, $, CountDistinctExpression, RefExpression } = plywood;

const isExpression = e => e instanceof Expression;
const isRefExpression = e => e instanceof RefExpression;
const isCountDistinctExpression = e => e instanceof CountDistinctExpression;

describe("Expression.every", () => {
  it("return true for trivial case", () => {
    const exp = $("main");
    expect(exp.every(isRefExpression)).to.equal(true);
  });

  it("returns true for nested case", () => {
    const exp = $("main").multiply(2);
    expect(exp.every(isExpression)).to.equal(true);
  });

  it("returns false if something doesn\'t pass predicate", () => {
    const exp = $("main").multiply(2);
    expect(exp.every(isRefExpression)).to.equal(false);
  });
});

describe("Expression.some", () => {
  it("return true for trivial case", () => {
    const exp = $("main");
    expect(exp.some(isRefExpression)).to.equal(true);
  });

  it("returns true for nested case", () => {
    const exp = $("main").multiply(2);
    expect(exp.some(isRefExpression)).to.equal(true);
  });

  it("returns false if trivial case doesn\'t pass predicate", () => {
    const exp = $("main");
    expect(exp.some(isCountDistinctExpression)).to.equal(false);
  });

  it("returns false if everything doesn\'t pass predicate", () => {
    const exp = $("main").multiply(2);
    expect(exp.some(isCountDistinctExpression)).to.equal(false);
  });
});
