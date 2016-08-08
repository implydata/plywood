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
var { Expression, $, ply, r } = plywood;

describe("typecheck", () => {
  it("should throw silly ref type", () => {
    expect(() => {
      Expression.fromJS({ op: 'ref', type: 'Corn', name: 'str' });
    }).to.throw("unsupported type 'Corn'");
  });

  it("should throw on unbalanced IS", () => {
    expect(() => {
      r(5).is('hello');
    }).to.throw('is must have input of type STRING (is NUMBER)');
  });

  it("should throw on unbalanced IS (via explicit type)", () => {
    expect(() => {
      r(5).is('$hello:STRING');
    }).to.throw('is must have input of type STRING (is NUMBER)');
  });

  it("should throw on non numeric lessThan", () => {
    expect(() => {
      r(5).lessThan('hello');
    }).to.throw("lessThan must have input of type STRING (is NUMBER)");
  });

  it("should throw on bad IN", () => {
    expect(() => {
      r(5).in('hello');
    }).to.throw('in action has a bad type combination NUMBER IN STRING');
  });

  it("should throw on SET IN", () => {
    expect(() => {
      $('tags', 'SET/STRING').in('$more_tags');
    }).to.throw('in action has a bad type combination SET/STRING IN *');
  });

  it("should throw on mismatching fallback type", () => {
    expect(() => {
      r(5).fallback('hello');
    }).to.throw('fallback must have input of type STRING (is NUMBER)');
  });

  it("should throw on bad aggregate (SUM)", () => {
    expect(() => {
      ply().sum($('x', 'STRING'));
    }).to.throw('sum must have expression of type NUMBER (is STRING)');
  });

  it("should throw on bad custom transform action input", () => {
    expect(() => {
      r(['A', 'B']).customTransform('myCustomTransformName');
    }).to.throw('customTransform must have input of type NULL or BOOLEAN or NUMBER or TIME or STRING (is SET/STRING)');
  });

  it("should throw on overlay type mismatch", () => {
    expect(() => {
      $('x', 'NUMBER').overlap($('y', 'SET/STRING'));
    }).to.throw('overlap must have input of type SET/STRING or STRING (is NUMBER)');
  });
});
