var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { Expression, $, ply, r } = plywood;

describe("typecheck", function() {
  it("should throw silly ref type", function() {
    return expect(function() {
      return Expression.fromJS({ op: 'ref', type: 'Corn', name: 'str' });
    }
    ).to.throw("unsupported type 'Corn'");
  });

  it("should throw on unbalanced IS", function() {
    return expect(function() {
      return r(5).is('hello');
    }
    ).to.throw('is must have input of type STRING (is NUMBER)');
  });

  it("should throw on unbalanced IS (via explicit type)", function() {
    return expect(function() {
      return r(5).is('$hello:STRING');
    }
    ).to.throw('is must have input of type STRING (is NUMBER)');
  });

  it("should throw on non numeric lessThan", function() {
    return expect(function() {
      return r(5).lessThan('hello');
    }
    ).to.throw('lessThan must have expression of type NUMBER or TIME (is STRING)');
  });

  it("should throw on bad in", function() {
    return expect(function() {
      return r(5).in('hello');
    }
    ).to.throw('in action has a bad type combination NUMBER in STRING');
  });

  return it("should throw on mismatching fallback type", function() {
    return expect(function() {
      return r(5).fallback('hello');
    }
    ).to.throw('fallback must have input of type STRING (is NUMBER)');
  });
});
