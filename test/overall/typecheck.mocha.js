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
    }).to.throw("could not parse 'hello' as time");
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

  it("should throw on overlay type mismatch", () => {
    expect(() => {
      $('x', 'NUMBER').overlap($('y', 'SET/STRING'));
    }).to.throw('type mismatch in overlap action: NUMBER is incompatible with SET/STRING');
  });
});
