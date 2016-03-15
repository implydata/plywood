var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { Expression, $, ply, r } = plywood;

var toJS = (extractAndRest) => {
  if (!extractAndRest) return extractAndRest;
  return {
    extract: extractAndRest.extract.toJS(),
    rest: extractAndRest.rest.toJS()
  };
};

function freeReferenceExtractor(refName) {
  return function(ex) {
    var freeRefs = ex.getFreeReferences();
    return freeRefs.length === 1 && freeRefs[0] === refName;
  };
}

describe("extractFromAnd", () => {
  it('works with TRUE expression', () => {
    var ex = Expression.TRUE;

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('venue')))).to.deep.equal(toJS({
      extract: Expression.TRUE,
      rest: Expression.TRUE
    }));
  });

  it('works with FALSE expression', () => {
    var ex = Expression.FALSE;

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('venue')))).to.deep.equal(toJS({
      extract: Expression.TRUE,
      rest: Expression.FALSE
    }));
  });

  it('works on a single extract expression', () => {
    var ex = $('venue').is('Google');

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('venue')))).to.deep.equal(toJS({
      extract: ex,
      rest: Expression.TRUE
    }));
  });

  it('works on a single rest expression', () => {
    var ex = $('venue').is('Google');

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('make')))).to.deep.equal(toJS({
      extract: Expression.TRUE,
      rest: ex
    }));
  });

  it('works on a small AND expression', () => {
    var ex = $('venue').is('Google').and($('country').is('USA'));

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('country')))).to.deep.equal(toJS({
      extract: $('country').is('USA'),
      rest: $('venue').is('Google')
    }));
  });

  it('works on an AND expression', () => {
    var ex = $('venue').is('Google').and($('country').is('USA'), $('state').is('California'));

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('country')))).to.deep.equal(toJS({
      extract: $('country').is('USA'),
      rest: $('venue').is('Google').and($('state').is('California'))
    }));
  });

  it('extracts a NOT expression', () => {
    var ex = $('venue').is('Google').and($('country').is('USA').not(), $('state').is('California'));

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('country')))).to.deep.equal(toJS({
      extract: $('country').is('USA').not(),
      rest: $('venue').is('Google').and($('state').is('California'))
    }));
  });

  it('works on mixed OR filter (all in)', () => {
    var ex = $('venue').is('Apple').or($('venue').is('Google').not());

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('venue')))).to.deep.equal(toJS({
      extract: ex,
      rest: Expression.TRUE
    }));
  });

  it('works on mixed OR filter (all out)', () => {
    var ex = $('venue').is('Google').or($('country').is('USA'), $('state').is('California'));

    expect(toJS(ex.extractFromAnd(freeReferenceExtractor('model')))).to.deep.equal(toJS({
      extract: Expression.TRUE,
      rest: ex
    }));
  });
});
