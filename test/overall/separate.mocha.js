var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { Expression, $, ply, r } = plywood;

var toJS = (sep) => {
  if (!sep) {
    return sep;
  }
  return {
    included: sep.included.toJS(),
    excluded: sep.excluded.toJS()
  };
};

describe("separate", () => {
  it('throws on bad input', () => {
    expect(() => {
        return Expression.TRUE.separateViaAnd();
      }
    ).to.throw('must have refName');
  });

  it('works with TRUE expression', () => {
    var ex = Expression.TRUE;

    expect(toJS(ex.separateViaAnd('venue'))).to.deep.equal(toJS({
      included: Expression.TRUE,
      excluded: Expression.TRUE
    }));
  });

  it('works with FALSE expression', () => {
    var ex = Expression.FALSE;

    expect(toJS(ex.separateViaAnd('venue'))).to.deep.equal(toJS({
      included: Expression.TRUE,
      excluded: Expression.FALSE
    }));
  });

  it('works on a single included expression', () => {
    var ex = $('venue').is('Google');

    expect(toJS(ex.separateViaAnd('venue'))).to.deep.equal(toJS({
      included: ex,
      excluded: Expression.TRUE
    }));
  });

  it('works on a single excluded expression', () => {
    var ex = $('venue').is('Google');

    expect(toJS(ex.separateViaAnd('make'))).to.deep.equal(toJS({
      included: Expression.TRUE,
      excluded: ex
    }));
  });

  it('works on a small AND expression', () => {
    var ex = $('venue').is('Google').and($('country').is('USA'));

    expect(toJS(ex.separateViaAnd('country'))).to.deep.equal(toJS({
      included: $('country').is('USA'),
      excluded: $('venue').is('Google')
    }));
  });

  it('works on an AND expression', () => {
    var ex = $('venue').is('Google').and($('country').is('USA'), $('state').is('California'));

    expect(toJS(ex.separateViaAnd('country'))).to.deep.equal(toJS({
      included: $('country').is('USA'),
      excluded: $('venue').is('Google').and($('state').is('California'))
    }));
  });

  it('extracts a NOT expression', () => {
    var ex = $('venue').is('Google').and($('country').is('USA').not(), $('state').is('California'));

    expect(toJS(ex.separateViaAnd('country'))).to.deep.equal(toJS({
      included: $('country').is('USA').not(),
      excluded: $('venue').is('Google').and($('state').is('California'))
    }));
  });

  it('does not work on mixed OR expression', () => {
    var ex = $('venue').is('Google').or($('country').is('USA'), $('state').is('California'));

    expect(toJS(ex.separateViaAnd('country'))).to.deep.equal(null);
  });

  it('works on mixed OR filter (all in)', () => {
    var ex = $('venue').is('Apple').or($('venue').is('Google').not());

    expect(toJS(ex.separateViaAnd('venue'))).to.deep.equal(toJS({
      included: ex,
      excluded: Expression.TRUE
    }));
  });

  it('works on mixed OR filter (all out)', () => {
    var ex = $('venue').is('Google').or($('country').is('USA'), $('state').is('California'));

    expect(toJS(ex.separateViaAnd('model'))).to.deep.equal(toJS({
      included: Expression.TRUE,
      excluded: ex
    }));
  });
});
