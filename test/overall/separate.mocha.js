var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { Expression, $, ply, r } = plywood;

var toJS = function(sep) {
  if (!sep) {
    return sep;
  }
  return {
    included: sep.included.toJS(),
    excluded: sep.excluded.toJS()
  };
};

describe("separate", function() {
  it('throws on bad input', function() {
    return expect(function() {
        return Expression.TRUE.separateViaAnd();
      }
    ).to.throw('must have refName');
  });

  it('works with TRUE expression', function() {
    var ex = Expression.TRUE;

    return expect(toJS(ex.separateViaAnd('venue'))).to.deep.equal(toJS({
      included: Expression.TRUE,
      excluded: Expression.TRUE
    }));
  });

  it('works with FALSE expression', function() {
    var ex = Expression.FALSE;

    return expect(toJS(ex.separateViaAnd('venue'))).to.deep.equal(toJS({
      included: Expression.TRUE,
      excluded: Expression.FALSE
    }));
  });

  it('works on a single included expression', function() {
    var ex = $('venue').is('Google');

    return expect(toJS(ex.separateViaAnd('venue'))).to.deep.equal(toJS({
      included: ex,
      excluded: Expression.TRUE
    }));
  });

  it('works on a single excluded expression', function() {
    var ex = $('venue').is('Google');

    return expect(toJS(ex.separateViaAnd('make'))).to.deep.equal(toJS({
      included: Expression.TRUE,
      excluded: ex
    }));
  });

  it('works on a small AND expression', function() {
    var ex = $('venue').is('Google').and($('country').is('USA'));

    return expect(toJS(ex.separateViaAnd('country'))).to.deep.equal(toJS({
      included: $('country').is('USA'),
      excluded: $('venue').is('Google')
    }));
  });

  it('works on an AND expression', function() {
    var ex = $('venue').is('Google').and($('country').is('USA'), $('state').is('California'));

    return expect(toJS(ex.separateViaAnd('country'))).to.deep.equal(toJS({
      included: $('country').is('USA'),
      excluded: $('venue').is('Google').and($('state').is('California'))
    }));
  });

  it('extracts a NOT expression', function() {
    var ex = $('venue').is('Google').and($('country').is('USA').not(), $('state').is('California'));

    return expect(toJS(ex.separateViaAnd('country'))).to.deep.equal(toJS({
      included: $('country').is('USA').not(),
      excluded: $('venue').is('Google').and($('state').is('California'))
    }));
  });

  it('does not work on mixed OR expression', function() {
    var ex = $('venue').is('Google').or($('country').is('USA'), $('state').is('California'));

    return expect(toJS(ex.separateViaAnd('country'))).to.deep.equal(null);
  });

  it('works on mixed OR filter (all in)', function() {
    var ex = $('venue').is('Apple').or($('venue').is('Google').not());

    return expect(toJS(ex.separateViaAnd('venue'))).to.deep.equal(toJS({
      included: ex,
      excluded: Expression.TRUE
    }));
  });

  return it('works on mixed OR filter (all out)', function() {
    var ex = $('venue').is('Google').or($('country').is('USA'), $('state').is('California'));

    return expect(toJS(ex.separateViaAnd('model'))).to.deep.equal(toJS({
      included: Expression.TRUE,
      excluded: ex
    }));
  });
});
