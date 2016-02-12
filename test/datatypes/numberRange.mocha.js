var { expect } = require("chai");

var { testImmutableClass } = require("immutable-class/build/tester");

var plywood = require('../../build/plywood');
var { NumberRange, $, ply, r } = plywood;

describe("NumberRange", function() {
  it("is immutable class", function() {
    return testImmutableClass(NumberRange, [
      {
        start: 0,
        end: 0
      },
      {
        start: 1,
        end: 1,
        bounds: '[]'
      },
      {
        start: 0,
        end: 1
      },
      {
        start: 7,
        end: 9
      },
      {
        start: 7,
        end: 9,
        bounds: '()'
      },
      {
        start: 7,
        end: 9,
        bounds: '[]'
      },
      {
        start: 7,
        end: 9,
        bounds: '(]'
      },
      {
        start: 7,
        end: null
      },
      {
        start: 7,
        end: null,
        bounds: '()'
      },
      {
        start: null,
        end: null,
        bounds: '()'
      }
    ]);
  });


  describe("does not die with hasOwnProperty", function() {
    return it("survives", function() {
      return expect(NumberRange.fromJS({
        start: 7,
        end: 9,
        hasOwnProperty: 'troll'
      }).toJS()).to.deep.equal({
        start: 7,
        end: 9
      });
    });
  });


  describe("errors", function() {
    return it("throws on bad numbers", function() {
      return expect(function() {
          return NumberRange.fromJS({
            start: 'lol',
            end: 'wat'
          });
        }
      ).to.throw('`start` must be a number');
    });
  });


  describe("#extend()", function() {
    it('works correctly with two bounded sets', function() {
      return expect(
        NumberRange.fromJS({ start: 0, end: 2 }).extend(NumberRange.fromJS({ start: 5, end: 6 })).toJS()
      ).to.deep.equal({ start: 0, end: 6 });
    });

    it('works correctly with a fancy bounds', function() {
      return expect(
        NumberRange.fromJS({ start: 0, end: 2, bounds: '(]' }).extend(NumberRange.fromJS({
          start: 5,
          end: 6,
          bounds: '(]'
        })).toJS()
      ).to.deep.equal({ start: 0, end: 6, bounds: '(]' });
    });

    return it('works correctly with infinite bounds on different sides', function() {
      return expect(
        NumberRange.fromJS({ start: null, end: 2 }).extend(NumberRange.fromJS({ start: 6, end: null })).toJS()
      ).to.deep.equal({ start: null, end: null, bounds: '()' });
    });
  });


  describe("#union()", function() {
    it('works correctly with a non-disjoint set', function() {
      return expect(
        NumberRange.fromJS({ start: 0, end: 2 }).union(NumberRange.fromJS({ start: 1, end: 3 })).toJS()
      ).to.deep.equal({ start: 0, end: 3 });
    });

    it('works correctly with a disjoint range', function() {
      return expect(
        NumberRange.fromJS({ start: 0, end: 1 }).union(NumberRange.fromJS({ start: 2, end: 3 }))
      ).to.deep.equal(null);
    });

    it('works correctly with a adjacent range', function() {
      return expect(
        NumberRange.fromJS({ start: 0, end: 1 }).union(NumberRange.fromJS({ start: 1, end: 2 })).toJS()
      ).to.deep.equal({ start: 0, end: 2 });
    });

    it('works correctly with a fancy bounds', function() {
      return expect(
        NumberRange.fromJS({ start: 0, end: 2, bounds: '(]' }).union(NumberRange.fromJS({
          start: 1,
          end: 3,
          bounds: '(]'
        })).toJS()
      ).to.deep.equal({ start: 0, end: 3, bounds: '(]' });
    });

    it('works with itself when open', function() {
      return expect(
        NumberRange.fromJS({ start: 0, end: 1, bounds: '()' }).union(NumberRange.fromJS({
          start: 0,
          end: 1,
          bounds: '()'
        })).toJS()
      ).to.deep.equal({ start: 0, end: 1, bounds: '()' });
    });

    it('works correctly with infinite bounds on different sides', function() {
      return expect(
        NumberRange.fromJS({ start: null, end: 2 }).union(NumberRange.fromJS({ start: 1, end: null })).toJS()
      ).to.deep.equal({ start: null, end: null, bounds: '()' });
    });

    return it('works correctly with infinite non intersecting bounds', function() {
      return expect(
        NumberRange.fromJS({ start: 1, end: null, bounds: '()' }).union(NumberRange.fromJS({
          start: null,
          end: 0,
          bounds: '(]'
        }))
      ).to.deep.equal(null);
    });
  });


  return describe("#intersect()", function() {
    it('works correctly with a non-disjoint range', function() {
      return expect(
        NumberRange.fromJS({ start: 0, end: 2 }).intersect(NumberRange.fromJS({ start: 1, end: 3 })).toJS()
      ).to.deep.equal({ start: 1, end: 2 });
    });

    it('works correctly with a disjoint range', function() {
      return expect(
        NumberRange.fromJS({ start: 0, end: 1 }).intersect(NumberRange.fromJS({ start: 2, end: 3 }))
      ).to.deep.equal(null);
    });

    it('works correctly with a adjacent range', function() {
      return expect(
        NumberRange.fromJS({ start: 0, end: 1 }).intersect(NumberRange.fromJS({ start: 1, end: 2 })).toJS()
      ).to.deep.equal({ start: 0, end: 0 });
    });

    it('works correctly with a fancy bounds', function() {
      return expect(
        NumberRange.fromJS({ start: 0, end: 2, bounds: '(]' }).intersect(NumberRange.fromJS({
          start: 1,
          end: 3,
          bounds: '(]'
        })).toJS()
      ).to.deep.equal({ start: 1, end: 2, bounds: '(]' });
    });

    it('works with itself when open', function() {
      return expect(
        NumberRange.fromJS({ start: 0, end: 1, bounds: '()' }).intersect(NumberRange.fromJS({
          start: 0,
          end: 1,
          bounds: '()'
        })).toJS()
      ).to.deep.equal({ start: 0, end: 1, bounds: '()' });
    });

    it('works correctly with infinite bounds', function() {
      return expect(
        NumberRange.fromJS({ start: null, end: 2 }).intersect(NumberRange.fromJS({ start: 1, end: null })).toJS()
      ).to.deep.equal({ start: 1, end: 2 });
    });

    return it('works correctly with infinite non intersecting bounds', function() {
      return expect(
        NumberRange.fromJS({ start: 1, end: null, bounds: '()' }).intersect(NumberRange.fromJS({
          start: null,
          end: 0,
          bounds: '(]'
        }))
      ).to.deep.equal(null);
    });
  });
});
