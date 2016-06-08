var { expect } = require("chai");

var { testImmutableClass } = require("immutable-class/build/tester");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { Expression, TimeRange, NumberRange, $, r, ply, Set } = plywood;

function simplifiesTo(ex1, ex2) {
  var ex1Simple = ex1.simplify();
  expect(ex1Simple.simple, 'simplified version must be simple').to.equal(true);
  expect(ex1Simple.toJS(), 'must be the same').to.deep.equal(ex2.toJS());
}

function leavesAlone(ex) {
  simplifiesTo(ex, ex);
}

describe("Simplify", () => {
  describe('literals', () => {
    it("simplifies to number", () => {
      var ex1 = r(5).add(1).subtract(4);
      var ex2 = r(2);
      simplifiesTo(ex1, ex2);
    });

    it("simplifies literal prefix", () => {
      var ex1 = r(5).add(1).subtract(4).multiply('$x');
      var ex2 = r(2).multiply('$x');
      simplifiesTo(ex1, ex2);
    });
  });


  describe('leaves', () => {
    it('does not touch a lookup on a literal', () => {
      var ex = $('city').contains('San').and($('city').is(r('San Francisco')));
      leavesAlone(ex);
    });
  });


  describe('add', () => {
    it("removes 0 in simple case", () => {
      var ex1 = $('x').add(0);
      var ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("removes 0 complex case", () => {
      var ex1 = $('x').add(0, '$y', 0, '$z');
      var ex2 = $('x').add('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("removes leading 0", () => {
      var ex1 = r(0).add('$y', '$z');
      var ex2 = $('y').add('$z');
      simplifiesTo(ex1, ex2);
    });

    it("works in nested expression case", () => {
      var ex1 = $('x').add('0 + $y + 0 + $z');
      var ex2 = $('x').add('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("works with nested add", () => {
      var ex1 = $('x').add('2 * $y + $z');
      var ex2 = $('x').add('2 * $y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("works with literals", () => {
      var ex1 = r(1).add('2 + $y + 3 + 4');
      var ex2 = r(3).add('$y', 7);
      simplifiesTo(ex1, ex2);
    });
  });

  describe('fallback', () => {
    it("removes self if fallbackVal is null", () => {
      var ex1 = $('x').fallback(null);
      var ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });
  });


  describe('power', () => {
    it("removes self if 1", () => {
      var ex1 = $('x').power(1);
      var ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("removes self if 0", () => {
      var ex1 = $('x').power(0);
      var ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });
  });

  describe.skip('negate', () => {
    it("collapses double", () => {
      var ex1 = $('x').negate().negate();
      var ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("collapses long chain", () => {
      var ex1 = $('x').negate().negate().negate().negate().negate().negate().negate();
      var ex2 = $('x').negate();
      simplifiesTo(ex1, ex2);
    });
  });


  describe('multiply', () => {
    it("collapses 0 in simple case", () => {
      var ex1 = $('x').multiply(0);
      var ex2 = r(0);
      simplifiesTo(ex1, ex2);
    });

    it("collapses 0 in complex case", () => {
      var ex1 = $('x').multiply(6, '$y', 0, '$z');
      var ex2 = r(0);
      simplifiesTo(ex1, ex2);
    });

    it("collapses leading 0", () => {
      var ex1 = r(0).multiply(6, '$y', '$z');
      var ex2 = r(0);
      simplifiesTo(ex1, ex2);
    });

    it("removes 1 in simple case", () => {
      var ex1 = $('x').multiply(1);
      var ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("removes 1 complex case", () => {
      var ex1 = $('x').multiply(1, '$y', 1, '$z');
      var ex2 = $('x').multiply('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("removes leading 1", () => {
      var ex1 = r(1).multiply('$y', '$z');
      var ex2 = $('y').multiply('$z');
      simplifiesTo(ex1, ex2);
    });

    it("works in nested expression case", () => {
      var ex1 = $('x').multiply('1 * $y * 1 * $z');
      var ex2 = $('x').multiply('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("works with nested add", () => {
      var ex1 = $('x').multiply('(1 + $y) * $z');
      var ex2 = $('x').multiply('1 + $y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it.skip("works with trailing literals", () => {
      var ex1 = $('x').multiply(3).multiply(3);
      var ex2 = $('x').multiply(9);
      simplifiesTo(ex1, ex2);
    });
  });


  describe('and', () => {
    it("collapses false in simple case", () => {
      var ex1 = $('x').and(false);
      var ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it("collapses false in complex case", () => {
      var ex1 = $('x').and('$y', false, '$z');
      var ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it("collapses leading false", () => {
      var ex1 = r(false).and('$y', '$z');
      var ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it("removes true in simple case", () => {
      var ex1 = $('x').and(true);
      var ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("removes true complex case", () => {
      var ex1 = $('x').and(true, '$y', true, '$z');
      var ex2 = $('x').and('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("removes leading true", () => {
      var ex1 = r(true).and('$y', '$z');
      var ex2 = $('y').and('$z');
      simplifiesTo(ex1, ex2);
    });

    it("works in nested expression case", () => {
      var ex1 = $('x').and('true and $y and true and $z');
      var ex2 = $('x').and('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("works with nested add", () => {
      var ex1 = $('x').and('($a or $b) and $z');
      var ex2 = $('x').and('$a or $b', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("works with different filters", () => {
      var ex1 = $('flight').is(5).and($('flight').is(7));
      var ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it("works with same filters", () => {
      var ex1 = $('flight').is(5).and($('flight').is(5));
      var ex2 = $('flight').is(5);
      simplifiesTo(ex1, ex2);
    });

    it("works with IS and IN", () => {
      var ex1 = $('flight').is(5).and($('flight').in(new NumberRange({ start: 5, end: 7 })));
      var ex2 = $('flight').is(5);
      simplifiesTo(ex1, ex2);
    });

    it("re-arranges filters 1", () => {
      var ex1 = $('flight').is(5).and($('x').is(1)).and($('flight').is(7));
      var ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it("re-arranges filters 2", () => {
      var ex1 = $('flight').is(5).and($('x').is(1)).and($('flight').is(5));
      var ex2 = $('flight').is(5).and($('x').is(1));
      simplifiesTo(ex1, ex2);
    });
  });


  describe('or', () => {
    it("collapses true in simple case", () => {
      var ex1 = $('x').or(true);
      var ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it("collapses true in complex case", () => {
      var ex1 = $('x').or('$y', true, '$z');
      var ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it("collapses leading true", () => {
      var ex1 = r(true).or('$y', '$z');
      var ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it("removes false in simple case", () => {
      var ex1 = $('x').or(false);
      var ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("removes false complex case", () => {
      var ex1 = $('x').or(false, '$y', false, '$z');
      var ex2 = $('x').or('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("removes leading false", () => {
      var ex1 = r(false).or('$y', '$z');
      var ex2 = $('y').or('$z');
      simplifiesTo(ex1, ex2);
    });

    it("works in nested expression case", () => {
      var ex1 = $('x').or('false or $y or false or $z');
      var ex2 = $('x').or('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("works with nested add", () => {
      var ex1 = $('x').or('($a and $b) or $z');
      var ex2 = $('x').or('$a and $b', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("works with different filters", () => {
      var ex1 = $('flight').is(5).or($('flight').is(7));
      var ex2 = $('flight').in([5, 7]);
      simplifiesTo(ex1, ex2);
    });

    it("works with same filters", () => {
      var ex1 = $('flight').is(5).or($('flight').is(5));
      var ex2 = $('flight').is(5);
      simplifiesTo(ex1, ex2);
    });

    it("works with IS and IN", () => {
      var ex1 = $('flight').is(5).or($('flight').in(new NumberRange({ start: 5, end: 7 })));
      var ex2 = $('flight').in(new NumberRange({ start: 5, end: 7 }));
      simplifiesTo(ex1, ex2);
    });

    it("re-arranges filters 1", () => {
      var ex1 = $('flight').is(5).or($('x').is(1)).or($('flight').is(7));
      var ex2 = $('flight').in([5, 7]).or($('x').is(1));
      simplifiesTo(ex1, ex2);
    });

    it("re-arranges filters 2", () => {
      var ex1 = $('flight').is(5).or($('x').is(1)).or($('flight').is(5));
      var ex2 = $('flight').is(5).or($('x').is(1));
      simplifiesTo(ex1, ex2);
    });
  });


  describe('not', () => {
    it("collapses double", () => {
      var ex1 = $('x').not().not();
      var ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("collapses long chain", () => {
      var ex1 = $('x').not().not().not().not().not().not().not();
      var ex2 = $('x').not();
      simplifiesTo(ex1, ex2);
    });
  });


  describe('is', () => {
    it("simplifies to false", () => {
      var ex1 = r(5).is(8);
      var ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it("simplifies to true", () => {
      var ex1 = r(5).is(5);
      var ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it("simplifies to true", () => {
      var ex1 = $('x').is('$x');
      var ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it('swaps yoda literal (with ref)', () => {
      var ex1 = r("Honda").is('$x');
      var ex2 = $('x').is('Honda');
      simplifiesTo(ex1, ex2);
    });

    it('swaps yoda literal (with complex)', () => {
      var ex1 = r("Dhello").is($('color').concat(r('hello')));
      var ex2 = $('color').concat(r('hello')).is(r("Dhello"));
      simplifiesTo(ex1, ex2);
    });

    it('removes a timeBucket', () => {
      var interval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-03Z')
      });
      var ex1 = $('time').timeBucket('P1D', 'Etc/UTC').is(interval);
      var ex2 = $('time').in(interval);
      simplifiesTo(ex1, ex2);
    });

    it('does not remove a timeBucket with no timezone', () => {
      var interval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-03Z')
      });
      var ex = $('time').timeBucket('P1D').is(interval);
      expect(ex.simplify().toJS()).to.deep.equal(ex.toJS());
    });

    it('kills impossible timeBucket (no start)', () => {
      var interval = TimeRange.fromJS({
        start: null,
        end: new Date('2016-01-03Z')
      });
      var ex1 = $('time').timeBucket('P1D', 'Etc/UTC').is(interval);
      var ex2 = Expression.FALSE;
      simplifiesTo(ex1, ex2);
    });

    it('kills impossible timeBucket (not aligned)', () => {
      var interval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-04Z')
      });
      var ex1 = $('time').timeBucket('P1D', 'Etc/UTC').is(interval);
      var ex2 = Expression.FALSE;
      simplifiesTo(ex1, ex2);
    });

    it('removes a numberBucket', () => {
      var interval = NumberRange.fromJS({
        start: 1,
        end: 6
      });
      var ex1 = $('num').numberBucket(5, 1).is(interval);
      var ex2 = $('num').in(interval);
      simplifiesTo(ex1, ex2);
    });

    it('removes a numberBucket with 0 start', () => {
      var interval = NumberRange.fromJS({
        start: 0,
        end: 5
      });
      var ex1 = $('num').numberBucket(5, 0).is(interval);
      var ex2 = $('num').in(interval);
      simplifiesTo(ex1, ex2);
    });

    it('kills impossible numberBucket (no start)', () => {
      var interval = NumberRange.fromJS({
        start: null,
        end: 6
      });
      var ex1 = $('time').numberBucket(5, 1).is(interval);
      var ex2 = Expression.FALSE;
      simplifiesTo(ex1, ex2);
    });

    it('kills impossible numberBucket (not aligned)', () => {
      var interval = NumberRange.fromJS({
        start: 2,
        end: 7
      });
      var ex1 = $('time').numberBucket(5, 1).is(interval);
      var ex2 = Expression.FALSE;
      simplifiesTo(ex1, ex2);
    });

    it('kills impossible fallback', () => {
      var ex1 = $('color').fallback('NoColor').is('D');
      var ex2 = $('color').is('D');;
      simplifiesTo(ex1, ex2);
    });

    it('leaves possible fallback', () => {
      var ex1 = $('color').fallback('D').is('D');
      var ex2 = $('color').fallback('D').is('D');
      simplifiesTo(ex1, ex2);
    });

  });


  describe('in', () => {
    it('simplifies when empty set', () => {
      var ex1 = $('x').in([]);
      var ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies when singleton set', () => {
      var ex1 = $('x', 'STRING').in(['A']);
      var ex2 = $('x', 'STRING').is('A');
      simplifiesTo(ex1, ex2);
    });
  });


  describe('overlap', () => {
    it('swaps yoda literal (with ref)', () => {
      var someSet = Set.fromJS(['A', 'B', 'C']);
      var ex1 = r(someSet).overlap('$x');
      var ex2 = $('x').overlap(someSet);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies when empty set (lhs)', () => {
      var ex1 = r([]).overlap('$x');
      var ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies when empty set (rhs)', () => {
      var ex1 = $('x').overlap([]);
      var ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies to IN', () => {
      var someSet = Set.fromJS(['A', 'B', 'C']);
      var ex1 = $('x', 'STRING').overlap(someSet);
      var ex2 = $('x', 'STRING').in(someSet);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies to IS (via IN)', () => {
      var ex1 = $('x', 'STRING').overlap(Set.fromJS(['A']));
      var ex2 = $('x', 'STRING').is('A');
      simplifiesTo(ex1, ex2);
    });
  });


  describe('match', () => {
    it('with false value', () => {
      var ex1 = r("Honda").match('^\\d+');
      var ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('with true value', () => {
      var ex1 = r("123").match('^\\d+');
      var ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it('with reference value', () => {
      var ex1 = $('test').match('^\\d+');
      var ex2 = $('test').match('^\\d+');
      simplifiesTo(ex1, ex2);
    });
  });


  describe('timeFloor', () => {
    it('with simple expression', () => {
      var ex1 = r(new Date('2015-02-20T15:41:12')).timeFloor('P1D', 'Etc/UTC');
      var ex2 = r(new Date('2015-02-20T00:00:00'));
      simplifiesTo(ex1, ex2);
    });

    it('wipes out itself', () => {
      var ex1 = $('x').timeFloor('P1D', 'Etc/UTC').timeFloor('P1D', 'Etc/UTC');
      var ex2 = $('x').timeFloor('P1D', 'Etc/UTC');
      simplifiesTo(ex1, ex2);
    });
  });


  describe('timeShift', () => {
    it('with simple expression', () => {
      var ex1 = r(new Date('2015-02-20T15:41:12')).timeShift('P1D', 1, 'Etc/UTC');
      var ex2 = r(new Date('2015-02-21T15:41:12'));
      simplifiesTo(ex1, ex2);
    });

    it('combines with itself', () => {
      var ex1 = $('x').timeShift('P1D', 10, 'Etc/UTC').timeShift('P1D', -7, 'Etc/UTC');
      var ex2 = $('x').timeShift('P1D', 3, 'Etc/UTC');
      simplifiesTo(ex1, ex2);
    });
  });


  describe('timeBucket', () => {
    it('with simple expression', () => {
      var ex1 = r(new Date("2015-02-19T05:59:02.822Z")).timeBucket('P1D', 'Etc/UTC');
      var ex2 = r(TimeRange.fromJS({
        start: new Date("2015-02-19T00:00:00.000Z"),
        end: new Date("2015-02-20T00:00:00.000Z")
      }));
      simplifiesTo(ex1, ex2);
    });
  });


  describe('numberBucket', () => {
    it('with simple expression', () => {
      var ex1 = r(1.03).numberBucket(0.05, 0.02);
      var ex2 = r(NumberRange.fromJS({
        start: 1.02,
        end: 1.07
      }));
      simplifiesTo(ex1, ex2);
    });
  });


  describe('filter', () => {
    it('consecutive filters fold together', () => {
      var ex1 = ply()
        .filter('$x == 1')
        .filter('$y == 2');

      var ex2 = ply()
        .filter('$x == 1 and $y == 2');

      simplifiesTo(ex1, ex2);
    });

    it('moves filter before applies', () => {
      var ex1 = ply()
        .apply('Wiki', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)')
        .filter('$x == "en"');

      var ex2 = ply()
        .filter('$x == "en"')
        .apply('Wiki', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('does not change the meaning', () => {
      var ex1 = ply()
        .apply('Wiki', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)')
        .filter('$AddedByDeleted == 1');

      var ex2 = ply()
        .apply('Wiki', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .filter('$AddedByDeleted == 1')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('it can move past a split', () => {
      var ex1 = $('wiki')
        .split('$page', 'Page')
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)')
        .filter('$Page == "hello world"');

      var ex2 = $('wiki')
        .filter('$page == "hello world"')
        .split('$page', 'Page')
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('it can move past a fancy split', () => {
      var ex1 = $('wiki')
        .split('$time.timeBucket(P1D)', 'TimeByDay')
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)')
        .filter('$TimeByDay != null');

      var ex2 = $('wiki')
        .filter('$time.timeBucket(P1D) != null')
        .split('$time.timeBucket(P1D)', 'TimeByDay')
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('it can move past a sort', () => {
      var ex1 = ply()
        .sort('$deleted', 'ascending')
        .filter('$AddedByDeleted == 1');

      var ex2 = ply()
        .filter('$AddedByDeleted == 1')
        .sort('$deleted', 'ascending');

      simplifiesTo(ex1, ex2);
    });
  });


  describe('split', () => {
    it('does not touch a split on a literal', () => {
      var ex1 = ply().split('$page', 'Page', 'data');
      var ex2 = ply().split('$page', 'Page', 'data');
      simplifiesTo(ex1, ex2);
    });

    it('simplifies the split expression', () => {
      var ex1 = ply().split('$x.absolute().absolute()', 'Page', 'data');
      var ex2 = ply().split('$x.absolute()', 'Page', 'data');
      simplifiesTo(ex1, ex2);
    });
  });


  describe('apply', () => {
    it('removes no-op applies', () => {
      var ex1 = ply()
        .apply('x', '$x');

      var ex2 = ply();

      simplifiesTo(ex1, ex2);
    });

    it('sorts applies does not mess with sort if all are simple 1', () => {
      var ex1 = ply()
        .apply('Count', '$wiki.count()')
        .apply('Deleted', '$wiki.sum($deleted)');

      var ex2 = ply()
        .apply('Count', '$wiki.count()')
        .apply('Deleted', '$wiki.sum($deleted)');

      simplifiesTo(ex1, ex2);
    });

    it('sorts applies does not mess with sort if all are simple 2', () => {
      var ex1 = ply()
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('Count', '$wiki.count()');

      var ex2 = ply()
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('Count', '$wiki.count()');

      simplifiesTo(ex1, ex2);
    });

    it('sorts applies 2', () => {
      var ex1 = ply()
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)')
        .apply('Deleted', '$wiki.sum($deleted)');

      var ex2 = ply()
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });
  });


  describe('sort', () => {
    it('consecutive identical sorts fold together', () => {
      var ex1 = $('main')
        .sort('$x', 'descending')
        .sort('$x', 'ascending');

      var ex2 = $('main')
        .sort('$x', 'ascending');

      simplifiesTo(ex1, ex2);
    });
  });


  describe('limit', () => {
    it('consecutive limits fold together', () => {
      var ex1 = $('main')
        .limit(10)
        .limit(20);

      var ex2 = $('main')
        .limit(10);

      simplifiesTo(ex1, ex2);
    });

    it('moves past apply', () => {
      var ex1 = $('main')
        .apply('Wiki', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)')
        .limit(10);

      var ex2 = $('main')
        .limit(10)
        .apply('Wiki', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });
  });


  describe('select', () => {
    it('consecutive selects fold together', () => {
      var ex1 = $('main')
        .select('a', 'b')
        .select('a', 'c');

      var ex2 = $('main')
        .select('a');

      simplifiesTo(ex1, ex2);
    });

    it('removes a preceding apply', () => {
      var ex1 = $('main')
        .apply('Added', '$wiki.sum($added)')
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)')
        .select('Added', 'Deleted');

      var ex2 = $('main')
        .apply('Added', '$wiki.sum($added)')
        .apply('Deleted', '$wiki.sum($deleted)')
        .select('Added', 'Deleted');

      simplifiesTo(ex1, ex2);
    });
  });


  describe('concat', () => {
    it('removes empty strings', () => {
      var ex1 = r('').concat('$x', r(''));
      var ex2 = $('x');

      simplifiesTo(ex1, ex2);
    });

    it('concatenates literal', () => {
      var ex1 = r('p_').concat('hello', '$x', 'i_', 'love');
      var ex2 = r('p_hello').concat('$x', 'i_love');

      simplifiesTo(ex1, ex2);
    });
  });


  describe('lookup', () => {
    it('does not touch a lookup on a literal', () => {
      var ex1 = r('hello')
        .lookup('hello_lookup');

      var ex2 = r('hello')
        .lookup('hello_lookup');

      simplifiesTo(ex1, ex2);
    });
  });
});
