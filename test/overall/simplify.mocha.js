var { expect } = require("chai");

var { testImmutableClass } = require("immutable-class/build/tester");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { Expression, TimeRange, NumberRange, $, r, ply } = plywood;

describe("Simplify", function() {
  it("simplifies to number", function() {
    var ex1 = r(5).add(1).subtract(4);
    expect(ex1.simplify().toJS()).to.deep.equal({
      op: 'literal',
      value: 2
    });
  });

  it("simplifies literal prefix", function() {
    var ex1 = r(5).add(1).subtract(4).multiply('$x');
    var ex2 = r(2).multiply('$x');
    expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
  });


  describe('add', function() {
    it("removes 0 in simple case", function() {
      var ex1 = $('x').add(0);
      var ex2 = $('x');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("removes 0 complex case", function() {
      var ex1 = $('x').add(0, '$y', 0, '$z');
      var ex2 = $('x').add('$y', '$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("removes leading 0", function() {
      var ex1 = r(0).add('$y', '$z');
      var ex2 = $('y').add('$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works in nested expression case", function() {
      var ex1 = $('x').add('0 + $y + 0 + $z');
      var ex2 = $('x').add('$y', '$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with nested add", function() {
      var ex1 = $('x').add('2 * $y + $z');
      var ex2 = $('x').add('2 * $y', '$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with literals", function() {
      var ex1 = r(1).add('2 + $y + 3 + 4');
      var ex2 = r(3).add('$y', 7);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });

  describe('fallback', function() {
    it("removes self if fallbackVal is null", function() {
      var ex1 = $('x').fallback(null);
      var ex2 = $('x');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('power', function() {
    it("removes self if 1", function() {
      var ex1 = $('x').power(1);
      var ex2 = $('x');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("removes self if 0", function() {
      var ex1 = $('x').power(0);
      var ex2 = $('x');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });

  describe.skip('negate', function() {
    it("collapses double", function() {
      var ex1 = $('x').negate().negate();
      var ex2 = $('x');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("collapses long chain", function() {
      var ex1 = $('x').negate().negate().negate().negate().negate().negate().negate();
      var ex2 = $('x').negate();
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('multiply', function() {
    it("collapses 0 in simple case", function() {
      var ex1 = $('x').multiply(0);
      var ex2 = r(0);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("collapses 0 in complex case", function() {
      var ex1 = $('x').multiply(6, '$y', 0, '$z');
      var ex2 = r(0);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("collapses leading 0", function() {
      var ex1 = r(0).multiply(6, '$y', '$z');
      var ex2 = r(0);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("removes 1 in simple case", function() {
      var ex1 = $('x').multiply(1);
      var ex2 = $('x');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("removes 1 complex case", function() {
      var ex1 = $('x').multiply(1, '$y', 1, '$z');
      var ex2 = $('x').multiply('$y', '$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("removes leading 1", function() {
      var ex1 = r(1).multiply('$y', '$z');
      var ex2 = $('y').multiply('$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works in nested expression case", function() {
      var ex1 = $('x').multiply('1 * $y * 1 * $z');
      var ex2 = $('x').multiply('$y', '$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with nested add", function() {
      var ex1 = $('x').multiply('(1 + $y) * $z');
      var ex2 = $('x').multiply('1 + $y', '$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('and', function() {
    it("collapses false in simple case", function() {
      var ex1 = $('x').and(false);
      var ex2 = r(false);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("collapses false in complex case", function() {
      var ex1 = $('x').and('$y', false, '$z');
      var ex2 = r(false);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("collapses leading false", function() {
      var ex1 = r(false).and('$y', '$z');
      var ex2 = r(false);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("removes true in simple case", function() {
      var ex1 = $('x').and(true);
      var ex2 = $('x');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("removes true complex case", function() {
      var ex1 = $('x').and(true, '$y', true, '$z');
      var ex2 = $('x').and('$y', '$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("removes leading true", function() {
      var ex1 = r(true).and('$y', '$z');
      var ex2 = $('y').and('$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works in nested expression case", function() {
      var ex1 = $('x').and('true and $y and true and $z');
      var ex2 = $('x').and('$y', '$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with nested add", function() {
      var ex1 = $('x').and('($a or $b) and $z');
      var ex2 = $('x').and('$a or $b', '$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with different filters", function() {
      var ex1 = $('flight').is(5).and($('flight').is(7));
      var ex2 = r(false);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with same filters", function() {
      var ex1 = $('flight').is(5).and($('flight').is(5));
      var ex2 = $('flight').is(5);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with IS and IN", function() {
      var ex1 = $('flight').is(5).and($('flight').in(new NumberRange({ start: 5, end: 7 })));
      var ex2 = $('flight').is(5);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("re-arranges filters 1", function() {
      var ex1 = $('flight').is(5).and($('x').is(1)).and($('flight').is(7));
      var ex2 = r(false);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("re-arranges filters 2", function() {
      var ex1 = $('flight').is(5).and($('x').is(1)).and($('flight').is(5));
      var ex2 = $('flight').is(5).and($('x').is(1));
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('or', function() {
    it("collapses true in simple case", function() {
      var ex1 = $('x').or(true);
      var ex2 = r(true);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("collapses true in complex case", function() {
      var ex1 = $('x').or('$y', true, '$z');
      var ex2 = r(true);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("collapses leading true", function() {
      var ex1 = r(true).or('$y', '$z');
      var ex2 = r(true);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("removes false in simple case", function() {
      var ex1 = $('x').or(false);
      var ex2 = $('x');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("removes false complex case", function() {
      var ex1 = $('x').or(false, '$y', false, '$z');
      var ex2 = $('x').or('$y', '$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("removes leading false", function() {
      var ex1 = r(false).or('$y', '$z');
      var ex2 = $('y').or('$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works in nested expression case", function() {
      var ex1 = $('x').or('false or $y or false or $z');
      var ex2 = $('x').or('$y', '$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with nested add", function() {
      var ex1 = $('x').or('($a and $b) or $z');
      var ex2 = $('x').or('$a and $b', '$z');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with different filters", function() {
      var ex1 = $('flight').is(5).or($('flight').is(7));
      var ex2 = $('flight').in([5, 7]);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with same filters", function() {
      var ex1 = $('flight').is(5).or($('flight').is(5));
      var ex2 = $('flight').is(5);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with IS and IN", function() {
      var ex1 = $('flight').is(5).or($('flight').in(new NumberRange({ start: 5, end: 7 })));
      var ex2 = $('flight').in(new NumberRange({ start: 5, end: 7 }));
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("re-arranges filters 1", function() {
      var ex1 = $('flight').is(5).or($('x').is(1)).or($('flight').is(7));
      var ex2 = $('flight').in([5, 7]).or($('x').is(1));
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("re-arranges filters 2", function() {
      var ex1 = $('flight').is(5).or($('x').is(1)).or($('flight').is(5));
      var ex2 = $('flight').is(5).or($('x').is(1));
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('not', function() {
    it("collapses double", function() {
      var ex1 = $('x').not().not();
      var ex2 = $('x');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("collapses long chain", function() {
      var ex1 = $('x').not().not().not().not().not().not().not();
      var ex2 = $('x').not();
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('is', function() {
    it("simplifies to false", function() {
      var ex1 = r(5).is(8);
      expect(ex1.simplify().toJS()).to.deep.equal({
        op: 'literal',
        value: false
      });
    });

    it("simplifies to true", function() {
      var ex1 = r(5).is(5);
      expect(ex1.simplify().toJS()).to.deep.equal({
        op: 'literal',
        value: true
      });
    });

    it("simplifies to true", function() {
      var ex1 = $('x').is('$x');
      expect(ex1.simplify().toJS()).to.deep.equal({
        op: 'literal',
        value: true
      });
    });

    it('swaps yoda literal (with ref)', function() {
      var ex1 = r("Honda").is('$x');
      var ex2 = $('x').is('Honda');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('swaps yoda literal (with complex)', function() {
      var ex1 = r("Dhello").is($('color').concat(r('hello')));
      var ex2 = $('color').concat(r('hello')).is(r("Dhello"));
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('removes a timeBucket', function() {
      var interval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-03Z')
      });
      var ex1 = $('time').timeBucket('P1D', 'Etc/UTC').is(interval);
      var ex2 = $('time').in(interval);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('does not remove a timeBucket with no timezone', function() {
      var interval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-03Z')
      });
      var ex = $('time').timeBucket('P1D').is(interval);
      expect(ex.simplify().toJS()).to.deep.equal(ex.toJS());
    });
  });


  describe('match', function() {
    it('with false value', function() {
      var ex1 = r("Honda").match('^\\d+');
      var ex2 = r(false);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('with true value', function() {
      var ex1 = r("123").match('^\\d+');
      var ex2 = r(true);
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('with reference value', function() {
      var ex1 = $('test').match('^\\d+');
      var ex2 = $('test').match('^\\d+');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('timeFloor', function() {
    it('with simple expression', function() {
      var ex1 = r(new Date('2015-02-20T15:41:12')).timeFloor('P1D', 'Etc/UTC');
      var ex2 = r(new Date('2015-02-20T00:00:00'));
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('wipes out itself', function() {
      var ex1 = $('x').timeFloor('P1D', 'Etc/UTC').timeFloor('P1D', 'Etc/UTC');
      var ex2 = $('x').timeFloor('P1D', 'Etc/UTC');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('timeShift', function() {
    it('with simple expression', function() {
      var ex1 = r(new Date('2015-02-20T15:41:12')).timeShift('P1D', 1, 'Etc/UTC');
      var ex2 = r(new Date('2015-02-21T15:41:12'));
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('combines with itself', function() {
      var ex1 = $('x').timeShift('P1D', 10, 'Etc/UTC').timeShift('P1D', -7, 'Etc/UTC');
      var ex2 = $('x').timeShift('P1D', 3, 'Etc/UTC');
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('timeBucket', function() {
    it('with simple expression', function() {
      var ex1 = r(new Date("2015-02-19T05:59:02.822Z")).timeBucket('P1D', 'Etc/UTC');
      var ex2 = r(TimeRange.fromJS({
        start: new Date("2015-02-19T00:00:00.000Z"),
        end: new Date("2015-02-20T00:00:00.000Z")
      }));
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('numberBucket', function() {
    it('with simple expression', function() {
      var ex1 = r(1.03).numberBucket(0.05, 0.02);
      var ex2 = r(NumberRange.fromJS({
        start: 1.02,
        end: 1.07
      }));
      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('filter', function() {
    it('consecutive filters fold together', function() {
      var ex1 = ply()
        .filter('$x == 1')
        .filter('$y == 2');

      var ex2 = ply()
        .filter('$x == 1 and $y == 2');

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('moves filter before applies', function() {
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

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('does not change the meaning', function() {
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

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('it can move past a split', function() {
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

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('it can move past a fancy split', function() {
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

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('it can move past a sort', function() {
      var ex1 = ply()
        .sort('$deleted', 'ascending')
        .filter('$AddedByDeleted == 1');

      var ex2 = ply()
        .filter('$AddedByDeleted == 1')
        .sort('$deleted', 'ascending');

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('split', function() {
    it('does not touch a split on a literal', function() {
      var ex1 = ply()
        .split('$page', 'Page', 'data');

      var ex2 = ply()
        .split('$page', 'Page', 'data');

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('apply', function() {
    it('sorts applies does not mess with sort if all are simple 1', function() {
      var ex1 = ply()
        .apply('Count', '$wiki.count()')
        .apply('Deleted', '$wiki.sum($deleted)');

      var ex2 = ply()
        .apply('Count', '$wiki.count()')
        .apply('Deleted', '$wiki.sum($deleted)');

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });


    it('sorts applies does not mess with sort if all are simple 2', function() {
      var ex1 = ply()
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('Count', '$wiki.count()');

      var ex2 = ply()
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('Count', '$wiki.count()');

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('sorts applies 2', function() {
      var ex1 = ply()
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)')
        .apply('Deleted', '$wiki.sum($deleted)');

      var ex2 = ply()
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)');

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('sort', function() {
    it('consecutive identical sorts fold together', function() {
      var ex1 = $('main')
        .sort('$x', 'descending')
        .sort('$x', 'ascending');

      var ex2 = $('main')
        .sort('$x', 'ascending');

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('limit', function() {
    it('consecutive limits fold together', function() {
      var ex1 = $('main')
        .limit(10)
        .limit(20);

      var ex2 = $('main')
        .limit(10);

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('moves past apply', function() {
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

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('concat', function() {
    it('removes empty strings', function() {
      var ex1 = r('').concat('$x', r(''));
      var ex2 = $('x');

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it('concatenates literal', function() {
      var ex1 = r('p_').concat('hello', '$x', 'i_', 'love');
      var ex2 = r('p_hello').concat('$x', 'i_love');

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('lookup', function() {
    it('does not touch a lookup on a literal', function() {
      var ex1 = r('hello')
        .lookup('hello_lookup');

      var ex2 = r('hello')
        .lookup('hello_lookup');

      expect(ex1.simplify().toJS()).to.deep.equal(ex2.toJS());
    });
  });
});
