/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2017 Imply Data, Inc.
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

let { testImmutableClass } = require("immutable-class-tester");

let plywood = require('../plywood');
let { Expression, TimeRange, NumberRange, $, r, ply, Set, Dataset, External, ExternalExpression } = plywood;

function simplifiesTo(ex1, ex2) {
  let ex1Simple = ex1.simplify();
  expect(ex1Simple.simple, 'simplified version must be simple').to.equal(true);
  expect(ex1Simple.toJS(), 'must be the same').to.deep.equal(ex2.toJS());
}

function leavesAlone(ex) {
  simplifiesTo(ex, ex);
}

let diamonds = External.fromJS({
  engine: 'druid',
  source: 'diamonds',
  timeAttribute: 'time',
  attributes: [
    { name: 'time', type: 'TIME' },
    { name: 'color', type: 'STRING' },
    { name: 'cut', type: 'STRING' },
    { name: 'isNice', type: 'BOOLEAN' },
    { name: 'tags', type: 'SET/STRING' },
    { name: 'pugs', type: 'SET/STRING' },
    { name: 'carat', type: 'NUMBER' },
    { name: 'height_bucket', type: 'NUMBER' },
    { name: 'price', type: 'NUMBER', unsplitable: true },
    { name: 'tax', type: 'NUMBER', unsplitable: true },
    { name: 'vendor_id', special: 'unique', unsplitable: true }
  ],
  allowSelectQueries: true
});

describe("Simplify", () => {
  describe('literals', () => {
    it("simplifies to number", () => {
      let ex1 = r(5).add(1).subtract(4);
      let ex2 = r(2);
      simplifiesTo(ex1, ex2);
    });

    it("simplifies literal prefix", () => {
      let ex1 = r(5).add(1).subtract(4).multiply('$x');
      let ex2 = $('x').multiply(2);
      simplifiesTo(ex1, ex2);
    });

    it("simplifies cast", () => {
      let ex1 = r(1447430881000).cast('TIME');
      let ex2 = r(new Date('2015-11-13T16:08:01.000Z'));
      simplifiesTo(ex1, ex2);
    });

    it("simplifies cast to in statement", () => {
      let ex1 = $('time').greaterThan(r(1447430881000).cast('TIME')).and($('time').lessThan(r(1547430881000).cast('TIME')));
      let ex2 = $('time').in(new NumberRange({start: new Date('2015-11-13T16:08:01.000Z'), end: new Date('2019-01-14T01:54:41.000Z'), bounds: '()'}));
      simplifiesTo(ex1, ex2);
    });

    it("simplifies double cast", () => {
      let ex1 = $('time', 'TIME').cast('TIME').cast('TIME');
      let ex2 = $('time', 'TIME');
      simplifiesTo(ex1, ex2);
    });

    it("simplifies string cast", () => {
      let ex1 = r("blah").cast('STRING');
      let ex2 = r("blah");
      simplifiesTo(ex1, ex2);
    });

    it("simplifies time range to in statement", () => {
      let ex1 = $('time').greaterThan(r(new Date('2015-11-13T16:08:01.000Z'))).and($('time').lessThan(r(new Date('2019-01-14T01:54:41.000Z'))));
      let ex2 = $('time').in(new NumberRange({start: new Date('2015-11-13T16:08:01.000Z'), end: new Date('2019-01-14T01:54:41.000Z'), bounds: '()'}));
      simplifiesTo(ex1, ex2);
    });

    it("str.indexOf(substr) > -1 should simplify to CONTAINS(str, substr)", () => {
      let ex1 = $('page').indexOf('sdf').greaterThan(-1);
      let ex2 = $('page').contains('sdf');
      simplifiesTo(ex1, ex2);
    });

    it("str.indexOf(substr) >= 0 should simplify to CONTAINS(str, substr)", () => {
      let ex1 = $('page').indexOf('sdf').greaterThanOrEqual(0);
      let ex2 = $('page').contains('sdf');
      simplifiesTo(ex1, ex2);
    });

    it("str.indexOf(substr) < 1 should not simplify to contains", () => {
      let ex1 = $('page').indexOf('sdf').lessThan(1);
      let ex2 = $('page').indexOf('sdf').in(new NumberRange({ start: null, end: 1, bounds: "()" }));
      simplifiesTo(ex1, ex2);
    });

    it("str.indexOf(substr) != -1 should simplify to CONTAINS(str, substr)", () => {
      let ex1 = $('page').indexOf('sdf').isnt(-1);
      let ex2 = $('page').contains('sdf');
      simplifiesTo(ex1, ex2);
    });

    it("str.indexOf(substr) == -1 should simplify to str.contains(substr).not()", () => {
      let ex1 = $('page').indexOf('sdf').is(-1);
      let ex2 = $('page').contains('sdf').not();
      simplifiesTo(ex1, ex2);
    });

    it("chained transform case simplifies to last one", () => {
      let ex1 = $('page').transformCase('lowerCase').transformCase('upperCase').transformCase('lowerCase').transformCase('upperCase');
      let ex2 = $('page').transformCase('upperCase');
      simplifiesTo(ex1, ex2);
    });

    it("str.transformCase('lowerCase').contains(str.transformCase('lowerCase'))", () => {
      let ex1 = $('page').transformCase('lowerCase').contains($('comment').transformCase('lowerCase'));
      let ex2 = $('page').contains('$comment', 'ignoreCase');
      simplifiesTo(ex1, ex2);
    });

    it("transform case is idempotent", () => {
      let ex1 = $('page').transformCase('lowerCase').transformCase('lowerCase');
      let ex2 = $('page').transformCase('lowerCase');
      simplifiesTo(ex1, ex2);
    });
  });


  describe('leaves', () => {
    it('does not touch a lookup on a literal', () => {
      let ex = $('city').contains('San').and($('city').is(r('San Francisco')));
      leavesAlone(ex);
    });
  });


  describe('add', () => {
    it("removes 0 in simple case", () => {
      let ex1 = $('x').add(0);
      let ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("removes 0 complex case", () => {
      let ex1 = $('x').add(0, '$y', 0, '$z');
      let ex2 = $('x').add('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("removes leading 0", () => {
      let ex1 = r(0).add('$y', '$z');
      let ex2 = $('y').add('$z');
      simplifiesTo(ex1, ex2);
    });

    it("works in nested expression case", () => {
      let ex1 = $('x').add('0 + $y + 0 + $z');
      let ex2 = $('x').add('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("works with nested add", () => {
      let ex1 = $('x').add('2 * $y + $z');
      let ex2 = $('x').add('$y * 2', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("works with literals", () => {
      let ex1 = r(1).add('2 + $y + 30 + 40');
      let ex2 = $('y').add(73);
      simplifiesTo(ex1, ex2);
    });

    it("handles commutativity", () => {
      let ex1 = r(1).add($('x'));
      let ex2 = $('x').add(1);
      simplifiesTo(ex1, ex2);
    });

    it("handles associativity", () => {
      let ex1 = $('a').add($('b').add('$c'));
      let ex2 = $('a').add('$b').add('$c');
      simplifiesTo(ex1, ex2);
    });
  });


  describe('fallback', () => {
    it("removes self if else is null", () => {
      let ex1 = $('x').fallback(null);
      let ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("removes self if null", () => {
      let ex1 = r(null).fallback('$x');
      let ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("removes self if X = X", () => {
      let ex1 = $('x').fallback('$x');
      let ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("removes self if X = X", () => {
      let ex1 = $('x').fallback('$x');
      let ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

  });


  describe('power', () => {
    it("removes self if 1", () => {
      let ex1 = $('x').power(1);
      let ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("removes self if 0", () => {
      let ex1 = $('x').power(0);
      let ex2 = r(1);
      simplifiesTo(ex1, ex2);
    });
  });

  describe.skip('negate', () => {
    it("collapses double", () => {
      let ex1 = $('x').negate().negate();
      let ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("collapses long chain", () => {
      let ex1 = $('x').negate().negate().negate().negate().negate().negate().negate();
      let ex2 = $('x').negate();
      simplifiesTo(ex1, ex2);
    });
  });


  describe('multiply', () => {
    it("collapses 0 in simple case", () => {
      let ex1 = $('x').multiply(0);
      let ex2 = r(0);
      simplifiesTo(ex1, ex2);
    });

    it("collapses 0 in complex case", () => {
      let ex1 = $('x').multiply(6, '$y', 0, '$z');
      let ex2 = r(0);
      simplifiesTo(ex1, ex2);
    });

    it("collapses leading 0", () => {
      let ex1 = r(0).multiply(6, '$y', '$z');
      let ex2 = r(0);
      simplifiesTo(ex1, ex2);
    });

    it("removes 1 in simple case", () => {
      let ex1 = $('x').multiply(1);
      let ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("removes 1 complex case", () => {
      let ex1 = $('x').multiply(1, '$y', 1, '$z');
      let ex2 = $('x').multiply('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("removes leading 1", () => {
      let ex1 = r(1).multiply('$y', '$z');
      let ex2 = $('y').multiply('$z');
      simplifiesTo(ex1, ex2);
    });

    it("works in nested expression case", () => {
      let ex1 = $('x').multiply('1 * $y * 1 * $z');
      let ex2 = $('x').multiply('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("works with nested add", () => {
      let ex1 = $('x').multiply('(1 + $y) * $z');
      let ex2 = $('x').multiply('$y + 1', '$z');
      simplifiesTo(ex1, ex2);
    });

    it.skip("works with trailing literals", () => {
      let ex1 = $('x').multiply(3).multiply(3);
      let ex2 = $('x').multiply(9);
      simplifiesTo(ex1, ex2);
    });
  });


  describe('multiply', () => {
    it("collapses / 0", () => {
      let ex1 = $('x').divide(0);
      let ex2 = Expression.NULL;
      simplifiesTo(ex1, ex2);
    });

    it("removes 1 in simple case", () => {
      let ex1 = $('x').multiply(1);
      let ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

  });


  describe('and', () => {
    it("collapses false in simple case", () => {
      let ex1 = $('x').and(false);
      let ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it("collapses false in complex case", () => {
      let ex1 = $('x').and('$y', false, '$z');
      let ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it("collapses leading false", () => {
      let ex1 = r(false).and('$y', '$z');
      let ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it("removes true in simple case", () => {
      let ex1 = $('x').and(true);
      let ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("removes true complex case", () => {
      let ex1 = $('x').and(true, '$y', true, '$z');
      let ex2 = $('x').and('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("removes leading true", () => {
      let ex1 = r(true).and('$y', '$z');
      let ex2 = $('y').and('$z');
      simplifiesTo(ex1, ex2);
    });

    it("works in nested expression case", () => {
      let ex1 = $('x').and('true and $y and true and $z');
      let ex2 = $('x').and('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("works with nested or", () => {
      let ex1 = $('x').and('($a or $b) and $z');
      let ex2 = $('x').and('$a or $b', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("works with different filters", () => {
      let ex1 = $('flight').is(5).and($('flight').is(7));
      let ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it("works with different filters across filter", () => {
      let ex1 = $('flight').is(5).and($('lol').is(3)).and($('flight').is(7));
      let ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it("works with same filters", () => {
      let ex1 = $('flight').is(5).and($('flight').is(5));
      let ex2 = $('flight').is(5);
      simplifiesTo(ex1, ex2);
    });

    it("works with same filters across filter", () => {
      let ex1 = $('flight').is(5).and($('lol').is(3)).and($('flight').is(5));
      let ex2 = $('flight').is(5).and($('lol').is(3));
      simplifiesTo(ex1, ex2);
    });

    it("works with IS and IN", () => {
      let ex1 = $('flight').is(5).and($('flight').in(new NumberRange({ start: 5, end: 7 })));
      let ex2 = $('flight').is(5);
      simplifiesTo(ex1, ex2);
    });

    it("works with two number ranges", () => {
      let ex1 = $('x', 'NUMBER').in({ start: 1, end: 5 })
        .and($('x', 'NUMBER').in({ start: 1, end: 2 }));
      let ex2 = $('x', 'NUMBER').in({ start: 1, end: 2 });
      simplifiesTo(ex1, ex2);
    });

    it("works with two time ranges", () => {
      let ex1 = $('time', 'TIME').in({ start: new Date('2015-03-12T00:00:00'), end: new Date('2015-03-16T00:00:00') })
        .and($('time', 'TIME').in({ start: new Date('2015-03-12T00:00:00'), end: new Date('2015-03-13T00:00:00') }));
      let ex2 = $('time', 'TIME').in({ start: new Date('2015-03-12T00:00:00'), end: new Date('2015-03-13T00:00:00') });
      simplifiesTo(ex1, ex2);
    });

    it('removes a timeBucket', () => {
      let largeInterval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-09Z')
      });
      let smallInterval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-03Z')
      });
      let ex1 = $('time').in(largeInterval).and($('time').timeBucket('P1D', 'Etc/UTC').is(smallInterval));
      let ex2 = $('time').in(smallInterval);
      simplifiesTo(ex1, ex2);
    });

    it("re-arranges filters 1", () => {
      let ex1 = $('flight').is(5).and($('x').is(1)).and($('flight').is(7));
      let ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it("re-arranges filters 2", () => {
      let ex1 = $('flight').is(5).and($('x').is(1)).and($('flight').is(5));
      let ex2 = $('flight').is(5).and($('x').is(1));
      simplifiesTo(ex1, ex2);
    });

    it("works with match", () => {
      let ex1 = $('cityName').match("San").and($('cityName').match("Hello"));
      simplifiesTo(ex1, ex1);
    });

    it("works with same expression", () => {
      let ex1 = $('cityName').match("San").and($('cityName').match("San"));
      let ex2 = $('cityName').match("San");
      simplifiesTo(ex1, ex2);
    });

  });


  describe('or', () => {
    it("collapses true in simple case", () => {
      let ex1 = $('x').or(true);
      let ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it("collapses true in complex case", () => {
      let ex1 = $('x').or('$y', true, '$z');
      let ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it("collapses leading true", () => {
      let ex1 = r(true).or('$y', '$z');
      let ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it("removes false in simple case", () => {
      let ex1 = $('x').or(false);
      let ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("removes false complex case", () => {
      let ex1 = $('x').or(false, '$y', false, '$z');
      let ex2 = $('x').or('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("removes leading false", () => {
      let ex1 = r(false).or('$y', '$z');
      let ex2 = $('y').or('$z');
      simplifiesTo(ex1, ex2);
    });

    it("works in nested expression case", () => {
      let ex1 = $('x').or('false or $y or false or $z');
      let ex2 = $('x').or('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("works with nested and", () => {
      let ex1 = $('x').or('($a and $b) or $z');
      let ex2 = $('x').or('$a and $b', '$z');
      simplifiesTo(ex1, ex2);
    });

    it("works with different filters", () => {
      let ex1 = $('flight').is(5).or($('flight').is(7));
      let ex2 = $('flight').in([5, 7]);
      simplifiesTo(ex1, ex2);
    });

    it("works with same filters", () => {
      let ex1 = $('flight').is(5).or($('flight').is(5));
      let ex2 = $('flight').is(5);
      simplifiesTo(ex1, ex2);
    });

    it("works with IS and IN", () => {
      let ex1 = $('flight').is(5).or($('flight').in(new NumberRange({ start: 5, end: 7 })));
      let ex2 = $('flight').in(new NumberRange({ start: 5, end: 7 }));
      simplifiesTo(ex1, ex2);
    });

    it("re-arranges filters 1", () => {
      let ex1 = $('flight').is(5).or($('x').is(1)).or($('flight').is(7));
      let ex2 = $('flight').in([5, 7]).or($('x').is(1));
      simplifiesTo(ex1, ex2);
    });

    it("re-arranges filters 2", () => {
      let ex1 = $('flight').is(5).or($('x').is(1)).or($('flight').is(5));
      let ex2 = $('flight').is(5).or($('x').is(1));
      simplifiesTo(ex1, ex2);
    });

    it("works with match", () => {
      let ex1 = $('cityName').match("San").or($('cityName').match("Hello"));
      simplifiesTo(ex1, ex1);
    });

    it("works with same expression", () => {
      let ex1 = $('cityName').match("San").or($('cityName').match("San"));
      let ex2 = $('cityName').match("San");
      simplifiesTo(ex1, ex2);
    });

  });


  describe('not', () => {
    it("works on literal", () => {
      let ex1 = r(false).not();
      let ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it("collapses double", () => {
      let ex1 = $('x').not().not();
      let ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it("collapses long chain", () => {
      let ex1 = $('x').not().not().not().not().not().not().not();
      let ex2 = $('x').not();
      simplifiesTo(ex1, ex2);
    });
  });


  describe('is', () => {
    it("simplifies to false", () => {
      let ex1 = r(5).is(8);
      let ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it("simplifies to true with simple datatypes", () => {
      let ex1 = r(5).is(5);
      let ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it.skip("simplifies to true with complex datatypes", () => {
      let ex1 = r(TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-03Z')
      })).is(TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-03Z')
      }));
      let ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it("simplifies to true", () => {
      let ex1 = $('x').is('$x');
      let ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it('swaps yoda literal (with ref)', () => {
      let ex1 = r("Honda").is('$x');
      let ex2 = $('x').is('Honda');
      simplifiesTo(ex1, ex2);
    });

    it('swaps yoda literal (with complex)', () => {
      let ex1 = r("Dhello").is($('color').concat(r('hello')));
      let ex2 = $('color').concat(r('hello')).is(r("Dhello"));
      simplifiesTo(ex1, ex2);
    });

    it('removes a timeBucket', () => {
      let interval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-03Z')
      });
      let ex1 = $('time').timeBucket('P1D', 'Etc/UTC').is(interval);
      let ex2 = $('time').in(interval);
      simplifiesTo(ex1, ex2);
    });

    it('does not remove a timeBucket with no timezone', () => {
      let interval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-03Z')
      });
      let ex = $('time').timeBucket('P1D').is(interval);
      expect(ex.simplify().toJS()).to.deep.equal(ex.toJS());
    });

    it('kills impossible timeBucket (no start)', () => {
      let interval = TimeRange.fromJS({
        start: null,
        end: new Date('2016-01-03Z')
      });
      let ex1 = $('time').timeBucket('P1D', 'Etc/UTC').is(interval);
      let ex2 = Expression.FALSE;
      simplifiesTo(ex1, ex2);
    });

    it('kills impossible timeBucket (not aligned)', () => {
      let interval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-04Z')
      });
      let ex1 = $('time').timeBucket('P1D', 'Etc/UTC').is(interval);
      let ex2 = Expression.FALSE;
      simplifiesTo(ex1, ex2);
    });

    it('removes a numberBucket', () => {
      let interval = NumberRange.fromJS({
        start: 1,
        end: 6
      });
      let ex1 = $('num').numberBucket(5, 1).is(interval);
      let ex2 = $('num').in(interval);
      simplifiesTo(ex1, ex2);
    });

    it('removes a numberBucket with 0 start', () => {
      let interval = NumberRange.fromJS({
        start: 0,
        end: 5
      });
      let ex1 = $('num').numberBucket(5, 0).is(interval);
      let ex2 = $('num').in(interval);
      simplifiesTo(ex1, ex2);
    });

    it('kills impossible numberBucket (no start)', () => {
      let interval = NumberRange.fromJS({
        start: null,
        end: 6
      });
      let ex1 = $('time').numberBucket(5, 1).is(interval);
      let ex2 = Expression.FALSE;
      simplifiesTo(ex1, ex2);
    });

    it('kills impossible numberBucket (not aligned)', () => {
      let interval = NumberRange.fromJS({
        start: 2,
        end: 7
      });
      let ex1 = $('time').numberBucket(5, 1).is(interval);
      let ex2 = Expression.FALSE;
      simplifiesTo(ex1, ex2);
    });

    it('kills impossible fallback', () => {
      let ex1 = $('color').fallback('NoColor').is('D');
      let ex2 = $('color').is('D');
      simplifiesTo(ex1, ex2);
    });

    it('leaves possible fallback', () => {
      let ex1 = $('color').fallback('D').is('D');
      let ex2 = $('color').fallback('D').is('D');
      simplifiesTo(ex1, ex2);
    });

  });


  describe('in', () => {
    it('simplifies when empty set', () => {
      let ex1 = $('x').in([]);
      let ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies when singleton set', () => {
      let ex1 = $('x', 'STRING').in(['A']);
      let ex2 = $('x', 'STRING').is('A');
      simplifiesTo(ex1, ex2);
    });

    it('simplifies when set can be unified', () => {
      let ex1 = $('x', 'NUMBER').in(Set.fromJS({
        setType: 'NUMBER_RANGE',
        elements: [
          { start: 1, end: 3 },
          { start: 2, end: 5 },
        ]
      }));
      let ex2 = $('x', 'NUMBER').in(NumberRange.fromJS({ start: 1, end: 5 }));
      simplifiesTo(ex1, ex2);
    });

  });


  describe('overlap', () => {
    it('swaps yoda literal (with ref)', () => {
      let someSet = Set.fromJS(['A', 'B', 'C']);
      let ex1 = r(someSet).overlap('$x');
      let ex2 = $('x').overlap(someSet);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies when empty set (lhs)', () => {
      let ex1 = r([]).overlap('$x');
      let ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies when empty set (rhs)', () => {
      let ex1 = $('x').overlap([]);
      let ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies to IN', () => {
      let someSet = Set.fromJS(['A', 'B', 'C']);
      let ex1 = $('x', 'STRING').overlap(someSet);
      let ex2 = $('x', 'STRING').in(someSet);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies to IS (via IN)', () => {
      let ex1 = $('x', 'STRING').overlap(Set.fromJS(['A']));
      let ex2 = $('x', 'STRING').is('A');
      simplifiesTo(ex1, ex2);
    });
  });


  describe('match', () => {
    it('with false value', () => {
      let ex1 = r("Honda").match('^\\d+');
      let ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('with true value', () => {
      let ex1 = r("123").match('^\\d+');
      let ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it('with reference value', () => {
      let ex1 = $('test').match('^\\d+');
      let ex2 = $('test').match('^\\d+');
      simplifiesTo(ex1, ex2);
    });
  });


  describe('timeFloor', () => {
    it('with simple expression', () => {
      let ex1 = r(new Date('2015-02-20T15:41:12')).timeFloor('P1D', 'Etc/UTC');
      let ex2 = r(new Date('2015-02-20T00:00:00'));
      simplifiesTo(ex1, ex2);
    });

    it('wipes out itself', () => {
      let ex1 = $('x').timeFloor('P1D', 'Etc/UTC').timeFloor('P1D', 'Etc/UTC');
      let ex2 = $('x').timeFloor('P1D', 'Etc/UTC');
      simplifiesTo(ex1, ex2);
    });
  });


  describe('timeShift', () => {
    it('with simple expression', () => {
      let ex1 = r(new Date('2015-02-20T15:41:12')).timeShift('P1D', 1, 'Etc/UTC');
      let ex2 = r(new Date('2015-02-21T15:41:12'));
      simplifiesTo(ex1, ex2);
    });

    it('shifts 0', () => {
      let ex1 = $('x').timeShift('P1D', 0, 'Etc/UTC');
      let ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it('combines with itself', () => {
      let ex1 = $('x').timeShift('P1D', 10, 'Etc/UTC').timeShift('P1D', -7, 'Etc/UTC');
      let ex2 = $('x').timeShift('P1D', 3, 'Etc/UTC');
      simplifiesTo(ex1, ex2);
    });
  });


  describe('timeBucket', () => {
    it('with simple expression', () => {
      let ex1 = r(new Date("2015-02-19T05:59:02.822Z")).timeBucket('P1D', 'Etc/UTC');
      let ex2 = r(TimeRange.fromJS({
        start: new Date("2015-02-19T00:00:00.000Z"),
        end: new Date("2015-02-20T00:00:00.000Z")
      }));
      simplifiesTo(ex1, ex2);
    });
  });


  describe('numberBucket', () => {
    it('with simple expression', () => {
      let ex1 = r(1.03).numberBucket(0.05, 0.02);
      let ex2 = r(NumberRange.fromJS({
        start: 1.02,
        end: 1.07
      }));
      simplifiesTo(ex1, ex2);
    });
  });


  describe('filter', () => {
    it('folds with literal', () => {
      let ex1 = ply(Dataset.fromJS([
        { x: 1 },
        { x: 2 }
      ])).filter('$x == 2');

      let ex2 = ply(Dataset.fromJS([
        { x: 2 }
      ]));

      simplifiesTo(ex1, ex2);
    });

    it('consecutive filters fold together', () => {
      let ex1 = ply()
        .filter('$^x == 1')
        .filter('$^y == 2');

      let ex2 = ply()
        .filter('$^x == 1 and $^y == 2');

      simplifiesTo(ex1, ex2);
    });

    it('moves filter before applies', () => {
      let ex1 = ply()
        .apply('Wiki', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)')
        .filter('$^x == "en"');

      let ex2 = ply()
        .filter('$^x == "en"')
        .apply('Wiki', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('does not change the meaning', () => {
      let ex1 = ply()
        .apply('Wiki', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)')
        .filter('$AddedByDeleted == 1');

      let ex2 = ply()
        .apply('Wiki', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .filter('$AddedByDeleted == 1')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('can move past a linear split', () => {
      let ex1 = $('wiki')
        .split('$page:STRING', 'Page')
        .filter('$Page.contains("hello world")');

      let ex2 = $('wiki')
        .filter('$page:STRING.contains("hello world")')
        .split('$page:STRING', 'Page');

      simplifiesTo(ex1, ex2);
    });

    it('can not move past a non linear split', () => {
      let ex1 = $('wiki')
        .split('$page:SET/STRING', 'Page')
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)')
        .filter('$Page.contains("hello world")');

      let ex2 = $('wiki')
        .split('$page:SET/STRING', 'Page')
        .filter('$Page.contains("hello world")')
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('can move past a fancy split', () => {
      let ex1 = $('wiki')
        .split('$time.timeBucket(P1D)', 'TimeByDay')
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)')
        .filter('$TimeByDay != null');

      let ex2 = $('wiki')
        .filter('$time.timeBucket(P1D) != null')
        .split('$time.timeBucket(P1D)', 'TimeByDay')
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('can move past a sort', () => {
      let ex1 = $('d')
        .sort('$deleted', 'ascending')
        .filter('$^AddedByDeleted == 1');

      let ex2 = $('d')
        .filter('$^AddedByDeleted == 1')
        .sort('$deleted', 'ascending');

      simplifiesTo(ex1, ex2);
    });
  });


  describe('split', () => {
    it('does not touch a split on a reference', () => {
      let ex1 = $('d').split('$page', 'Page', 'data');
      let ex2 = $('d').split('$page', 'Page', 'data');
      simplifiesTo(ex1, ex2);
    });

    it('simplifies the split expression', () => {
      let ex1 = $('d').split('$x.absolute().absolute()', 'Page', 'data');
      let ex2 = $('d').split('$x.absolute()', 'Page', 'data');
      simplifiesTo(ex1, ex2);
    });

    it('simplifies on empty literal', () => {
      let ex1 = ply().split('$x', 'Page', 'data');
      let ex2 = ply(Dataset.fromJS([
        { Page: null }
      ]));
      simplifiesTo(ex1, ex2);
    });

    it('simplifies on non-empty literal', () => {
      let ex1 = ply(Dataset.fromJS([
        { a: 1, b: 10 },
        { a: 1, b: 20 },
        { a: 2, b: 30 }
      ])).split('$a', 'A', 'data');

      let ex2 = ply(Dataset.fromJS([
        { A: 1 },
        { A: 2 }
      ]));

      simplifiesTo(ex1, ex2);
    });
  });


  describe('apply', () => {
    it('removes no-op applies', () => {
      let ex1 = ply()
        .apply('x', '$x');

      let ex2 = ply();

      simplifiesTo(ex1, ex2);
    });

    it('sorts applies does not mess with sort if all are simple 1', () => {
      let ex1 = ply()
        .apply('Count', '$^wiki.count()')
        .apply('Deleted', '$^wiki.sum($deleted)');

      let ex2 = ply()
        .apply('Count', '$^wiki.count()')
        .apply('Deleted', '$^wiki.sum($deleted)');

      simplifiesTo(ex1, ex2);
    });

    it('sorts applies does not mess with sort if all are simple 2', () => {
      let ex1 = ply()
        .apply('Deleted', '$^wiki.sum($deleted)')
        .apply('Count', '$^wiki.count()');

      let ex2 = ply()
        .apply('Deleted', '$^wiki.sum($deleted)')
        .apply('Count', '$^wiki.count()');

      simplifiesTo(ex1, ex2);
    });

    it('sorts applies 2', () => {
      let ex1 = ply()
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)')
        .apply('Deleted', '$^wiki.sum($deleted)');

      let ex2 = ply()
        .apply('Deleted', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('applies simple', () => {
      let ex1 = ply()
        .apply('Stuff', 5)
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)');

      let ex2 = ply(Dataset.fromJS([{
        Stuff: 5
      }]))
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)');

      simplifiesTo(ex1, ex2);
    });

    it('applies more complex', () => {
      let ex1 = ply(Dataset.fromJS([{
        Stuff: 5
      }]))
        .apply('StuffX3', '$Stuff * 3');

      let ex2 = ply(Dataset.fromJS([{
        Stuff: 5,
        StuffX3: 15
      }]));

      simplifiesTo(ex1, ex2);
    });

    it('applies more complex', () => {
      let ex1 = ply(Dataset.fromJS([{
        Stuff: 5
      }]))
        .apply('StuffX3', '$Stuff * 3');

      let ex2 = ply(Dataset.fromJS([{
        Stuff: 5,
        StuffX3: 15
      }]));

      simplifiesTo(ex1, ex2);
    });

    it.skip('applies externals', () => {
      let diamondEx = new ExternalExpression({ external: diamonds });

      let ex1 = ply()
        .apply('diamonds', diamondEx)
        .apply('Total', '$diamonds.count()')
        .apply('TotalX2', '$Total * 2')
        .apply('SomeSplit', $('diamonds').split('$cut:STRING', 'Cut').limit(10))
        .apply('SomeNestedSplit',
          $('diamonds').split('$color:STRING', 'Color')
            .limit(10)
            .apply('SubSplit', $('diamonds').split('$cut:STRING', 'SubCut').limit(5))
        );

      let ex2 = ex1.simplify();
      let data = ex2.value.data;
      expect(data.length).to.equal(1);
      expect(data[0].diamonds.mode).to.equal('raw');
      expect(data[0].Total.mode).to.equal('value');
      expect(data[0].TotalX2.mode).to.equal('value');
      expect(data[0].SomeSplit.mode).to.equal('split');
      expect(data[0].SomeNestedSplit.op).to.equal('chain');
      expect(data[0].SomeNestedSplit.actions.length).to.equal(1);
    });

  });


  describe('sort', () => {
    it('consecutive identical sorts fold together', () => {
      let ex1 = $('main')
        .sort('$x', 'descending')
        .sort('$x', 'ascending');

      let ex2 = $('main')
        .sort('$x', 'ascending');

      simplifiesTo(ex1, ex2);
    });

    it('works on literal', () => {
      let ex1 = ply().sort('$x', 'ascending');
      let ex2 = ply();
      simplifiesTo(ex1, ex2);
    });
  });


  describe('limit', () => {
    it('consecutive limits fold together', () => {
      let ex1 = $('main')
        .limit(10)
        .limit(20);

      let ex2 = $('main')
        .limit(10);

      simplifiesTo(ex1, ex2);
    });

    it('removes infinite, no-op limit', () => {
      let ex1 = $('main')
        .limit(Infinity);

      let ex2 = $('main');

      simplifiesTo(ex1, ex2);
    });

    it('moves past apply', () => {
      let ex1 = $('main')
        .apply('Wiki', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)')
        .limit(10);

      let ex2 = $('main')
        .limit(10)
        .apply('Wiki', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('works on literals', () => {
      let ex1 = ply().limit(20);
      let ex2 = ply();
      simplifiesTo(ex1, ex2);
    });
  });


  describe('select', () => {
    it('consecutive selects fold together', () => {
      let ex1 = $('main')
        .select('a', 'b')
        .select('a', 'c');

      let ex2 = $('main')
        .select('a');

      simplifiesTo(ex1, ex2);
    });

    it('removes a preceding apply', () => {
      let ex1 = $('main')
        .apply('Added', '$^wiki.sum($added)')
        .apply('Deleted', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)')
        .select('Added', 'Deleted');

      let ex2 = $('main')
        .apply('Added', '$^wiki.sum($added)')
        .apply('Deleted', '$^wiki.sum($deleted)')
        .select('Added', 'Deleted');

      simplifiesTo(ex1, ex2);
    });
  });


  describe('concat', () => {
    it('removes empty strings', () => {
      let ex1 = r('').concat('$x', r(''));
      let ex2 = $('x');

      simplifiesTo(ex1, ex2);
    });

    it('concatenates literal', () => {
      let ex1 = r('p_').concat('hello', '$x', 'i_', 'love');
      let ex2 = r('p_hello').concat('$x', 'i_love');

      simplifiesTo(ex1, ex2);
    });
  });


  describe('lookup', () => {
    it('does not touch a lookup on a literal', () => {
      let ex1 = r('hello').lookup('hello_lookup');
      leavesAlone(ex1);
    });
  });
});
