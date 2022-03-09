/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2020 Imply Data, Inc.
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

const { expect } = require('chai');

const plywood = require('../plywood');

const {
  Expression,
  TimeRange,
  NumberRange,
  StringRange,
  $,
  r,
  ply,
  Set,
  Dataset,
  External,
  ExternalExpression,
} = plywood;

function simplifiesTo(ex1, ex2) {
  const ex1Simple = ex1.simplify();
  expect(ex1Simple.simple, 'simplified version must be simple').to.equal(true);
  expect(ex1Simple.toJS(), 'must be the same').to.deep.equal(ex2.toJS());
}

function leavesAlone(ex) {
  simplifiesTo(ex, ex);
}

const diamonds = External.fromJS({
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
    { name: 'carat', type: 'NUMBER', nativeType: 'STRING' },
    { name: 'height_bucket', type: 'NUMBER' },
    { name: 'price', type: 'NUMBER', unsplitable: true },
    { name: 'tax', type: 'NUMBER', unsplitable: true },
    { name: 'vendor_id', type: 'NULL', nativeType: 'hyperUnique', unsplitable: true },
  ],
  allowSelectQueries: true,
});

describe('Simplify', () => {
  describe('literals', () => {
    it('simplifies to number', () => {
      const ex1 = r(5).add(1).subtract(4);
      const ex2 = r(2);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies literal prefix', () => {
      const ex1 = r(5).add(1).subtract(4).multiply('$x');
      const ex2 = $('x').multiply(2);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies cast', () => {
      const ex1 = r(1447430881000).cast('TIME');
      const ex2 = r(new Date('2015-11-13T16:08:01.000Z'));
      simplifiesTo(ex1, ex2);
    });

    it('simplifies double cast', () => {
      const ex1 = $('time', 'TIME').cast('TIME').cast('TIME');
      const ex2 = $('time', 'TIME');
      simplifiesTo(ex1, ex2);
    });

    it('simplifies string cast', () => {
      const ex1 = r('blah').cast('STRING');
      const ex2 = r('blah');
      simplifiesTo(ex1, ex2);
    });

    it('str.indexOf(substr) > -1 should simplify to CONTAINS(str, substr)', () => {
      const ex1 = $('page').indexOf('sdf').greaterThan(-1);
      const ex2 = $('page').contains('sdf');
      simplifiesTo(ex1, ex2);
    });

    it('str.indexOf(substr) >= 0 should simplify to CONTAINS(str, substr)', () => {
      const ex1 = $('page').indexOf('sdf').greaterThanOrEqual(0);
      const ex2 = $('page').contains('sdf');
      simplifiesTo(ex1, ex2);
    });

    it('str.indexOf(substr) < 1 should not simplify to contains', () => {
      const ex1 = $('page').indexOf('sdf').lessThan(1);
      const ex2 = $('page')
        .indexOf('sdf')
        .overlap(new NumberRange({ start: null, end: 1, bounds: '()' }));
      simplifiesTo(ex1, ex2);
    });

    it('str.indexOf(substr) != -1 should simplify to CONTAINS(str, substr)', () => {
      const ex1 = $('page').indexOf('sdf').isnt(-1);
      const ex2 = $('page').contains('sdf');
      simplifiesTo(ex1, ex2);
    });

    it('str.indexOf(substr) == -1 should simplify to str.contains(substr).not()', () => {
      const ex1 = $('page').indexOf('sdf').is(-1);
      const ex2 = $('page').contains('sdf').not();
      simplifiesTo(ex1, ex2);
    });

    it('chained transform case simplifies to last one', () => {
      const ex1 = $('page')
        .transformCase('lowerCase')
        .transformCase('upperCase')
        .transformCase('lowerCase')
        .transformCase('upperCase');
      const ex2 = $('page').transformCase('upperCase');
      simplifiesTo(ex1, ex2);
    });

    it('transform case is idempotent', () => {
      const ex1 = $('page').transformCase('lowerCase').transformCase('lowerCase');
      const ex2 = $('page').transformCase('lowerCase');
      simplifiesTo(ex1, ex2);
    });
  });

  describe('leaves', () => {
    it('does not touch a lookup on a literal', () => {
      const ex = $('city')
        .contains('San')
        .and($('city').is(r('San Francisco')));
      leavesAlone(ex);
    });
  });

  describe('add', () => {
    it('removes 0 in simple case', () => {
      const ex1 = $('x').add(0);
      const ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it('removes 0 complex case', () => {
      const ex1 = $('x').add(0, '$y', 0, '$z');
      const ex2 = $('x').add('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it('removes leading 0', () => {
      const ex1 = r(0).add('$y', '$z');
      const ex2 = $('y').add('$z');
      simplifiesTo(ex1, ex2);
    });

    it('works in nested expression case', () => {
      const ex1 = $('x').add('0 + $y + 0 + $z');
      const ex2 = $('x').add('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it('works with nested add', () => {
      const ex1 = $('x').add('2 * $y + $z');
      const ex2 = $('x').add('$y * 2', '$z');
      simplifiesTo(ex1, ex2);
    });

    it('works with literals', () => {
      const ex1 = r(1).add('2 + $y + 30 + 40');
      const ex2 = $('y').add(73);
      simplifiesTo(ex1, ex2);
    });

    it('handles commutativity', () => {
      const ex1 = r(1).add($('x'));
      const ex2 = $('x').add(1);
      simplifiesTo(ex1, ex2);
    });

    it('handles associativity', () => {
      const ex1 = $('a').add($('b').add('$c'));
      const ex2 = $('a').add('$b').add('$c');
      simplifiesTo(ex1, ex2);
    });
  });

  describe('fallback', () => {
    it('removes self if else is null', () => {
      const ex1 = $('x').fallback(null);
      const ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it('removes self if null', () => {
      const ex1 = r(null).fallback('$x');
      const ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it('removes self if X = X', () => {
      const ex1 = $('x').fallback('$x');
      const ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it('removes self if X = X', () => {
      const ex1 = $('x').fallback('$x');
      const ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it('removes self if operand is literal', () => {
      const ex1 = r('hello').fallback('$x');
      const ex2 = r('hello');
      simplifiesTo(ex1, ex2);
    });
  });

  describe('power', () => {
    it('removes self if 1', () => {
      const ex1 = $('x').power(1);
      const ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it('removes self if 0', () => {
      const ex1 = $('x').power(0);
      const ex2 = r(1);
      simplifiesTo(ex1, ex2);
    });
  });

  describe.skip('negate', () => {
    it('collapses double', () => {
      const ex1 = $('x').negate().negate();
      const ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it('collapses long chain', () => {
      const ex1 = $('x').negate().negate().negate().negate().negate().negate().negate();
      const ex2 = $('x').negate();
      simplifiesTo(ex1, ex2);
    });
  });

  describe('multiply', () => {
    it('collapses 0 in simple case', () => {
      const ex1 = $('x').multiply(0);
      const ex2 = r(0);
      simplifiesTo(ex1, ex2);
    });

    it('collapses 0 in complex case', () => {
      const ex1 = $('x').multiply(6, '$y', 0, '$z');
      const ex2 = r(0);
      simplifiesTo(ex1, ex2);
    });

    it('collapses leading 0', () => {
      const ex1 = r(0).multiply(6, '$y', '$z');
      const ex2 = r(0);
      simplifiesTo(ex1, ex2);
    });

    it('removes 1 in simple case', () => {
      const ex1 = $('x').multiply(1);
      const ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it('removes 1 complex case', () => {
      const ex1 = $('x').multiply(1, '$y', 1, '$z');
      const ex2 = $('x').multiply('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it('removes leading 1', () => {
      const ex1 = r(1).multiply('$y', '$z');
      const ex2 = $('y').multiply('$z');
      simplifiesTo(ex1, ex2);
    });

    it('works in nested expression case', () => {
      const ex1 = $('x').multiply('1 * $y * 1 * $z');
      const ex2 = $('x').multiply('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it('works with nested add', () => {
      const ex1 = $('x').multiply('(1 + $y) * $z');
      const ex2 = $('x').multiply('$y + 1', '$z');
      simplifiesTo(ex1, ex2);
    });

    it.skip('works with trailing literals', () => {
      const ex1 = $('x').multiply(3).multiply(3);
      const ex2 = $('x').multiply(9);
      simplifiesTo(ex1, ex2);
    });
  });

  describe('multiply', () => {
    it('collapses / 0', () => {
      const ex1 = $('x').divide(0);
      const ex2 = Expression.NULL;
      simplifiesTo(ex1, ex2);
    });

    it('removes 1 in simple case', () => {
      const ex1 = $('x').multiply(1);
      const ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });
  });

  describe('and', () => {
    it('collapses false in simple case', () => {
      const ex1 = $('x').and(false);
      const ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('collapses false in complex case', () => {
      const ex1 = $('x').and('$y', false, '$z');
      const ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('collapses leading false', () => {
      const ex1 = r(false).and('$y', '$z');
      const ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('removes true in simple case', () => {
      const ex1 = $('x').and(true);
      const ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it('removes true complex case', () => {
      const ex1 = $('x').and(true, '$y', true, '$z');
      const ex2 = $('x').and('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it('removes leading true', () => {
      const ex1 = r(true).and('$y', '$z');
      const ex2 = $('y').and('$z');
      simplifiesTo(ex1, ex2);
    });

    it('works in nested expression case', () => {
      const ex1 = $('x').and('true and $y and true and $z');
      const ex2 = $('x').and('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it('works with nested or', () => {
      const ex1 = $('x').and('($a or $b) and $z');
      const ex2 = $('x').and('$a or $b', '$z');
      simplifiesTo(ex1, ex2);
    });

    it('works with different filters', () => {
      const ex1 = $('flight', 'NUMBER').is(5).and($('flight', 'NUMBER').is(7));
      const ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('works with different filters across filter', () => {
      const ex1 = $('flight', 'NUMBER').is(5).and($('lol').is(3)).and($('flight', 'NUMBER').is(7));
      const ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('works with same filters', () => {
      const ex1 = $('flight', 'NUMBER').is(5).and($('flight', 'NUMBER').is(5));
      const ex2 = $('flight', 'NUMBER').is(5);
      simplifiesTo(ex1, ex2);
    });

    it('works with same filters across filter', () => {
      const ex1 = $('flight', 'NUMBER').is(5).and($('lol').is(3)).and($('flight', 'NUMBER').is(5));
      const ex2 = $('flight', 'NUMBER').is(5).and($('lol').is(3));
      simplifiesTo(ex1, ex2);
    });

    it('leaves types filters 1', () => {
      const ex1 = $('flight').is(5).and($('x').is(1)).and($('flight').is(7));
      leavesAlone(ex1);
    });

    it('leaves NULL types', () => {
      const ex1 = $('uc', 'NULL').is('A').and($('uc', 'NULL').is('B'));
      leavesAlone(ex1);
    });

    it('re-arranges filters 2', () => {
      const ex1 = $('flight').is(5).and($('x').is(1)).and($('flight').is(5));
      const ex2 = $('flight').is(5).and($('x').is(1));
      simplifiesTo(ex1, ex2);
    });

    it('works with IS and OVERLAP (with types)', () => {
      const ex1 = $('flight', 'NUMBER')
        .is(5)
        .and($('flight', 'NUMBER').overlap({ start: 5, end: 7 }));
      const ex2 = $('flight', 'NUMBER').is(5);
      simplifiesTo(ex1, ex2);
    });

    it('leaves IS and OVERLAP (without types)', () => {
      const ex1 = $('flight')
        .is(5)
        .and($('flight').overlap({ start: 5, end: 7 }));
      leavesAlone(ex1);
    });

    it('works with two number ranges', () => {
      const ex1 = $('x', 'NUMBER')
        .overlap({ start: 1, end: 5 })
        .and($('x', 'NUMBER').overlap({ start: 1, end: 2 }));
      const ex2 = $('x', 'NUMBER').overlap({ start: 1, end: 2 });
      simplifiesTo(ex1, ex2);
    });

    it('works with two time ranges', () => {
      const ex1 = $('time', 'TIME')
        .overlap({ start: new Date('2015-03-12T00:00:00Z'), end: new Date('2015-03-16T00:00:00Z') })
        .and(
          $('time', 'TIME').overlap({
            start: new Date('2015-03-12T00:00:00Z'),
            end: new Date('2015-03-13T00:00:00Z'),
          }),
        );
      const ex2 = $('time', 'TIME').overlap({
        start: new Date('2015-03-12T00:00:00Z'),
        end: new Date('2015-03-13T00:00:00Z'),
      });
      simplifiesTo(ex1, ex2);
    });

    it('works with time range to overlap statement', () => {
      const ex1 = $('time', 'TIME')
        .greaterThan(r(new Date('2015-11-13T16:08:01.000Z')))
        .and($('time', 'TIME').lessThan(r(new Date('2019-01-14T01:54:41.000Z'))));
      const ex2 = $('time', 'TIME').overlap(
        new NumberRange({
          start: new Date('2015-11-13T16:08:01.000Z'),
          end: new Date('2019-01-14T01:54:41.000Z'),
          bounds: '()',
        }),
      );
      simplifiesTo(ex1, ex2);
    });

    it('works with string overlaps', () => {
      const ex1 = $('cityName', 'STRING')
        .greaterThan('Kab')
        .and($('cityName', 'STRING').lessThan('Kar'));
      const ex2 = $('cityName', 'STRING').overlap({ start: 'Kab', end: 'Kar', bounds: '()' });
      simplifiesTo(ex1, ex2);
    });

    it('removes a timeBucket', () => {
      const largeInterval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-09Z'),
      });
      const smallInterval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-03Z'),
      });
      const ex1 = $('time', 'TIME')
        .overlap(largeInterval)
        .and($('time', 'TIME').timeBucket('P1D', 'Etc/UTC').is(smallInterval));
      const ex2 = $('time', 'TIME').overlap(smallInterval);
      simplifiesTo(ex1, ex2);
    });

    it('works with match', () => {
      const ex1 = $('cityName').match('San').and($('cityName').match('Hello'));
      simplifiesTo(ex1, ex1);
    });

    it('works with same expression', () => {
      const ex1 = $('cityName').match('San').and($('cityName').match('San'));
      const ex2 = $('cityName').match('San');
      simplifiesTo(ex1, ex2);
    });
  });

  describe('or', () => {
    it('collapses true in simple case', () => {
      const ex1 = $('x').or(true);
      const ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it('collapses true in complex case', () => {
      const ex1 = $('x').or('$y', true, '$z');
      const ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it('collapses leading true', () => {
      const ex1 = r(true).or('$y', '$z');
      const ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it('removes false in simple case', () => {
      const ex1 = $('x').or(false);
      const ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it('removes false complex case', () => {
      const ex1 = $('x').or(false, '$y', false, '$z');
      const ex2 = $('x').or('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it('removes leading false', () => {
      const ex1 = r(false).or('$y', '$z');
      const ex2 = $('y').or('$z');
      simplifiesTo(ex1, ex2);
    });

    it('works in nested expression case', () => {
      const ex1 = $('x').or('false or $y or false or $z');
      const ex2 = $('x').or('$y', '$z');
      simplifiesTo(ex1, ex2);
    });

    it('works with nested and', () => {
      const ex1 = $('x').or('($a and $b) or $z');
      const ex2 = $('x').or('$a and $b', '$z');
      simplifiesTo(ex1, ex2);
    });

    it('works with different filters', () => {
      const ex1 = $('flight').is(5).or($('flight').is(7));
      const ex2 = $('flight').is([5, 7]);
      simplifiesTo(ex1, ex2);
    });

    it('works with same filters', () => {
      const ex1 = $('flight').is(5).or($('flight').is(5));
      const ex2 = $('flight').is(5);
      simplifiesTo(ex1, ex2);
    });

    it('works with IS and OVERLAP', () => {
      const ex1 = $('flight')
        .is(5)
        .or($('flight').overlap({ start: 5, end: 7 }));
      const ex2 = $('flight').overlap({ start: 5, end: 7 });
      simplifiesTo(ex1, ex2);
    });

    it('re-arranges filters 1', () => {
      const ex1 = $('flight').is(5).or($('x').is(1)).or($('flight').is(7));
      const ex2 = $('flight').is([5, 7]).or($('x').is(1));
      simplifiesTo(ex1, ex2);
    });

    it('re-arranges filters 2', () => {
      const ex1 = $('flight').is(5).or($('x').is(1)).or($('flight').is(5));
      const ex2 = $('flight').is(5).or($('x').is(1));
      simplifiesTo(ex1, ex2);
    });

    it('works with match', () => {
      const ex1 = $('cityName').match('San').or($('cityName').match('Hello'));
      simplifiesTo(ex1, ex1);
    });

    it('works with same expression', () => {
      const ex1 = $('cityName').match('San').or($('cityName').match('San'));
      const ex2 = $('cityName').match('San');
      simplifiesTo(ex1, ex2);
    });
  });

  describe('not', () => {
    it('works on literal', () => {
      const ex1 = r(false).not();
      const ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it('collapses double', () => {
      const ex1 = $('x').not().not();
      const ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it('collapses long chain', () => {
      const ex1 = $('x').not().not().not().not().not().not().not();
      const ex2 = $('x').not();
      simplifiesTo(ex1, ex2);
    });
  });

  describe('is', () => {
    it('simplifies to false', () => {
      const ex1 = r(5).is(8);
      const ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies to true with simple datatypes', () => {
      const ex1 = r(5).is(5);
      const ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies to true with complex datatypes', () => {
      const ex1 = r(
        TimeRange.fromJS({
          start: new Date('2016-01-02Z'),
          end: new Date('2016-01-03Z'),
        }),
      ).is(
        TimeRange.fromJS({
          start: new Date('2016-01-02Z'),
          end: new Date('2016-01-03Z'),
        }),
      );
      const ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies to true', () => {
      const ex1 = $('x').is('$x');
      const ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it('swaps yoda literal (with ref)', () => {
      const ex1 = r('Honda').is('$x');
      const ex2 = $('x').is('Honda');
      simplifiesTo(ex1, ex2);
    });

    it('swaps yoda literal (with complex)', () => {
      const ex1 = r('Dhello').is($('color').concat(r('hello')));
      const ex2 = $('color').concat(r('hello')).is(r('Dhello'));
      simplifiesTo(ex1, ex2);
    });

    it('removes a timeBucket', () => {
      const interval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-03Z'),
      });
      const ex1 = $('time').timeBucket('P1D', 'Etc/UTC').is(interval);
      const ex2 = $('time').overlap(interval);
      simplifiesTo(ex1, ex2);
    });

    it('does not remove a timeBucket with no timezone', () => {
      const interval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-03Z'),
      });
      const ex = $('time').timeBucket('P1D').is(interval);
      expect(ex.simplify().toJS()).to.deep.equal(ex.toJS());
    });

    it('kills impossible timeBucket (no start)', () => {
      const interval = TimeRange.fromJS({
        start: null,
        end: new Date('2016-01-03Z'),
      });
      const ex1 = $('time').timeBucket('P1D', 'Etc/UTC').is(interval);
      const ex2 = Expression.FALSE;
      simplifiesTo(ex1, ex2);
    });

    it('kills impossible timeBucket (not aligned)', () => {
      const interval = TimeRange.fromJS({
        start: new Date('2016-01-02Z'),
        end: new Date('2016-01-04Z'),
      });
      const ex1 = $('time').timeBucket('P1D', 'Etc/UTC').is(interval);
      const ex2 = Expression.FALSE;
      simplifiesTo(ex1, ex2);
    });

    it('removes a numberBucket', () => {
      const interval = NumberRange.fromJS({
        start: 1,
        end: 6,
      });
      const ex1 = $('num').numberBucket(5, 1).is(interval);
      const ex2 = $('num').overlap(interval);
      simplifiesTo(ex1, ex2);
    });

    it('removes a numberBucket with 0 start', () => {
      const interval = NumberRange.fromJS({
        start: 0,
        end: 5,
      });
      const ex1 = $('num').numberBucket(5, 0).is(interval);
      const ex2 = $('num').overlap(interval);
      simplifiesTo(ex1, ex2);
    });

    it('kills impossible numberBucket (no start)', () => {
      const interval = NumberRange.fromJS({
        start: null,
        end: 6,
      });
      const ex1 = $('time').numberBucket(5, 1).is(interval);
      const ex2 = Expression.FALSE;
      simplifiesTo(ex1, ex2);
    });

    it('kills impossible numberBucket (not aligned)', () => {
      const interval = NumberRange.fromJS({
        start: 2,
        end: 7,
      });
      const ex1 = $('time').numberBucket(5, 1).is(interval);
      const ex2 = Expression.FALSE;
      simplifiesTo(ex1, ex2);
    });

    it('leaves possible fallback', () => {
      const ex1 = $('color').fallback('D').is('D');
      const ex2 = $('color').fallback('D').is('D');
      simplifiesTo(ex1, ex2);
    });

    it('kills .then() 1', () => {
      const ex1 = $('color').then('T').is('T');
      const ex2 = $('color').is(true);
      simplifiesTo(ex1, ex2);
    });

    it('kills .then() 2', () => {
      const ex1 = $('color').then('T').is('F');
      const ex2 = $('color').isnt(true);
      simplifiesTo(ex1, ex2);
    });

    it('leaves with lookup', () => {
      const ex = $('channel').lookup('channel-lookup').fallback(r('LOL')).is(['English', 'LOL']);
      leavesAlone(ex);
    });
  });

  describe('in', () => {
    it('simplifies when singleton set', () => {
      const ex1 = $('x', 'STRING').in(['A']);
      const ex2 = $('x', 'STRING').is('A');
      simplifiesTo(ex1, ex2);
    });
  });

  describe('overlap', () => {
    it('swaps yoda literal (with ref)', () => {
      const someSet = Set.fromJS([
        NumberRange.fromJS({ start: 1, end: 2 }),
        NumberRange.fromJS({ start: 3, end: 4 }),
      ]);
      const ex1 = r(someSet).overlap('$x');
      const ex2 = $('x').overlap(someSet);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies when empty set (lhs)', () => {
      const ex1 = r([]).overlap('$x');
      const ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies when empty set (rhs)', () => {
      const ex1 = $('x').overlap([]);
      const ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies to IS', () => {
      const someSet = Set.fromJS(['A', 'B', 'C']);
      const ex1 = $('x', 'STRING').overlap(someSet);
      const ex2 = $('x', 'STRING').is(someSet);
      simplifiesTo(ex1, ex2);
    });

    it('simplifies to singleton IS', () => {
      const ex1 = $('x', 'STRING').overlap(Set.fromJS(['A']));
      const ex2 = $('x', 'STRING').is('A');
      simplifiesTo(ex1, ex2);
    });

    it('simplifies when set can be unified', () => {
      const ex1 = $('x', 'NUMBER').overlap(
        Set.fromJS({
          setType: 'NUMBER_RANGE',
          elements: [
            { start: 1, end: 3 },
            { start: 2, end: 5 },
          ],
        }),
      );
      const ex2 = $('x', 'NUMBER').overlap({ start: 1, end: 5 });
      simplifiesTo(ex1, ex2);
    });
  });

  describe('match', () => {
    it('with false value', () => {
      const ex1 = r('Honda').match('^\\d+');
      const ex2 = r(false);
      simplifiesTo(ex1, ex2);
    });

    it('with true value', () => {
      const ex1 = r('123').match('^\\d+');
      const ex2 = r(true);
      simplifiesTo(ex1, ex2);
    });

    it('with reference value', () => {
      const ex1 = $('test').match('^\\d+');
      const ex2 = $('test').match('^\\d+');
      simplifiesTo(ex1, ex2);
    });
  });

  describe('substr', () => {
    it('works with length 0', () => {
      const ex1 = $('x').substr(0, 0);
      const ex2 = r('');
      simplifiesTo(ex1, ex2);
    });
  });

  describe('contains', () => {
    it('works with transformCase Upper', () => {
      const ex1 = $('x').transformCase('upperCase').contains($('y').transformCase('upperCase'));
      const ex2 = $('x').contains($('y'), 'ignoreCase');
      simplifiesTo(ex1, ex2);
    });

    it('works with transformCase Lower', () => {
      const ex1 = $('x').transformCase('lowerCase').contains($('y').transformCase('lowerCase'));
      const ex2 = $('x').contains($('y'), 'ignoreCase');
      simplifiesTo(ex1, ex2);
    });

    it('works removes useless ignoreCase', () => {
      const ex1 = $('x').contains(r('[['), 'ignoreCase');
      const ex2 = $('x').contains(r('[['));
      simplifiesTo(ex1, ex2);
    });

    it('works removes useless ignoreCase', () => {
      const ex1 = $('x').contains('xxx', 'ignoreCase');
      leavesAlone(ex1);
    });
  });

  describe('timeFloor', () => {
    it('with simple expression', () => {
      const ex1 = r(new Date('2015-02-20T15:41:12Z')).timeFloor('P1D', 'Etc/UTC');
      const ex2 = r(new Date('2015-02-20T00:00:00Z'));
      simplifiesTo(ex1, ex2);
    });

    it('wipes out itself', () => {
      const ex1 = $('x').timeFloor('P1D', 'Etc/UTC').timeFloor('P1D', 'Etc/UTC');
      const ex2 = $('x').timeFloor('P1D', 'Etc/UTC');
      simplifiesTo(ex1, ex2);
    });
  });

  describe('timeShift', () => {
    it('with simple expression', () => {
      const ex1 = r(new Date('2015-02-20T15:41:12Z')).timeShift('P1D', 1, 'Etc/UTC');
      const ex2 = r(new Date('2015-02-21T15:41:12Z'));
      simplifiesTo(ex1, ex2);
    });

    it('shifts 0', () => {
      const ex1 = $('x').timeShift('P1D', 0, 'Etc/UTC');
      const ex2 = $('x');
      simplifiesTo(ex1, ex2);
    });

    it('combines with itself', () => {
      const ex1 = $('x').timeShift('P1D', 10, 'Etc/UTC').timeShift('P1D', -7, 'Etc/UTC');
      const ex2 = $('x').timeShift('P1D', 3, 'Etc/UTC');
      simplifiesTo(ex1, ex2);
    });
  });

  describe('timeBucket', () => {
    it('with simple expression', () => {
      const ex1 = r(new Date('2015-02-19T05:59:02.822Z')).timeBucket('P1D', 'Etc/UTC');
      const ex2 = r(
        TimeRange.fromJS({
          start: new Date('2015-02-19T00:00:00.000Z'),
          end: new Date('2015-02-20T00:00:00.000Z'),
        }),
      );
      simplifiesTo(ex1, ex2);
    });
  });

  describe('numberBucket', () => {
    it('with simple expression', () => {
      const ex1 = r(1.03).numberBucket(0.05, 0.02);
      const ex2 = r(
        NumberRange.fromJS({
          start: 1.02,
          end: 1.07,
        }),
      );
      simplifiesTo(ex1, ex2);
    });
  });

  describe('filter', () => {
    it('folds with literal', () => {
      const ex1 = ply(Dataset.fromJS([{ x: 1 }, { x: 2 }])).filter('$x == 2');

      const ex2 = ply(Dataset.fromJS([{ x: 2 }]));

      simplifiesTo(ex1, ex2);
    });

    it('consecutive filters fold together', () => {
      const ex1 = ply().filter('$^x == 1').filter('$^y == 2');

      const ex2 = ply().filter('$^x == 1 and $^y == 2');

      simplifiesTo(ex1, ex2);
    });

    it('moves filter before applies', () => {
      const ex1 = ply()
        .apply('Wiki', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)')
        .filter('$^x == "en"');

      const ex2 = ply()
        .filter('$^x == "en"')
        .apply('Wiki', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('does not change the meaning', () => {
      const ex1 = ply()
        .apply('Wiki', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)')
        .filter('$AddedByDeleted == 1');

      const ex2 = ply()
        .apply('Wiki', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .filter('$AddedByDeleted == 1')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('can move past a linear split', () => {
      const ex1 = $('wiki').split('$page:STRING', 'Page').filter('$Page.contains("hello world")');

      const ex2 = $('wiki')
        .filter('$page:STRING.contains("hello world")')
        .split('$page:STRING', 'Page');

      simplifiesTo(ex1, ex2);
    });

    it('can not move past a non linear split', () => {
      const ex1 = $('wiki')
        .split('$page:SET/STRING', 'Page')
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)')
        .filter('$Page.contains("hello world")');

      const ex2 = $('wiki')
        .split('$page:SET/STRING', 'Page')
        .filter('$Page.contains("hello world")')
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('can move past a fancy split', () => {
      const ex1 = $('wiki')
        .split('$time.timeBucket(P1D)', 'TimeByDay')
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)')
        .filter('$TimeByDay != null');

      const ex2 = $('wiki')
        .filter('$time.timeBucket(P1D) != null')
        .split('$time.timeBucket(P1D)', 'TimeByDay')
        .apply('Deleted', '$wiki.sum($deleted)')
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('can move past a sort', () => {
      const ex1 = $('d').sort('$deleted', 'ascending').filter('$^AddedByDeleted == 1');

      const ex2 = $('d').filter('$^AddedByDeleted == 1').sort('$deleted', 'ascending');

      simplifiesTo(ex1, ex2);
    });
  });

  describe('split', () => {
    it('does not touch a split on a reference', () => {
      const ex1 = $('d').split('$page', 'Page', 'data');
      const ex2 = $('d').split('$page', 'Page', 'data');
      simplifiesTo(ex1, ex2);
    });

    it('simplifies the split expression', () => {
      const ex1 = $('d').split('$x.absolute().absolute()', 'Page', 'data');
      const ex2 = $('d').split('$x.absolute()', 'Page', 'data');
      simplifiesTo(ex1, ex2);
    });

    it('simplifies on empty literal', () => {
      const ex1 = ply().split('$x', 'Page', 'data');
      expect(ex1.simplify().toJS()).to.deep.equal({
        op: 'literal',
        type: 'DATASET',
        value: {
          attributes: [
            {
              name: 'Page',
              type: 'STRING',
            },
            {
              name: 'data',
              type: 'DATASET',
            },
          ],
          data: [
            {
              Page: null,
            },
          ],
          keys: ['Page'],
        },
      });
    });

    it('simplifies on non-empty literal', () => {
      const ex1 = ply(
        Dataset.fromJS([
          { a: 1, b: 10 },
          { a: 1, b: 20 },
          { a: 2, b: 30 },
        ]),
      ).split('$a', 'A', 'data');

      expect(ex1.simplify().toJS()).to.deep.equal({
        op: 'literal',
        type: 'DATASET',
        value: {
          attributes: [
            {
              name: 'A',
              type: 'NUMBER',
            },
            {
              name: 'data',
              type: 'DATASET',
            },
          ],
          data: [
            {
              A: 1,
            },
            {
              A: 2,
            },
          ],
          keys: ['A'],
        },
      });
    });
  });

  describe('apply', () => {
    it('removes no-op applies', () => {
      const ex1 = ply().apply('x', '$x');

      const ex2 = ply();

      simplifiesTo(ex1, ex2);
    });

    it('sorts applies does not mess with sort if all are simple 1', () => {
      const ex1 = ply().apply('Count', '$^wiki.count()').apply('Deleted', '$^wiki.sum($deleted)');

      const ex2 = ply().apply('Count', '$^wiki.count()').apply('Deleted', '$^wiki.sum($deleted)');

      simplifiesTo(ex1, ex2);
    });

    it('sorts applies does not mess with sort if all are simple 2', () => {
      const ex1 = ply().apply('Deleted', '$^wiki.sum($deleted)').apply('Count', '$^wiki.count()');

      const ex2 = ply().apply('Deleted', '$^wiki.sum($deleted)').apply('Count', '$^wiki.count()');

      simplifiesTo(ex1, ex2);
    });

    it('sorts applies 2', () => {
      const ex1 = ply()
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)')
        .apply('Deleted', '$^wiki.sum($deleted)');

      const ex2 = ply()
        .apply('Deleted', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('applies simple', () => {
      const ex1 = ply()
        .apply('Stuff', 5)
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)');

      const ex2 = ply(
        Dataset.fromJS({
          keys: [],
          data: [{ Stuff: 5 }],
        }),
      ).apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)');

      simplifiesTo(ex1, ex2);
    });

    it('applies more complex', () => {
      const ex1 = ply(
        Dataset.fromJS([
          {
            Stuff: 5,
          },
        ]),
      ).apply('StuffX3', '$Stuff * 3');

      const ex2 = ply(
        Dataset.fromJS([
          {
            Stuff: 5,
            StuffX3: 15,
          },
        ]),
      );

      simplifiesTo(ex1, ex2);
    });

    it('applies more complex', () => {
      const ex1 = ply(
        Dataset.fromJS([
          {
            Stuff: 5,
          },
        ]),
      ).apply('StuffX3', '$Stuff * 3');

      const ex2 = ply(
        Dataset.fromJS([
          {
            Stuff: 5,
            StuffX3: 15,
          },
        ]),
      );

      simplifiesTo(ex1, ex2);
    });

    it.skip('applies externals', () => {
      const diamondEx = new ExternalExpression({ external: diamonds });

      const ex1 = ply()
        .apply('diamonds', diamondEx)
        .apply('Total', '$diamonds.count()')
        .apply('TotalX2', '$Total * 2')
        .apply('SomeSplit', $('diamonds').split('$cut:STRING', 'Cut').limit(10))
        .apply(
          'SomeNestedSplit',
          $('diamonds')
            .split('$color:STRING', 'Color')
            .limit(10)
            .apply('SubSplit', $('diamonds').split('$cut:STRING', 'SubCut').limit(5)),
        );

      const ex2 = ex1.simplify();
      const data = ex2.value.data;
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
      const ex1 = $('main').sort('$x', 'descending').sort('$x', 'ascending');

      const ex2 = $('main').sort('$x', 'ascending');

      simplifiesTo(ex1, ex2);
    });

    it('works on literal', () => {
      const ex1 = ply().sort('$x', 'ascending');
      const ex2 = ply();
      simplifiesTo(ex1, ex2);
    });
  });

  describe('limit', () => {
    it('consecutive limits fold together', () => {
      const ex1 = $('main').limit(10).limit(20);

      const ex2 = $('main').limit(10);

      simplifiesTo(ex1, ex2);
    });

    it('removes infinite, no-op limit', () => {
      const ex1 = $('main').limit(Infinity);

      const ex2 = $('main');

      simplifiesTo(ex1, ex2);
    });

    it('moves past apply', () => {
      const ex1 = $('main')
        .apply('Wiki', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)')
        .limit(10);

      const ex2 = $('main')
        .limit(10)
        .apply('Wiki', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)');

      simplifiesTo(ex1, ex2);
    });

    it('works on literals', () => {
      const ex1 = ply().limit(20);
      const ex2 = ply();
      simplifiesTo(ex1, ex2);
    });
  });

  describe('select', () => {
    it('consecutive selects fold together', () => {
      const ex1 = $('main').select('a', 'b').select('a', 'c');

      const ex2 = $('main').select('a');

      simplifiesTo(ex1, ex2);
    });

    it('removes a preceding apply', () => {
      const ex1 = $('main')
        .apply('Added', '$^wiki.sum($added)')
        .apply('Deleted', '$^wiki.sum($deleted)')
        .apply('AddedByDeleted', '$^wiki.sum($added) / $^wiki.sum($deleted)')
        .apply('DeletedByInserted', '$^wiki.sum($deleted) / $^wiki.sum($inserted)')
        .select('Added', 'Deleted');

      const ex2 = $('main')
        .apply('Added', '$^wiki.sum($added)')
        .apply('Deleted', '$^wiki.sum($deleted)')
        .select('Added', 'Deleted');

      simplifiesTo(ex1, ex2);
    });
  });

  describe('concat', () => {
    it('removes empty strings', () => {
      const ex1 = r('').concat('$x', r(''));
      const ex2 = $('x');

      simplifiesTo(ex1, ex2);
    });

    it('concatenates literal', () => {
      const ex1 = r('p_').concat('hello', '$x', 'i_', 'love');
      const ex2 = r('p_hello').concat('$x', 'i_love');

      simplifiesTo(ex1, ex2);
    });
  });

  describe('lookup', () => {
    it('does not touch a lookup on a literal', () => {
      const ex1 = r('hello').lookup('hello_lookup');
      leavesAlone(ex1);
    });
  });
});
