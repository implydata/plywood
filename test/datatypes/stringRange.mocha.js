/*
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
let { StringRange, $, ply, r } = plywood;

describe("StringRange", () => {
  it("is immutable class", () => {
    testImmutableClass(StringRange, [
      {
        start: 'k',
        end: 'z'
      },
      {
        start: '&',
        end: '&&',
        bounds: '[]'
      },
      {
        start: '7',
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


  describe("does not die with hasOwnProperty", () => {
    it("survives", () => {
      expect(StringRange.fromJS({
        start: 'a',
        end: 'd',
        hasOwnProperty: 'troll'
      }).toJS()).to.deep.equal({
        start: 'a',
        end: 'd'
      });
    });
  });


  describe("errors", () => {
    it("throws on bad strings", () => {
      expect(() => {
        StringRange.fromJS({
          start: 1,
          end: 2
        });
      }).to.throw('`start` must be a string');
    });
  });


  describe("#extend()", () => {
    it('works correctly with two bounded sets', () => {
      expect(
        StringRange.fromJS({ start: 'apple', end: 'bat' }).extend(StringRange.fromJS({ start: 'whale', end: 'zoo' })).toJS()
      ).to.deep.equal({ start: 'apple', end: 'zoo' });
    });

    it('works correctly with a fancy bounds', () => {
      expect(
        StringRange.fromJS({ start: 'person', end: 'tab', bounds: '(]' }).extend(StringRange.fromJS({
          start: 'van',
          end: 'van#',
          bounds: '(]'
        })).toJS()
      ).to.deep.equal({ start: 'person', end: 'van#', bounds: '(]' });
    });

    it('works correctly with infinite bounds on different sides', () => {
      expect(
        StringRange.fromJS({ start: null, end: 'CAB' }).extend(StringRange.fromJS({ start: 'CAR', end: null })).toJS()
      ).to.deep.equal({ start: null, end: null, bounds: '()' });
    });
  });


  describe("#union()", () => {
    it('works correctly with a non-disjoint set', () => {
      expect(
        StringRange.fromJS({ start: '#', end: 'f' }).union(StringRange.fromJS({ start: 'b', end: 'z' })).toJS()
      ).to.deep.equal({ start: '#', end: 'z' });
    });

    it('works correctly with a disjoint range', () => {
      expect(
        StringRange.fromJS({ start: '$', end: 'a' }).union(StringRange.fromJS({ start: 'zo', end: 'zoo' }))
      ).to.deep.equal(null);
    });
    it('works with itself when open', () => {
      expect(
        StringRange.fromJS({ start: 'aaa', end: 'aaaa', bounds: '()' }).union(StringRange.fromJS({
          start: 'aaa',
          end: 'aaaa',
          bounds: '()'
        })).toJS()
      ).to.deep.equal({ start: 'aaa', end: 'aaaa', bounds: '()' });
    });

    it('works correctly with infinite bounds on different sides', () => {
      expect(
        StringRange.fromJS({ start: null, end: 'bean' }).union(StringRange.fromJS({ start: 'bean', end: null })).toJS()
      ).to.deep.equal({ start: null, end: null, bounds: '()' });
    });

    it('works correctly with infinite non intersecting bounds', () => {
      expect(
        StringRange.fromJS({ start: 'rabbit', end: null, bounds: '()' }).union(StringRange.fromJS({
          start: null,
          end: 'ape',
          bounds: '(]'
        }))
      ).to.deep.equal(null);
    });
  });


  describe("#intersect()", () => {
    it('works correctly with a non-disjoint range', () => {
      expect(
        StringRange.fromJS({ start: 'finland', end: 'korea' }).intersect(StringRange.fromJS({ start: 'india', end: 'laos' })).toJS()
      ).to.deep.equal({ start: 'india', end: 'korea' });
    });

    it('works correctly with a disjoint range', () => {
      expect(
        StringRange.fromJS({ start: 'alabama', end: 'alaska' }).intersect(StringRange.fromJS({ start: 'india', end: 'israel' }))
      ).to.deep.equal(null);
    });

    it('works correctly with a adjacent range (what should zero endpoint look like? )', () => {
      expect(
        StringRange.fromJS({ start: 'alabama', end: 'alaska' }).intersect(StringRange.fromJS({ start: 'alaska', end: 'arizona' })).toJS()
      ).to.deep.equal({start: '', end: ''});
    });
  });
});
