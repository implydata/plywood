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
let { NumberRange, $, ply, r } = plywood;

describe("NumberRange", () => {
  it("is immutable class", () => {
    testImmutableClass(NumberRange, [
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


  describe("does not die with hasOwnProperty", () => {
    it("survives", () => {
      expect(NumberRange.fromJS({
        start: 7,
        end: 9,
        hasOwnProperty: 'troll'
      }).toJS()).to.deep.equal({
        start: 7,
        end: 9
      });
    });
  });


  describe("errors", () => {
    it("throws on bad numbers", () => {
      expect(() => {
        NumberRange.fromJS({
          start: 'lol',
          end: 'wat'
        });
      }).to.throw('`start` must be a number');
    });
  });


  describe("#extend()", () => {
    it('works correctly with two bounded sets', () => {
      expect(
        NumberRange.fromJS({ start: 0, end: 2 }).extend(NumberRange.fromJS({ start: 5, end: 6 })).toJS()
      ).to.deep.equal({ start: 0, end: 6 });
    });

    it('works correctly with a fancy bounds', () => {
      expect(
        NumberRange.fromJS({ start: 0, end: 2, bounds: '(]' }).extend(NumberRange.fromJS({
          start: 5,
          end: 6,
          bounds: '(]'
        })).toJS()
      ).to.deep.equal({ start: 0, end: 6, bounds: '(]' });
    });

    it('works correctly with infinite bounds on different sides', () => {
      expect(
        NumberRange.fromJS({ start: null, end: 2 }).extend(NumberRange.fromJS({ start: 6, end: null })).toJS()
      ).to.deep.equal({ start: null, end: null, bounds: '()' });
    });
  });


  describe("#union()", () => {
    it('works correctly with a non-disjoint set', () => {
      expect(
        NumberRange.fromJS({ start: 0, end: 2 }).union(NumberRange.fromJS({ start: 1, end: 3 })).toJS()
      ).to.deep.equal({ start: 0, end: 3 });
    });

    it('works correctly with a disjoint range', () => {
      expect(
        NumberRange.fromJS({ start: 0, end: 1 }).union(NumberRange.fromJS({ start: 2, end: 3 }))
      ).to.deep.equal(null);
    });

    it('works correctly with a adjacent range', () => {
      expect(
        NumberRange.fromJS({ start: 0, end: 1 }).union(NumberRange.fromJS({ start: 1, end: 2 })).toJS()
      ).to.deep.equal({ start: 0, end: 2 });
    });

    it('works correctly with a fancy bounds', () => {
      expect(
        NumberRange.fromJS({ start: 0, end: 2, bounds: '(]' }).union(NumberRange.fromJS({
          start: 1,
          end: 3,
          bounds: '(]'
        })).toJS()
      ).to.deep.equal({ start: 0, end: 3, bounds: '(]' });
    });

    it('works with itself when open', () => {
      expect(
        NumberRange.fromJS({ start: 0, end: 1, bounds: '()' }).union(NumberRange.fromJS({
          start: 0,
          end: 1,
          bounds: '()'
        })).toJS()
      ).to.deep.equal({ start: 0, end: 1, bounds: '()' });
    });

    it('works correctly with infinite bounds on different sides', () => {
      expect(
        NumberRange.fromJS({ start: null, end: 2 }).union(NumberRange.fromJS({ start: 1, end: null })).toJS()
      ).to.deep.equal({ start: null, end: null, bounds: '()' });
    });

    it('works correctly with infinite non intersecting bounds', () => {
      expect(
        NumberRange.fromJS({ start: 1, end: null, bounds: '()' }).union(NumberRange.fromJS({
          start: null,
          end: 0,
          bounds: '(]'
        }))
      ).to.deep.equal(null);
    });
  });


  describe("#intersect()", () => {
    it('works correctly with a non-disjoint range', () => {
      expect(
        NumberRange.fromJS({ start: 0, end: 2 }).intersect(NumberRange.fromJS({ start: 1, end: 3 })).toJS()
      ).to.deep.equal({ start: 1, end: 2 });
    });

    it('works correctly with a disjoint range', () => {
      expect(
        NumberRange.fromJS({ start: 0, end: 1 }).intersect(NumberRange.fromJS({ start: 2, end: 3 }))
      ).to.deep.equal(null);
    });

    it('works correctly with a adjacent range', () => {
      expect(
        NumberRange.fromJS({ start: 0, end: 1 }).intersect(NumberRange.fromJS({ start: 1, end: 2 })).toJS()
      ).to.deep.equal({ start: 0, end: 0 });
    });

    it('works correctly with a fancy bounds', () => {
      expect(
        NumberRange.fromJS({ start: 0, end: 2, bounds: '(]' }).intersect(NumberRange.fromJS({
          start: 1,
          end: 3,
          bounds: '(]'
        })).toJS()
      ).to.deep.equal({ start: 1, end: 2, bounds: '(]' });
    });

    it('works with itself when open', () => {
      expect(
        NumberRange.fromJS({ start: 0, end: 1, bounds: '()' }).intersect(NumberRange.fromJS({
          start: 0,
          end: 1,
          bounds: '()'
        })).toJS()
      ).to.deep.equal({ start: 0, end: 1, bounds: '()' });
    });

    it('works correctly with infinite bounds', () => {
      expect(
        NumberRange.fromJS({ start: null, end: 2 }).intersect(NumberRange.fromJS({ start: 1, end: null })).toJS()
      ).to.deep.equal({ start: 1, end: 2 });
    });

    it('works correctly with infinite non intersecting bounds', () => {
      expect(
        NumberRange.fromJS({ start: 1, end: null, bounds: '()' }).intersect(NumberRange.fromJS({
          start: null,
          end: 0,
          bounds: '(]'
        }))
      ).to.deep.equal(null);
    });
  });
});
