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

let { Timezone } = require('chronoshift');
let plywood = require('../plywood');
let { TimeRange, $, ply, r } = plywood;

describe("TimeRange", () => {
  it("is immutable class", () => {
    testImmutableClass(TimeRange, [
      {
        start: new Date('2015-01-26T04:54:10Z'),
        end: new Date('2015-01-26T05:54:10Z')
      },
      {
        start: new Date('2015-01-26T04:54:10Z'),
        end: new Date('2015-01-26T05:00:00Z')
      }
    ]);
  });

  describe("does not die with hasOwnProperty", () => {
    it("survives", () => {
      expect(TimeRange.fromJS({
        start: new Date('2015-01-26T04:54:10Z'),
        end: new Date('2015-01-26T05:54:10Z'),
        hasOwnProperty: 'troll'
      }).toJS()).to.deep.equal({
        start: new Date('2015-01-26T04:54:10Z'),
        end: new Date('2015-01-26T05:54:10Z')
      });
    });
  });

  describe("toString", () => {
    it("works with timezone", () => {
      expect(TimeRange.fromJS({
        start: new Date('2015-01-26T04:54:10Z'),
        end: new Date('2015-01-26T05:54:10Z')
      }).toString()).to.deep.equal('[2015-01-26T04:54:10Z,2015-01-26T05:54:10Z]');

      expect(TimeRange.fromJS({
        start: new Date('2015-01-26T04:54:10Z'),
        end: new Date('2015-01-26T05:54:10Z')
      }).toString(Timezone.fromJS('Asia/Kathmandu'))).to.deep.equal('[2015-01-26T10:39:10+05:45,2015-01-26T11:39:10+05:45]');
    });
  });


  describe("upgrades", () => {
    it("upgrades from a string", () => {
      let timeRange = TimeRange.fromJS({
        start: '2015-01-26T04:54:10Z',
        end: '2015-01-26T05:00:00Z'
      });
      expect(timeRange.start.valueOf()).to.equal(Date.parse('2015-01-26T04:54:10Z'));
      expect(timeRange.end.valueOf()).to.equal(Date.parse('2015-01-26T05:00:00Z'));
    });
  });


  describe("#union()", () => {
    it('works correctly with a non-disjoint range', () => {
      expect(
        TimeRange.fromJS({
          start: '2015-01-26T00:00:00',
          end: '2015-01-26T02:00:00'
        }).union(TimeRange.fromJS({ start: '2015-01-26T01:00:00', end: '2015-01-26T03:00:00' })).toJS()
      ).to.deep.equal({ start: new Date('2015-01-26T00:00:00'), end: new Date('2015-01-26T03:00:00') });
    });

    it('works correctly with a disjoint range', () => {
      expect(
        TimeRange.fromJS({
          start: '2015-01-26T00:00:00',
          end: '2015-01-26T01:00:00'
        }).union(TimeRange.fromJS({ start: '2015-01-26T02:00:00', end: '2015-01-26T03:00:00' }))
      ).to.deep.equal(null);
    });

    it('works correctly with a adjacent range', () => {
      expect(
        TimeRange.fromJS({
          start: '2015-01-26T00:00:00',
          end: '2015-01-26T01:00:00'
        }).union(TimeRange.fromJS({ start: '2015-01-26T01:00:00', end: '2015-01-26T02:00:00' })).toJS()
      ).to.deep.equal({ start: new Date('2015-01-26T00:00:00'), end: new Date('2015-01-26T02:00:00') });
    });
  });


  describe("#intersect()", () => {
    it('works correctly with a non-disjoint range', () => {
      expect(
        TimeRange.fromJS({
          start: '2015-01-26T00:00:00',
          end: '2015-01-26T02:00:00'
        }).intersect(TimeRange.fromJS({ start: '2015-01-26T01:00:00', end: '2015-01-26T03:00:00' })).toJS()
      ).to.deep.equal({ start: new Date('2015-01-26T01:00:00'), end: new Date('2015-01-26T02:00:00') });
    });

    it('works correctly with a disjoint range', () => {
      expect(
        TimeRange.fromJS({
          start: '2015-01-26T00:00:00',
          end: '2015-01-26T01:00:00'
        }).intersect(TimeRange.fromJS({ start: '2015-01-26T02:00:00', end: '2015-01-26T03:00:00' }))
      ).to.deep.equal(null);
    });

    it('works correctly with a adjacent range', () => {
      expect(
        TimeRange.fromJS({
          start: '2015-01-26T00:00:00',
          end: '2015-01-26T01:00:00'
        }).intersect(TimeRange.fromJS({ start: '2015-01-26T01:00:00', end: '2015-01-26T02:00:00' })).toJS()
      ).to.deep.equal({ start: new Date(0), end: new Date(0) });
    });
  });


  describe("#toInterval", () => {
    it("works in general", () => {
      let timeRange = TimeRange.fromJS({
        start: '2015-01-26T04:54:10Z',
        end: '2015-01-26T05:00:00Z'
      });
      expect(timeRange.toInterval()).to.equal('2015-01-26T04:54:10Z/2015-01-26T05Z');
    });

    it('works on a round interval', () => {
      expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-27T00:00:00' }).toInterval()
      ).to.deep.equal('2015-01-26T00Z/2015-01-27T00Z');
    });

    it('works on a non round interval', () => {
      expect(
        TimeRange.fromJS({ start: '2015-01-26T12:34:56', end: '2015-01-27T11:22:33' }).toInterval()
      ).to.deep.equal('2015-01-26T12:34:56Z/2015-01-27T11:22:33Z');
    });

    it('works on an interval with different bounds', () => {
      expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-27T00:00:00', bounds: '(]' }).toInterval()
      ).to.deep.equal('2015-01-26T00:00:00.001Z/2015-01-27T00:00:00.001Z');
    });
  });
});
