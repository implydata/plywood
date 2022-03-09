/*
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

const { $, ply, r, Expression } = plywood;

describe('TimeFloorExpression', () => {
  describe('errors', () => {
    it('errors on non-floorable duration', () => {
      expect(() => {
        Expression.fromJS({
          op: 'timeFloor',
          operand: { op: 'ref', name: '_' },
          duration: 'PT5H',
        });
      }).to.throw("duration 'PT5H' is not floorable");
    });
  });

  describe('#alignsWith', () => {
    const hourFloorUTC = Expression.fromJS({
      op: 'timeFloor',
      operand: { op: 'ref', name: '_' },
      duration: 'PT1H',
      timezone: 'Etc/UTC',
    });

    it('works with higher floor (PT2H)', () => {
      const ex = Expression.fromJS({
        op: 'timeFloor',
        operand: { op: 'ref', name: '_' },
        duration: 'PT2H',
        timezone: 'Etc/UTC',
      });

      expect(hourFloorUTC.alignsWith(ex)).to.equal(true);
    });

    it('works with higher floor (P1D)', () => {
      const ex = Expression.fromJS({
        op: 'timeFloor',
        operand: { op: 'ref', name: '_' },
        duration: 'P1D',
        timezone: 'Etc/UTC',
      });

      expect(hourFloorUTC.alignsWith(ex)).to.equal(true);
    });

    it('works fails on different timezone', () => {
      const ex = Expression.fromJS({
        op: 'timeFloor',
        operand: { op: 'ref', name: '_' },
        duration: 'PT2H',
        timezone: 'America/Los_Angeles',
      });

      expect(hourFloorUTC.alignsWith(ex)).to.equal(false);
    });

    it('works fails on lower duration', () => {
      const ex = Expression.fromJS({
        op: 'timeFloor',
        operand: { op: 'ref', name: '_' },
        duration: 'PT30M',
        timezone: 'Etc/UTC',
      });

      expect(hourFloorUTC.alignsWith(ex)).to.equal(false);
    });

    it('works with OVERLAP range', () => {
      const ex = Expression.fromJS({
        op: 'overlap',
        operand: { op: 'ref', name: '_' },
        expression: {
          op: 'literal',
          type: 'TIME_RANGE',
          value: {
            start: '2016-09-01T01:00:00Z',
            end: '2016-09-01T02:00:00Z',
          },
        },
      });

      expect(hourFloorUTC.alignsWith(ex)).to.equal(true);
    });

    it('works fails OVERLAP range (bad)', () => {
      const ex = Expression.fromJS({
        op: 'overlap',
        operand: { op: 'ref', name: '_' },
        expression: {
          op: 'literal',
          type: 'TIME_RANGE',
          value: {
            start: '2016-09-01T01:00:00Z',
            end: '2016-09-01T02:00:01Z',
          },
        },
      });

      expect(hourFloorUTC.alignsWith(ex)).to.equal(false);
    });

    it('works with OVERLAP set', () => {
      const ex = Expression.fromJS({
        op: 'overlap',
        operand: { op: 'ref', name: '_' },
        expression: {
          op: 'literal',
          type: 'SET',
          value: {
            setType: 'TIME_RANGE',
            elements: [
              { start: '2016-09-01T01:00:00Z', end: '2016-09-01T02:00:00Z' },
              { start: '2016-09-01T05:00:00Z', end: '2016-09-01T07:00:00Z' },
            ],
          },
        },
      });

      expect(hourFloorUTC.alignsWith(ex)).to.equal(true);
    });

    it('works fails OVERLAP set (bad)', () => {
      const ex = Expression.fromJS({
        op: 'overlap',
        operand: { op: 'ref', name: '_' },
        expression: {
          op: 'literal',
          type: 'SET',
          value: {
            setType: 'TIME_RANGE',
            elements: [
              { start: '2016-09-01T01:00:00Z', end: '2016-09-01T02:00:00Z' },
              { start: '2016-09-01T05:00:00Z', end: '2016-09-01T07:00:01Z' },
            ],
          },
        },
      });

      expect(hourFloorUTC.alignsWith(ex)).to.equal(false);
    });
  });
});
