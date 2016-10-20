/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
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

var { expect } = require("chai");

var { testImmutableClass } = require("immutable-class-tester");

var plywood = require('../../build/plywood');
var { Action, $, ply, r, LimitAction } = plywood;

describe("Action", () => {
  it("is immutable class", () => {
    testImmutableClass(Action, [
      {
        action: 'filter',
        expression: {
          op: 'chain',
          expression: { op: 'ref', name: 'myVar' },
          action: { action: 'is', expression: { op: 'literal', value: 5 } }
        }
      },
      {
        action: 'split',
        name: 'Page',
        expression: { op: 'ref', name: 'page' },
        dataName: 'myData'
      },
      {
        action: 'split',
        splits: {
          'Page': { op: 'ref', name: 'page' },
          'User': { op: 'ref', name: 'user' }
        },
        dataName: 'myData'
      },
      {
        action: 'apply',
        name: 'Five',
        expression: { op: 'literal', value: 5 }
      },
      {
        action: 'sort',
        expression: { op: 'ref', name: 'myVar' },
        direction: 'ascending'
      },
      { action: 'limit', limit: 10 },
      { action: 'select', attributes: ['a', 'b', 'c'] },
      { action: 'select', attributes: ['b', 'c'] },

      { action: 'fallback', expression: { op: 'ref', name: 'myVar2' } },
      { action: 'count' },
      { action: 'sum', expression: { op: 'ref', name: 'myVar' } },
      { action: 'power', expression: { op: 'ref', name: 'myVar' } },
      { action: 'absolute' },
      { action: 'min', expression: { op: 'ref', name: 'myVar' } },
      { action: 'max', expression: { op: 'ref', name: 'myVar' } },
      { action: 'average', expression: { op: 'ref', name: 'myVar' } },
      { action: 'countDistinct', expression: { op: 'ref', name: 'myVar' } },
      { action: 'quantile', expression: { op: 'ref', name: 'myVar' }, quantile: 0.5 },
      { action: 'collect', expression: { op: 'ref', name: 'myVar' } },
      { action: 'cast', outputType: 'TIME' },
      { action: 'cast', outputType: 'NUMBER' },

      { action: 'customAggregate', custom: 'blah' },
      { action: 'customTransform', custom: 'decodeURIComponentToLowerCaseAndTrim' },
      { action: 'customTransform', custom: 'includes', outputType: 'BOOLEAN' },

      { action: 'concat', expression: { op: 'literal', value: 'myVar' } },

      { action: 'contains', expression: { op: 'ref', name: 'myVar' }, compare: 'normal' },
      { action: 'contains', expression: { op: 'ref', name: 'myVar' }, compare: 'ignoreCase' },

      { action: 'match', regexp: 'A[B]' },
      { action: 'match', regexp: '^fu*$' },

      { action: 'lessThan', expression: { op: 'literal', type: 'TIME', value: new Date('2015-10-10Z') } },

      { action: 'overlap', expression: { op: 'ref', name: 'myVar' } },
      { action: 'overlap', expression: { op: 'literal', value: { setType: 'STRING', elements: ['BMW', 'Honda', 'Suzuki'] }, type: 'SET' } },

      { action: 'numberBucket', size: 5 },
      { action: 'numberBucket', size: 5, offset: 1 },

      { action: 'length' },
      { action: 'indexOf', expression: { op: 'literal', value: 'string' } },
      { action: 'cardinality' },

      { action: 'timeFloor', duration: 'P1D' },
      { action: 'timeFloor', duration: 'PT2H', timezone: 'Etc/UTC' },
      { action: 'timeFloor', duration: 'PT2H', timezone: 'America/Los_Angeles' },

      { action: 'timeBucket', duration: 'P1D' },
      { action: 'timeBucket', duration: 'PT2H', timezone: 'Etc/UTC' },
      { action: 'timeBucket', duration: 'PT2H', timezone: 'America/Los_Angeles' },

      { action: 'timePart', part: 'DAY_OF_WEEK' },
      { action: 'timePart', part: 'DAY_OF_MONTH', timezone: 'Etc/UTC' },
      { action: 'timePart', part: 'DAY_OF_MONTH', timezone: 'America/Los_Angeles' },

      { action: 'timeShift', duration: 'P1D', step: 1 },
      { action: 'timeShift', duration: 'P1D', step: -2 },
      { action: 'timeShift', duration: 'P2D', step: 3, timezone: 'Etc/UTC' },
      { action: 'timeShift', duration: 'P2D', step: 3, timezone: 'America/Los_Angeles' },

      { action: 'timeRange', duration: 'P1D', step: 1 },
      { action: 'timeRange', duration: 'P1D', step: -2 },
      { action: 'timeRange', duration: 'P2D', step: 3, timezone: 'Etc/UTC' },
      { action: 'timeRange', duration: 'P2D', step: 3, timezone: 'America/Los_Angeles' },

      { action: 'transformCase', transformType: 'upperCase'},
      { action: 'transformCase', transformType: 'lowerCase'},

      { action: 'customAggregate', custom: 'lol1' },
      { action: 'customAggregate', custom: 'lol2' }

    ], {
      newThrows: true
    });
  });

  it("does not die with hasOwnProperty", () => {
    expect(Action.fromJS({
      action: 'apply',
      name: 'Five',
      expression: { op: 'literal', value: 5 },
      hasOwnProperty: 'troll'
    }).toJS()).deep.equal({
      action: 'apply',
      name: 'Five',
      expression: { op: 'literal', value: 5 }
    });
  });

  describe('ensure no action', () => {
    it('makes sure there is no action on the correct actions', () => {
      var actionsWithNoExpression = [
        'absolute',
        'not',
        'count'
      ];

      for (var action of actionsWithNoExpression) {
        expect(() => {
          Action.fromJS({ action, expression: { op: 'ref', name: 'myVar' } })
        }, `works with ${action}`).to.throw(`${action} must no have an expression (is $myVar)`);
      }
    });
  });

  describe('fancy actions', () => {
    it('limit works with Infinity', () => {
      expect(new LimitAction({ limit: Infinity }).toJS()).to.deep.equal({
        "action": "limit",
        "limit": Infinity
      });
    });

  });

});
