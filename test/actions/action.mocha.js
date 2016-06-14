var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var { testImmutableClass } = require("immutable-class/build/tester");

var plywood = require('../../build/plywood');
var { Action, $, ply, r, MatchAction } = plywood;

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
      {
        action: 'limit',
        limit: 10
      },
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
      { action: 'custom', custom: 'blah' },

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

      { action: 'custom', custom: 'lol1' },
      { action: 'custom', custom: 'lol2' }
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
  })
});
