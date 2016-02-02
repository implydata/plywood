{ expect } = require("chai")

{ WallTime } = require('chronoshift')
if not WallTime.rules
  tzData = require("chronoshift/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

{ testImmutableClass } = require("immutable-class/build/tester")

plywood = require('../../build/plywood')
{ Action, $, ply, r, MatchAction } = plywood

describe "Actions", ->
  it "is immutable class", ->
    testImmutableClass(Action, [
      {
        action: 'filter'
        expression: {
          op: 'chain'
          expression: { op: 'ref', name: 'myVar' }
          action: { action: 'is', expression: { op: 'literal', value: 5 } }
        }
      }
      {
        action: 'split'
        name: 'Page'
        expression: { op: 'ref', name: 'page' }
        dataName: 'myData'
      }
      {
        action: 'split'
        splits: {
          'Page': { op: 'ref', name: 'page' }
          'User': { op: 'ref', name: 'user' }
        }
        dataName: 'myData'
      }
      {
        action: 'apply'
        name: 'Five'
        expression: { op: 'literal', value: 5 }
      }
      {
        action: 'sort'
        expression: { op: 'ref', name: 'myVar' }
        direction: 'ascending'
      }
      {
        action: 'limit'
        limit: 10
      }
      { action: 'count' }
      { action: 'sum', expression: { op: 'ref', name: 'myVar' } }
      { action: 'min', expression: { op: 'ref', name: 'myVar' } }
      { action: 'max', expression: { op: 'ref', name: 'myVar' } }
      { action: 'average', expression: { op: 'ref', name: 'myVar' } }
      { action: 'countDistinct', expression: { op: 'ref', name: 'myVar' } }
      { action: 'quantile', expression: { op: 'ref', name: 'myVar' }, quantile: 0.5 }
      { action: 'custom', custom: 'blah' }

      { action: 'contains', expression: { op: 'ref', name: 'myVar' }, compare: 'normal' }
      { action: 'contains', expression: { op: 'ref', name: 'myVar' }, compare: 'ignoreCase' }

      { action: 'timeBucket', duration: 'P1D' }
      { action: 'timeBucket', duration: 'P2D', timezone: 'Etc/UTC' }
      { action: 'timeBucket', duration: 'P2D', timezone: 'America/Los_Angeles' }

      { action: 'timePart', part: 'DAY_OF_WEEK' }
      { action: 'timePart', part: 'DAY_OF_MONTH', timezone: 'Etc/UTC' }
      { action: 'timePart', part: 'DAY_OF_MONTH', timezone: 'America/Los_Angeles' }

      { action: 'timeShift', duration: 'P1D', step: 1 }
      { action: 'timeShift', duration: 'P1D', step: -2 }
      { action: 'timeShift', duration: 'P2D', step: 3, timezone: 'Etc/UTC' }
      { action: 'timeShift', duration: 'P2D', step: 3, timezone: 'America/Los_Angeles' }

      { action: 'timeRange', duration: 'P1D', step: 1 }
      { action: 'timeRange', duration: 'P1D', step: -2 }
      { action: 'timeRange', duration: 'P2D', step: 3, timezone: 'Etc/UTC' }
      { action: 'timeRange', duration: 'P2D', step: 3, timezone: 'America/Los_Angeles' }
    ], {
      newThrows: true
    })

  it "does not die with hasOwnProperty", ->
    expect(Action.fromJS({
      action: 'apply'
      name: 'Five'
      expression: { op: 'literal', value: 5 }
      hasOwnProperty: 'troll'
    }).toJS()).deep.equal({
      action: 'apply'
      name: 'Five'
      expression: { op: 'literal', value: 5 }
    })


  describe "MatchAction", ->
    it ".likeToRegExp", ->
      expect(MatchAction.likeToRegExp('%David\\_R_ss%')).to.equal('^.*David_R.ss.*$')

      expect(MatchAction.likeToRegExp('%David|_R_ss||%', '|')).to.equal('^.*David_R.ss\\|.*$')
