{ expect } = require("chai")

{ testImmutableClass } = require("immutable-class/build/tester")

plywood = require('../../build/plywood')
{ Action, $, ply, r } = plywood

describe "Actions", ->
  it "passes higher object tests", ->
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
