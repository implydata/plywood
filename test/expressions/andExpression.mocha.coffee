{ expect } = require("chai")

tests = require './sharedTests'
plywood = require('../../build/plywood')
{ Set, TimeRange, NumberRange, $ } = plywood

describe 'AndExpression', ->
  describe 'with is expressions', ->
    beforeEach ->
      this.expression = { op: 'and', operands: [
        { op: 'is', lhs: "$test", rhs: "blah" },
        { op: 'is', lhs: "$test", rhs: "test2" },
      ] }

    tests.expressionCountIs(7)
    tests.simplifiedExpressionIs({op: 'literal', value: false})

  describe 'with is/in expressions', ->
    beforeEach ->
      this.expression = { op: 'and', operands: [
        { op: 'is', lhs: "$test", rhs: "blah" },
        {
          op: 'in',
          lhs: "$test",
          rhs: {
            op: 'literal'
            value: Set.fromJS(["blah", "test2"])
          }
        }
      ] }

    tests.expressionCountIs(7)
    tests.simplifiedExpressionIs({ op: 'is', lhs: { op: 'ref', name: "test" }, rhs: { op: 'literal', value: "blah" } })

  describe 'with is/in expressions 2', ->
    beforeEach ->
      this.expression = { op: 'and', operands: [
        {
          op: 'in',
          lhs: "$test",
          rhs: {
            op: 'literal'
            value: Set.fromJS(["blah", "test2"])
          }
        }
        { op: 'is', lhs: "$test", rhs: "blah" }
      ] }

    tests.expressionCountIs(7)
    tests.simplifiedExpressionIs({ op: 'is', lhs: { op: 'ref', name: "test" }, rhs: { op: 'literal', value: "blah" } })

  describe 'with hasOwnProperty', ->
    beforeEach ->
      this.expression = { op: 'and', operands: [
        {
          op: 'in',
          lhs: "$hasOwnProperty",
          rhs: {
            op: 'literal'
            value: Set.fromJS(["blah", "test2"])
          }
        }
        { op: 'is', lhs: "$hasOwnProperty", rhs: "blah" }
      ] }

    tests.expressionCountIs(7)
    tests.simplifiedExpressionIs({ op: 'is', lhs: { op: 'ref', name: "hasOwnProperty" }, rhs: { op: 'literal', value: "blah" } })

  describe 'with number comparison expressions', ->
    beforeEach ->
      this.expression = { op: 'and', operands: [
        { op: 'lessThan', lhs: "$test", rhs: 1 },
        { op: 'lessThanOrEqual', lhs: "$test", rhs: 0 }
      ] }

    tests.expressionCountIs(7)
    tests.simplifiedExpressionIs({
      op: 'in',
      lhs: { op: 'ref', name: "test" },
      rhs: { op: 'literal', type: "NUMBER_RANGE", value: { start: null, end: 0, bounds: '(]' } }
    })

  describe 'with and expressions', ->
    beforeEach ->
      this.expression = { op: 'and', operands: [
        { op: 'and', operands: [{ op: 'lessThan', lhs: "$test1", rhs: 1 }, { op: 'lessThanOrEqual', lhs: "$test2", rhs: 0 }]}
        { op: 'and', operands: [{ op: 'lessThan', lhs: "$test3", rhs: 1 }, { op: 'lessThanOrEqual', lhs: "$test4", rhs: 0 }]}
      ] }

    tests.expressionCountIs(15)
    tests.simplifiedExpressionIs({ op: 'and', operands: [
      {
        "lhs": { "name": "test1", "op": "ref" }
        "op": "in"
        "rhs": { "op": "literal", "type": "NUMBER_RANGE", "value": { "bounds": "()", "end": 1, "start": null } }
      }
      {
        "lhs": { "name": "test2", "op": "ref" }
        "op": "in"
        "rhs": { "op": "literal", "type": "NUMBER_RANGE", "value": { "bounds": "(]", "end": 0, "start": null } }
      }
      {
        "lhs": { "name": "test3", "op": "ref" }
        "op": "in"
        "rhs": { "op": "literal", "type": "NUMBER_RANGE", "value": { "bounds": "()", "end": 1, "start": null } }
      }
      {
        "lhs": {"name": "test4", "op": "ref"}
        "op": "in"
        "rhs": {"op": "literal", "type": "NUMBER_RANGE", "value": {"bounds": "(]", "end": 0, "start": null}}
      }
    ] })

  describe 'with collapsible expressions', ->
    beforeEach ->
      this.expression = { op: 'and', operands: [
        { op: 'lessThan', lhs: "$test", rhs: 1 },
        { op: 'lessThan', lhs: 0, rhs: "$test" }
      ] }

    tests.expressionCountIs(7)
    tests.simplifiedExpressionIs({
      "lhs": {
        "name": "test"
        "op": "ref"
      }
      "op": "in"
      "rhs": {
        "op": "literal"
        "type": "NUMBER_RANGE"
        "value": {
          "bounds": "()"
          "end": 1
          "start": 0
        }
      }
    })

  describe 'with time merge', ->
    beforeEach ->
      this.expression = $("time").in(TimeRange.fromJS({
        start: new Date('2015-03-14T00:00:00')
        end:   new Date('2015-03-21T00:00:00')
      })).and($("time").in(TimeRange.fromJS({
        start: new Date('2015-03-14T00:00:00')
        end:   new Date('2015-03-15T00:00:00')
      }))).toJS()

    tests.expressionCountIs(7)
    tests.simplifiedExpressionIs($("time").in(TimeRange.fromJS({
      start: new Date('2015-03-14T00:00:00')
      end:   new Date('2015-03-15T00:00:00')
    })).toJS())

  describe 'with collapsible expressions of different types', ->
    beforeEach ->
      this.expression = { op: 'and', operands: [
        { op: 'lessThan', lhs: "$test", rhs: 5 },
        { op: 'in', lhs: '$test', rhs: [0, 2, 4, 6, 8] }
      ] }

    tests.expressionCountIs(7)
    tests.simplifiedExpressionIs({
      "lhs": { "name": "test", "op": "ref" }
      "op": "in"
      "rhs": {
        "op": "literal"
        "type": "SET"
        "value": {
          "setType": "NUMBER"
          "elements": [0, 2, 4]
        }
      }
    })
