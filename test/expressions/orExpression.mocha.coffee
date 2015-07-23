{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Set, $ } = plywood

tests = require './sharedTests'

describe 'OrExpression', ->
  describe 'empty expressions', ->
    beforeEach ->
      this.expression = { op: 'or', operands: [] }

    tests.expressionCountIs(1)
    tests.simplifiedExpressionIs({op: 'literal', value: false})

  describe 'with false expressions', ->
    beforeEach ->
      this.expression = { op: 'or', operands: [
        { op: 'literal', value: false },
        { op: 'literal', value: false }
      ] }

    tests.expressionCountIs(3)
    tests.simplifiedExpressionIs({op: 'literal', value: false})

  describe 'with boolean expressions', ->
    beforeEach ->
      this.expression = { op: 'or', operands: [
        { op: 'literal', value: true },
        { op: 'literal', value: false },
        { op: 'literal', value: false }
      ] }

    tests.expressionCountIs(4)
    tests.simplifiedExpressionIs({op: 'literal', value: true})

  describe 'with IS expressions', ->
    beforeEach ->
      this.expression = { op: 'or', operands: [
        { op: 'is', lhs: "$test", rhs: "blah" },
        { op: 'is', lhs: "$test", rhs: "test2" },
      ] }

    tests.expressionCountIs(7)
    tests.simplifiedExpressionIs({
      op: 'in',
      lhs: { op: 'ref', name: 'test' },
      rhs: {
        op: 'literal'
        value: { elements: ["blah", "test2"], setType: 'STRING' }
        type: 'SET'
      }
    })

  describe 'with is/in expressions', ->
    beforeEach ->
      this.expression = { op: 'or', operands: [
        { op: 'is', lhs: "$test", rhs: "blah3" },
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
    tests.simplifiedExpressionIs({
      op: 'in',
      lhs: { op: 'ref', name: 'test' },
      rhs: {
        op: 'literal'
        value: { elements: ["blah", "blah3", "test2"], setType: 'STRING' }
        type: 'SET'
      }
    })

  describe 'with IS/IN expressions 2', ->
    beforeEach ->
      this.expression = { op: 'or', operands: [
        {
          op: 'in',
          lhs: "$test",
          rhs: {
            op: 'literal'
            value: Set.fromJS(["blah", "test2"])
          }
        }
        { op: 'is', lhs: "$test", rhs: "blah3" }
      ] }

    tests.expressionCountIs(7)
    tests.simplifiedExpressionIs({
      op: 'in',
      lhs: { op: 'ref', name: 'test' },
      rhs: {
        op: 'literal'
        value: { elements: ["blah", "blah3", "test2"], setType: 'STRING' }
        type: 'SET'
      }
    })

  describe 'with number comparison expressions', ->
    beforeEach ->
      this.expression = { op: 'or', operands: [
        { op: 'lessThan', lhs: "$test", rhs: 1 },
        { op: 'lessThanOrEqual', lhs: "$test", rhs: 0 }
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
          "start": null
        }
      }
    })

  describe 'with or expressions', ->
    beforeEach ->
      this.expression = { op: 'or', operands: [
        { op: 'or', operands: [{ op: 'lessThan', lhs: "$test1", rhs: 1 }, { op: 'lessThanOrEqual', lhs: "$test2", rhs: 0 }]}
        { op: 'or', operands: [{ op: 'lessThan', lhs: "$test3", rhs: 1 }, { op: 'lessThanOrEqual', lhs: "$test4", rhs: 0 }]}
      ] }

    tests.expressionCountIs(15)
    tests.simplifiedExpressionIs({ op: 'or', operands: [
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
      this.expression = { op: 'or', operands: [
        { op: 'lessThan', lhs: "$test", rhs: 1 },
        { op: 'lessThan', lhs: 2, rhs: "$test" }
      ] }

    tests.expressionCountIs(7)
    tests.simplifiedExpressionIs({
      "lhs": { "name": "test", "op": "ref" }
      "op": "in"
      "rhs": {
        "op": "literal"
        "type": "SET"
        "value": {
          "setType": "NUMBER_RANGE"
          "elements": [
            {
              "start": 2
              "end": null
              "bounds": "()"
            }
            {
              "start": null
              "end": 1
              "bounds": "()"
            }
          ]
        }
      }
    })

  describe 'with collapsible expressions of different types', ->
    beforeEach ->
      this.expression = { op: 'or', operands: [
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
          "setType": "NUMBER_RANGE"
          "elements": [
            {
              "bounds": "()"
              "end": 5
              "start": null
            }
            {
              "bounds": "[]"
              "end": 6
              "start": 6
            }
            {
              "bounds": "[]"
              "end": 8
              "start": 8
            }
          ]
        }
      }
    })
