{ expect } = require("chai")
plywood = require('../../build/plywood')
{ NumberRange, Set, TimeRange, $ } = plywood

tests = require './sharedTests'

describe 'InExpression', ->
  describe 'with set', ->
    beforeEach ->
      this.expression = {
        op: 'in',
        lhs: { op: 'literal', value: 'Honda' },
        rhs: { op: 'literal', value: Set.fromJS(['Honda', 'BMW', 'Suzuki']) }
      }

    tests.expressionCountIs(3)
    tests.simplifiedExpressionIs({ op: 'literal', value: true })

  describe 'with empty set', ->
    beforeEach ->
      this.expression = {
        op: 'in',
        lhs: { op: 'ref', name: 'make' },
        rhs: { op: 'literal', value: Set.fromJS({ elements: [], setType: 'STRING' }) }
      }

    tests.expressionCountIs(3)
    tests.simplifiedExpressionIs({ op: 'literal', value: false })

  describe 'with number range', ->
    beforeEach ->
      this.expression = {
        op: 'in',
        lhs: { op: 'literal', value: 5 },
        rhs: { op: 'literal', value: new NumberRange({start: 0.05, end: 1}) }
      }

    tests.expressionCountIs(3)
    tests.simplifiedExpressionIs({ op: 'literal', value: false })

  describe 'with complex values', ->
    beforeEach ->
      this.expression = {
        op: 'in',
        lhs: { op: 'is', lhs: 1, rhs: 2 },
        rhs: { op: 'literal', value: Set.fromJS([true]) }
      }

    tests.expressionCountIs(5)
    tests.simplifiedExpressionIs({ op: 'literal', value: false })

  describe 'with right-handed reference', ->
    beforeEach ->
      this.expression = {
        op: 'in',
        lhs: { op: 'literal', value: 5 },
        rhs: { op: 'ref', name: 'test' }
      }

    tests.expressionCountIs(3)
    tests.simplifiedExpressionIs({ op: 'in', lhs: { op: 'literal', value: 5 }, rhs: { op: 'ref', name: 'test' }})

  describe 'with left-handed reference', ->
    describe 'with SET', ->
      beforeEach ->
        this.expression = {
          op: 'in',
          lhs: { op: 'ref', name: 'test' },
          rhs: { op: 'literal', value: Set.fromJS(['A', 'B', 'C']) }
        }

      tests.expressionCountIs(3)
      tests.simplifiedExpressionIs({
        op: 'in',
        lhs: { op: 'ref', name: 'test' },
        rhs: { op: 'literal', value: { elements: ['A', 'B', 'C'], setType: 'STRING' }, type: 'SET' }
      })

      describe '#mergeAnd', ->
        tests
          .mergeAndWith(
            "with an IS expression (contained in set)"
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: 'A' }
            }
          )
          .equals(
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: 'A' }
            }
          )

        tests
          .mergeAndWith(
            "with an IS expression (not contained in set)"
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: 'X' }
            }
          )
          .equals(
            {
              op: 'literal'
              value: false
            }
          )

        tests
          .mergeAndWith(
            "with a disjoint set"
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: Set.fromJS(['X', 'Y']) }
            }
          )
          .equals(
            {
              op: 'literal'
              value: false
            }
          )

        tests
          .mergeAndWith(
            "with an intersecting set"
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: Set.fromJS(['A', 'B', 'X']) }
            }
          )
          .equals(
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: { elements: ['A', 'B'], setType: 'STRING' }, type: 'SET' }
            }
          )

      describe '#mergeOr', ->
        tests
          .mergeOrWith(
            "with an is expression"
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: 'A' }
            }
          )
          .equals(
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: { elements: ['A', 'B', 'C'], setType: 'STRING'}, type: 'SET' }
            }
          )

        tests
          .mergeOrWith(
            "with an outside is expression"
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: 'X' }
            }
          )
          .equals(
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: { elements: ['A', 'B', 'C', 'X'], setType: 'STRING' }, type: 'SET' }
            }
          )

        tests
          .mergeOrWith(
            "with a disjoint set"
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: Set.fromJS(['X', 'Y']) }
            }
          )
          .equals(
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: { elements: ['A', 'B', 'C', 'X', 'Y'], setType: 'STRING' }, type: 'SET' }
            }
          )

        tests
          .mergeOrWith(
            "with an intersecting set"
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: Set.fromJS(['A', 'B', 'C']) }
            }
          )
          .equals(
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: { elements: ['A', 'B', 'C'], setType: 'STRING' }, type: 'SET' }
            }
          )

    describe 'with NUMBER_RANGE', ->
      beforeEach ->
        this.expression = {
          op: 'in',
          lhs: { op: 'ref', name: 'test' },
          rhs: { op: 'literal', value: new NumberRange({start: 0, end: 1}) }
        }

      tests.expressionCountIs(3)
      tests.simplifiedExpressionIs({
        op: 'in',
        lhs: { op: 'ref', name: 'test' },
        rhs: { op: 'literal', value: {start: 0, end: 1}, type: 'NUMBER_RANGE' }
      })

      describe '#mergeAnd', ->
        tests
          .mergeAndWith(
            "with an inside is expression"
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: 0.5 }
            }
          )
          .equals(
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: 0.5 }
            }
          )

        tests
          .mergeAndWith(
            "with an outside is expression"
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: 2 }
            }
          )
          .equals(
            {
              op: 'literal',
              value: false
            }
          )

        tests
          .mergeAndWith(
            "with a disjoint set"
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: new NumberRange({start: 2, end: 3}) }
            }
          )
          .equals(
            {
              op: 'literal',
              value: false
            }
          )

        tests
          .mergeAndWith(
            "with an intersecting set"
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: new NumberRange({start: 0.5, end: 1.5}) }
            }
          )
          .equals(
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: {start: 0.5, end: 1}, type: 'NUMBER_RANGE' }
            }
          )

      describe '#mergeOr', ->
        tests
          .mergeOrWith(
            "with an inside is expression"
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: 0.5 }
            }
          )
          .equals(
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: {start: 0, end: 1}, type: 'NUMBER_RANGE' }
            }
          )

        tests
          .mergeOrWith(
            "with an outside is expression"
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: 2 }
            }
          )
          .equals({
            "lhs": {
              "name": "test"
              "op": "ref"
            }
            "op": "in"
            "rhs": {
              "op": "literal"
              "type": "SET"
              "value": {
                "elements": [
                  {
                    "end": 1
                    "start": 0
                  }
                  {
                    "bounds": "[]"
                    "end": 2
                    "start": 2
                  }
                ]
                "setType": "NUMBER_RANGE"
              }
            }
          })

        tests
          .mergeOrWith(
            "with a disjoint set"
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: new NumberRange({start: 2, end: 3}) }
            }
          )
          .equals({
            "lhs": {
              "name": "test"
              "op": "ref"
            }
            "op": "in"
            "rhs": {
              "op": "literal"
              "type": "SET"
              "value": {
                "elements": [
                  {
                    "start": 0
                    "end": 1
                  }
                  {
                    "start": 2
                    "end": 3
                  }
                ]
                "setType": "NUMBER_RANGE"
              }
            }
          })

        tests
          .mergeOrWith(
            "with an intersecting set"
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: new NumberRange({start: 0.5, end: 1.5}) }
            }
          )
          .equals(
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: {start: 0, end: 1.5}, type: 'NUMBER_RANGE' }
            }
          )

    describe 'with TIME_RANGE', ->
      beforeEach ->
        this.expression = {
          op: 'in',
          lhs: { op: 'ref', name: 'test' },
          rhs: { op: 'literal', value: new TimeRange({start: new Date(0), end: new Date(10)}) }
        }

      tests.expressionCountIs(3)
      tests.simplifiedExpressionIs({
        op: 'in',
        lhs: { op: 'ref', name: 'test' },
        rhs: { op: 'literal', value: {start: new Date(0), end: new Date(10)}, type: 'TIME_RANGE' }
      })

      describe '#mergeAnd', ->
        tests
          .mergeAndWith(
            "with an inside is expression"
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: new Date(5) }
            }
          )
          .equals(
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: new Date(5) }
            }
          )

        tests
          .mergeAndWith(
            "with an outside is expression"
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: new Date(20) }
            }
          )
          .equals(
            {
              op: 'literal',
              value: false
            }
          )

        tests
          .mergeAndWith(
            "with a disjoint set"
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: new TimeRange({start: new Date(20), end: new Date(30)}) }
            }
          )
          .equals(
            {
              op: 'literal',
              value: false
            }
          )

        tests
          .mergeAndWith(
            "with an intersecting set"
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: new TimeRange({start: new Date(5), end: new Date(15)}) }
            }
          )
          .equals(
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: {start: new Date(5), end: new Date(10)}, type: 'TIME_RANGE' }
            }
          )

      describe '#mergeOr', ->
        tests
          .mergeOrWith(
            "with an inside is expression"
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: new Date(5) }
            }
          )
          .equals(
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: {start: new Date(0), end: new Date(10)}, type: 'TIME_RANGE' }
            }
          )

        tests
          .mergeOrWith(
            "with an outside is expression"
            {
              op: 'is',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: new Date(20) }
            }
          )
          .equals({
            "lhs": {
              "name": "test"
              "op": "ref"
            }
            "op": "in"
            "rhs": {
              "op": "literal"
              "type": "SET"
              "value": {
                "elements": [
                  {
                    "end": new Date(10)
                    "start": new Date(0)
                  }
                  {
                    "bounds": "[]"
                    "end": new Date(20)
                    "start": new Date(20)
                  }
                ]
                "setType": "TIME_RANGE"
              }
            }
          })

        tests
          .mergeOrWith(
            "with a disjoint set"
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: new TimeRange({start: new Date(20), end: new Date(30)}) }
            }
          )
          .equals({
            "lhs": {
              "name": "test"
              "op": "ref"
            }
            "op": "in"
            "rhs": {
              "op": "literal"
              "type": "SET"
              "value": {
                "elements": [
                  {
                    "end": new Date("1970-01-01T00:00:00.010Z")
                    "start": new Date("1970-01-01T00:00:00.000Z")
                  }
                  {
                    "end": new Date("1970-01-01T00:00:00.030Z")
                    "start": new Date("1970-01-01T00:00:00.020Z")
                  }
                ]
                "setType": "TIME_RANGE"
              }
            }
          })

        tests
          .mergeOrWith(
            "with an intersecting set"
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: new TimeRange({start: new Date(5), end: new Date(15)}) }
            }
          )
          .equals(
            {
              op: 'in',
              lhs: { op: 'ref', name: 'test' },
              rhs: { op: 'literal', value: {start: new Date(0), end: new Date(15)}, type: 'TIME_RANGE' }
            }
          )
