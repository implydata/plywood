{ expect } = require("chai")

plywood = require('../../build/plywood')
{ NumberRange, Set, TimeRange, $ } = plywood

tests = require './sharedTests'

describe 'IsExpression', ->
  describe 'with true value', ->
    beforeEach ->
      this.expression = { op: 'is', lhs: { op: 'literal', value: 5 }, rhs: { op: 'literal', value: 5 } }

    tests.expressionCountIs(3)
    tests.simplifiedExpressionIs({ op: 'literal', value: true })

  describe 'with false value', ->
    beforeEach ->
      this.expression = { op: 'is', lhs: { op: 'literal', value: 5 }, rhs: { op: 'literal', value: 2 } }

    tests.expressionCountIs(3)
    tests.simplifiedExpressionIs({ op: 'literal', value: false })

  describe 'with string value', ->
    beforeEach ->
      this.expression = { op: 'is', lhs: 'abc', rhs: 'abc' }

    tests.expressionCountIs(3)
    tests.simplifiedExpressionIs({ op: 'literal', value: true })

  describe 'with reference', ->
    describe 'in NUMBER type', ->
      beforeEach ->
        this.expression = { op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: 5 } }

      tests.expressionCountIs(3)
      tests.simplifiedExpressionIs({ op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: 5 } })

      describe '#mergeAnd', ->
        tests
          .mergeAndWith(
            "with a different IsExpression",
            {
              op: 'is',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: 7 }
            }
          )
          .equals({ op: 'literal', value: false })

        tests
          .mergeAndWith(
            "with the same IsExpression",
            {
              op: 'is',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: 5 }
            }
          )
          .equals({ op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: 5 }})

        tests
          .mergeAndWith(
            "with inclusive InExpression",
            {
              op: 'in',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: new NumberRange({start: 5, end: 7}), type: 'NUMBER_RANGE' }
            }
          )
          .equals({ op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: 5 }})

      describe '#mergeOr', ->
        tests
          .mergeOrWith(
            "with a different IsExpression",
            {
              op: 'is',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: 7 }
            }
          )
          .equals({
            op: 'in',
            lhs: { op: 'ref', name: 'flight_time' },
            rhs: { op: 'literal', value: { elements: [5, 7], setType: 'NUMBER' }, type: 'SET' }
          })

        tests
          .mergeOrWith(
            "with the same IsExpression",
            {
              op: 'is',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: 5 }
            }
          )
          .equals({ op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: 5 }})

        tests
          .mergeOrWith(
            "with inclusive InExpression",
            {
              op: 'in',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: new NumberRange({start: 5, end: 7}), type: 'NUMBER_RANGE' }
            }
          )
          .equals({
            op: 'in'
            lhs: { op: 'ref', name: 'flight_time' }
            rhs: { op: 'literal', value: {start: 5, end: 7}, type: 'NUMBER_RANGE' }
          })


    describe 'in TIME type', ->
      beforeEach ->
        this.expression = { op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: new Date(6) } }

      tests.expressionCountIs(3)
      tests.simplifiedExpressionIs({ op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: new Date(6) } })

      describe '#mergeAnd', ->
        tests
          .mergeAndWith(
            "with a different IsExpression",
            {
              op: 'is',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: new Date(2) }
            }
          )
          .equals({ op: 'literal', value: false })

        tests
          .mergeAndWith(
            "with the same IsExpression",
            {
              op: 'is',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: new Date(6) }
            }
          )
          .equals({ op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: new Date(6) }})

        tests
          .mergeAndWith(
            "with inclusive InExpression",
            {
              op: 'in'
              lhs: { op: 'ref', name: 'flight_time' }
              rhs: {
                op: 'literal'
                value: new TimeRange({
                  start: new Date(0),
                  end: new Date(7)
                })
                type: 'TIME_RANGE'
              }
            }
          )
          .equals({ op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: new Date(6) } })

      describe 'with timeBucket (simple)', ->
        beforeEach ->
          this.expression = {
            op: 'is',
            lhs: { op: 'timeBucket', operand: { op: 'ref', name: 'time' }, duration: 'P1D', timezone: 'Etc/UTC' },
            rhs: { op: 'literal', value: new TimeRange({ start: new Date('2015-03-14T00:00:00'), end: new Date('2015-03-15T00:00:00') }) }
          }

        tests.expressionCountIs(4)
        tests.simplifiedExpressionIs({
          op: 'in'
          lhs: { op: 'ref', name: 'time' }
          rhs: {
            op: 'literal'
            value: { start: new Date('2015-03-14T00:00:00'), end: new Date('2015-03-15T00:00:00') }
            type: 'TIME_RANGE'
          }
        })

      describe 'with timeBucket (simple) bogus', ->
        beforeEach ->
          this.expression = {
            op: 'is',
            lhs: { op: 'timeBucket', operand: { op: 'ref', name: 'time' }, duration: 'P1D', timezone: 'Etc/UTC' },
            rhs: { op: 'literal', value: new TimeRange({ start: new Date('2015-03-14T00:00:00'), end: new Date('2015-03-15T01:00:00') }) }
          }

        tests.expressionCountIs(4)
        tests.simplifiedExpressionIs({
          op: 'literal'
          value: false
        })

      describe '#mergeOr', ->
        tests
          .mergeOrWith(
            "with a different IsExpression",
            {
              op: 'is',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: new Date(2000) }
            }
          )
          .equals({
            op: 'in',
            lhs: { op: 'ref', name: 'flight_time' },
            rhs: {
              op: 'literal',
              value: {
                elements: [
                  new Date("1970-01-01T00:00:00.006Z")
                  new Date("1970-01-01T00:00:02.000Z")
                ]
                setType: 'TIME'
              }
              type: "SET"
            }
          })

        tests
          .mergeOrWith(
            "with the same IsExpression",
            {
              op: 'is',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: new Date(6) }
            }
          )
          .equals({ op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: new Date(6) }})

        tests
          .mergeOrWith(
            "with inclusive InExpression",
            {
              op: 'in'
              lhs: { op: 'ref', name: 'flight_time' }
              rhs: {
                op: 'literal'
                value: new TimeRange({
                  start: new Date(0),
                  end: new Date(7)
                })
                type: 'TIME_RANGE'
              }
            }
          )
          .equals({
            op: 'in'
            lhs: { op: 'ref', name: 'flight_time' }
            rhs: {
              op: 'literal'
              value: {
                start: new Date(0),
                end: new Date(7)
              }
              type: 'TIME_RANGE'
            }
          })

    describe 'in STRING type', ->
      beforeEach ->
        this.expression = { op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: 'ABC' } }

      tests.expressionCountIs(3)
      tests.simplifiedExpressionIs({ op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: 'ABC' } })

      describe '#mergeAnd', ->
        tests
          .mergeAndWith(
            "with a different IsExpression",
            {
              op: 'is',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: 'BCD' }
            }
          )
          .equals({ op: 'literal', value: false })

        tests
          .mergeAndWith(
            "with the same IsExpression",
            {
              op: 'is',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: 'ABC' }
            }
          )
          .equals({ op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: 'ABC' }})

        tests
          .mergeAndWith(
            "with inclusive InExpression",
            {
              op: 'in',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: Set.fromJS({ elements: ['ABC', 'DEF'], setType: 'STRING' }) }
            }
          )
          .equals({ op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: 'ABC' } })

        tests
          .mergeAndWith(
              "with exclusive InExpression",
              {
                op: 'in',
                lhs: { op: 'ref', name: 'flight_time' },
                rhs: { op: 'literal', value: Set.fromJS({ elements: ['DEF'], setType: 'STRING' }) }
              }
            )
          .equals({ op: 'literal', value: false })

      describe '#mergeOr', ->
        tests
          .mergeOrWith(
            "with a different IsExpression",
            {
              op: 'is',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: 'BCD' }
            }
          )
          .equals({
            op: 'in',
            lhs: { op: 'ref', name: 'flight_time' },
            rhs: { op: 'literal', value: { elements: ['ABC', 'BCD'], setType: 'STRING' }, type: 'SET' }
          })

        tests
          .mergeOrWith(
            "with the same IsExpression",
            {
              op: 'is',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: 'ABC' }
            }
          )
          .equals({ op: 'is', lhs: { op: 'ref', name: 'flight_time' }, rhs: { op: 'literal', value: 'ABC' }})

        tests
          .mergeOrWith(
            "with inclusive InExpression",
            {
              op: 'in',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: Set.fromJS(['ABC', 'DEF']) }
            }
          )
          .equals({
            op: 'in',
            lhs: { op: 'ref', name: 'flight_time' },
            rhs: { op: 'literal', value: { elements: ['ABC', 'DEF'], setType: 'STRING' }, type: 'SET' }
          })

        tests
          .mergeOrWith(
            "with exclusive InExpression",
            {
              op: 'in',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: Set.fromJS(['DEF']) }
            }
          )
          .equals({
              op: 'in',
              lhs: { op: 'ref', name: 'flight_time' },
              rhs: { op: 'literal', value: { elements: ['ABC', 'DEF'], setType: 'STRING' }, type: 'SET' }
            })

  describe 'with complex values', ->
    beforeEach ->
      this.expression = { op: 'is', lhs: { op: 'is', lhs: 1, rhs: 2 }, rhs: { op: 'is', lhs: 5, rhs: 2 }}

    tests.expressionCountIs(7)
    tests.simplifiedExpressionIs({ op: 'literal', value: true })
