{ expect } = require("chai")

{ testHigherObjects } = require("higher-object/build/tester")

{ WallTime } = require('chronology')
if not WallTime.rules
  tzData = require("chronology/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

plywood = require('../../build/plywood')
{ Expression, $, RefExpression } = plywood

describe "Expression", ->
  it "passes higher object tests", ->
    testHigherObjects(Expression, [
      { op: 'literal', value: null }
      { op: 'literal', value: false }
      { op: 'literal', value: true }
      { op: 'literal', value: 0 }
      { op: 'literal', value: 0.1 }
      { op: 'literal', value: 6 }
      { op: 'literal', value: '' }
      { op: 'literal', value: 'Honda' }
      { op: 'literal', value: '$honda' }
      { op: 'literal', value: { setType: 'STRING', elements: [] }, type: 'SET' }
      { op: 'literal', value: { setType: 'STRING', elements: ['BMW', 'Honda', 'Suzuki'] }, type: 'SET' }
      { op: 'literal', value: { setType: 'NUMBER', elements: [0.05, 0.1] }, type: 'SET' }
      { op: 'literal', value: [{}], type: 'DATASET' }
      { op: 'ref', name: 'authors' }
      { op: 'ref', name: 'light_time' }
      { op: 'ref', name: 'timestamp' }
      { op: 'ref', name: 'timestamp', nest: 1 }
      { op: 'ref', name: 'timestamp', nest: 2 }
      { op: 'ref', name: 'make', type: 'STRING' }
      { op: 'ref', name: 'a fish will "save" you - lol / (or not)' }

      {
        op: 'chain'
        expression: { op: 'ref', name: 'diamonds' }
        actions: [
          {
            action: 'apply'
            name: 'five'
            expression: { op: 'literal', value: 5 }
          }
        ]
      }

    ], {
      newThrows: true
    })


  ###
  { op: 'is', lhs: { op: 'literal', value: 5 }, rhs: { op: 'literal', value: 5 } }
  { op: 'is', lhs: { op: 'literal', value: 5 }, rhs: { op: 'ref', name: 'flight_time' } }
  { op: 'lessThan', lhs: { op: 'literal', value: 5 }, rhs: { op: 'literal', value: 5 } }
  { op: 'lessThan', lhs: { op: 'literal', value: 5 }, rhs: { op: 'literal', value: 6 } }
  { op: 'lessThanOrEqual', lhs: { op: 'literal', value: 5 }, rhs: { op: 'ref', name: 'flight_time' } }
  { op: 'greaterThan', lhs: { op: 'literal', value: 5 }, rhs: { op: 'literal', value: 5 } }
  { op: 'greaterThan', lhs: { op: 'literal', value: 5 }, rhs: { op: 'ref', name: 'flight_time' } }
  { op: 'greaterThanOrEqual', lhs: { op: 'literal', value: 5 }, rhs: { op: 'literal', value: 5 } }
  { op: 'greaterThanOrEqual', lhs: { op: 'literal', value: 5 }, rhs: { op: 'ref', name: 'flight_time' } }
  { op: 'in', lhs: { op: 'literal', value: 'Honda' }, rhs: { op: 'literal', value: { setType: 'STRING', elements: ['BMW', 'Honda', 'Suzuki'] }, type: 'SET' } }
  #{ op: 'in', lhs: { op: 'literal', value: 5 }, rhs: { op: 'literal', value: [0.05, 0.1] } }
  { op: 'match', regexp: '^\d+', operand: { op: 'literal', value: 'Honda' } }
  { op: 'not', operand: { op: 'literal', value: true } }
  { op: 'and', operands: [{ op: 'literal', value: true }, { op: 'literal', value: false }, { op: 'literal', value: false }] }
  { op: 'or', operands: [{ op: 'literal', value: true }, { op: 'literal', value: false }, { op: 'literal', value: false }] }

  { op: 'add', operands: [{ op: 'literal', value: 5 }, { op: 'literal', value: -12 }, { op: 'literal', value: 0.4 }] }
  { op: 'negate', operand: { op: 'literal', value: 5 } }
  { op: 'multiply', operands: [{ op: 'literal', value: 5 }, { op: 'literal', value: -12 }, { op: 'literal', value: 0.4 }] }
  { op: 'reciprocate', operand: { op: 'literal', value: 5 } }

  { op: 'numberBucket', operand: { op: 'ref', name: 'num' }, size: 1 }
  { op: 'numberBucket', operand: { op: 'ref', name: 'num' }, size: 0.05, offset: 0.01 }
  { op: 'numberBucket', operand: { op: 'ref', name: 'num' }, size: 1, lowerLimit: 0, upperLimit: 5 }
  { op: 'numberBucket', operand: { op: 'ref', name: 'num' }, size: 1, lowerLimit: -8, upperLimit: 0 }
  { op: 'numberBucket', operand: { op: 'ref', name: 'num' }, size: 1, lowerLimit: 0 }

  { op: 'timeBucket', operand: { op: 'ref', name: 'time' }, duration: 'P1D', timezone: 'Etc/UTC' }
  { op: 'timeBucket', operand: { op: 'ref', name: 'time' }, duration: 'PT1H', timezone: 'Etc/UTC' }
  { op: 'timeBucket', operand: { op: 'ref', name: 'time' }, duration: 'PT1H', timezone: 'America/Los_Angeles' }

  { op: 'aggregate', operand: { op: 'ref', name: 'diamonds', type: 'DATASET' }, fn: 'sum', attribute: { op: 'ref', name: 'added' } }
  { op: 'aggregate', operand: { op: 'ref', name: 'diamonds', type: 'DATASET' }, fn: 'min', attribute: { op: 'ref', name: 'added' } }
  { op: 'aggregate', operand: { op: 'ref', name: 'diamonds', type: 'DATASET' }, fn: 'max', attribute: { op: 'ref', name: 'added' } }
  { op: 'aggregate', operand: { op: 'ref', name: 'diamonds', type: 'DATASET' }, fn: 'quantile', attribute: { op: 'ref', name: 'added' }, value: 0.5 }
  { op: 'aggregate', operand: { op: 'ref', name: 'diamonds', type: 'DATASET' }, fn: 'quantile', attribute: { op: 'ref', name: 'added' }, value: 0.6 }

  { op: 'concat', operands: [{ op: 'literal', value: 'Honda' }, { op: 'literal', value: 'BMW' }, { op: 'literal', value: 'Suzuki' } ]}
  ###

  describe "does not die with hasOwnProperty", ->
    it "survives", ->
      expect(Expression.fromJS({
        op: 'literal'
        value: 'Honda'
        hasOwnProperty: 'troll'
      }).toJS()).to.deep.equal({
        op: 'literal'
        value: 'Honda'
      })


  describe "errors", ->
    it "does not like an expression without op", ->
      expect(->
        Expression.fromJS({
          name: 'hello'
        })
      ).to.throw('op must be defined')

    it "does not like an expression with a bad op", ->
      expect(->
        Expression.fromJS({
          op: 42
        })
      ).to.throw('op must be a string')

    it "does not like an expression with a unknown op", ->
      expect(->
        Expression.fromJS({
          op: 'this was once an empty file'
        })
      ).to.throw("unsupported expression op 'this was once an empty file'")


  describe "fancy names", ->
    it "behaves corretly with spaces", ->
      expect($("I blame your mother").toJS()).to.deep.equal({
        "op": "ref"
        "name": "I blame your mother"
      })

    it "works with fromJSLoose", ->
      expect(Expression.fromJSLoose("$^^{and do don't call me shirley}").toJS()).to.deep.equal({
        "op": "ref"
        "name": "and do don't call me shirley"
        "nest": 2
      })

    it "works with ref expression parse", ->
      expect(RefExpression.parse("{how are you today?}:NUMBER").toJS()).to.deep.equal({
        "op": "ref"
        "name": "how are you today?"
        "type": "NUMBER"
      })

    it "parses", ->
      expect(Expression.parse("$^{hello 'james'} + ${how are you today?}:NUMBER").toJS()).to.deep.equal({
        "op": "chain"
        "expression": {
          "name": "hello 'james'"
          "nest": 1
          "op": "ref"
        }
        "actions": [
          {
            "action": "add"
            "expression": {
              "name": "how are you today?"
              "op": "ref"
              "type": "NUMBER"
            }
          }
        ]
      })


  describe "#getFn", ->
    it "works in a simple case of IS", ->
      ex = $('x').is(8)
      exFn = ex.getFn()
      expect(exFn({x: 5})).to.equal(false)
      expect(exFn({x: 8})).to.equal(true)

    it "works in a simple case of addition", ->
      ex = $('x').add('$y', 5)
      exFn = ex.getFn()
      expect(exFn({x: 5, y: 1})).to.equal(11)
      expect(exFn({x: 8, y: -3})).to.equal(10)


  describe.skip '#decomposeAverage', ->
    it 'works', ->
      ex = $('data').average('$x')
      ex = ex.decomposeAverage()
      expect(ex.toString()).to.equal('($data.sum($x) / $data.count())')


  describe.skip '#distributeAggregates', ->
    it 'works in simple - case', ->
      ex = $('data').sum('-$x')
      ex = ex.distributeAggregates()
      expect(ex.toString()).to.equal('$data.sum($x).negate()')

    it 'works in simple + case', ->
      ex = $('data').sum('$x + $y')
      ex = ex.distributeAggregates()
      expect(ex.toString()).to.equal('($data.sum($x) + $data.sum($y))')

    it 'works in constant * case', ->
      ex = $('data').sum('$x * 6')
      ex = ex.distributeAggregates()
      expect(ex.toString()).to.equal('(6 * $data.sum($x))')

    it 'works in constant * case (multiple operands)', ->
      ex = $('data').sum('$x * 6 * $y')
      ex = ex.distributeAggregates()
      expect(ex.toString()).to.equal('(6 * $data.sum(($x * $y)))')

    it 'works in complex case', ->
      ex = $('data').sum('$x + $y - $z * 5 + 6')
      ex = ex.distributeAggregates()
      expect(ex.toString()).to.equal('($data.sum($x) + $data.sum($y) + (5 * $data.sum($z)).negate() + (6 * $data.count()))')
