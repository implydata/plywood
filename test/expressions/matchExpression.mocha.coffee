{ expect } = require("chai")

tests = require './sharedTests'

describe 'MatchExpression', ->
  describe 'with false value', ->
    beforeEach ->
      this.expression = { op: 'match', regexp: '^\\d+', operand: { op: 'literal', value: 'Honda' } }

    tests.expressionCountIs(2)
    tests.simplifiedExpressionIs({op: 'literal', value: false})

  describe 'with true value', ->
    beforeEach ->
      this.expression = { op: 'match', regexp: '^\\d+', operand: { op: 'literal', value: '123' } }

    tests.expressionCountIs(2)
    tests.simplifiedExpressionIs({op: 'literal', value: true})

  describe 'with reference value', ->
    beforeEach ->
      this.expression = { op: 'match', regexp: '^\\d+', operand: { op: 'ref', name: 'test' } }

    tests.expressionCountIs(2)
    tests.simplifiedExpressionIs({ op: 'match', regexp: '^\\d+', operand: { op: 'ref', name: 'test' }})
