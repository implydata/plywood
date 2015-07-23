tests = require './sharedTests'

describe 'NumberBucketExpression', ->
  describe 'with simple expression', ->
    beforeEach ->
      this.expression = {
        op: 'numberBucket'
        operand: { op: 'literal', value: 1.01 }
        size: 0.05
        offset: 1
      }

    tests.expressionCountIs(2)
    tests.simplifiedExpressionIs({
      "op": "literal"
      "type": "NUMBER_RANGE"
      "value": {
        "end": 1.05
        "start": 1
      }
    })

  describe 'with complex expression', ->
    beforeEach ->
      this.expression = {
        op: 'numberBucket'
        operand: {
          op: 'multiply',
          operands: [
            { op: 'literal', value: 1 }
            { op: 'literal', value: 4 }
          ]
        }
        size: 0.05
        offset: 1
      }

    tests.expressionCountIs(4)
    tests.simplifiedExpressionIs({
      "op": "literal"
      "type": "NUMBER_RANGE"
      "value": {
        "end": 4.05
        "start": 4
      }
    })
