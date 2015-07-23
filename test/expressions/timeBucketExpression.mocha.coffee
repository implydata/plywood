tests = require './sharedTests'

describe 'TimeBucketExpression', ->
  describe 'with simple expression', ->
    beforeEach ->
      this.expression = {
        op: 'timeBucket',
        operand: {
          op: 'literal'
          value: new Date("2015-02-19T05:59:02.822Z")
        }
        duration: 'P1D'
        timezone: 'Etc/UTC'
      }

    tests.expressionCountIs(2)
    tests.simplifiedExpressionIs({
      "op": "literal"
      "type": "TIME_RANGE"
      "value": {
        "end": new Date("2015-02-20T00:00:00.000Z")
        "start": new Date("2015-02-19T00:00:00.000Z")
      }
    })
