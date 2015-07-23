tests = require './sharedTests'

describe 'TimeOffsetExpression', ->
  describe 'with simple expression', ->
    beforeEach ->
      this.expression = {
        op: 'timeOffset',
        operand: {
          op: 'literal'
          value: new Date('2015-02-20T15:41:12')
        }
        duration: 'P1D'
        timezone: 'Etc/UTC'
      }

    tests.expressionCountIs(2)
    tests.simplifiedExpressionIs({
      op: 'literal'
      value: new Date('2015-02-21T15:41:12')
    })
