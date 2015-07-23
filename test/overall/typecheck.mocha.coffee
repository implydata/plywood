{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, $ } = plywood

describe "typecheck", ->
  it "should throw silly ref type", ->
    expect(->
      Expression.fromJS({ op: 'ref', type: 'Corn', name: 'str' })
    ).to.throw("unsupported type 'Corn'")

  it "should throw on unbalanced IS", ->
    expect(->
      Expression.fromJS({
        op: 'is'
        lhs: 5
        rhs: 'hello'
      })
    ).to.throw('is expression must have matching types, (are: NUMBER, STRING)')

  it "should throw on unbalanced IS (via explicit type)", ->
    expect(->
      Expression.fromJS({
        op: 'is'  
        lhs: 5
        rhs: { op: 'ref', type: 'STRING', name: 'str' }
      })
    ).to.throw('is expression must have matching types, (are: NUMBER, STRING)')

  it "should throw on non numeric lessThan", ->
    expect(->
      Expression.fromJS({
        op: 'lessThan'
        lhs: 5
        rhs: 'hello'
      })
    ).to.throw('lessThan expression must have matching types, (are: NUMBER, STRING)')

  it "should throw on bad in", ->
    expect(->
      Expression.fromJS({
        op: 'in'
        lhs: 5
        rhs: 'hello'
      })
    ).to.throw('in expression has a bad type combination NUMBER in STRING')
