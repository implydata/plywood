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
      $(5).is('hello')
    ).to.throw('is must have input of type STRING (is NUMBER)')

  it "should throw on unbalanced IS (via explicit type)", ->
    expect(->
      $(5).is('$hello:STRING')
    ).to.throw('is must have input of type STRING (is NUMBER)')

  it "should throw on non numeric lessThan", ->
    expect(->
      $(5).lessThan('hello')
    ).to.throw('lessThan must have expression of type NUMBER or TIME (is STRING)')

  it "should throw on bad in", ->
    expect(->
      $(5).in('hello')
    ).to.throw('in action has a bad type combination NUMBER in STRING')
