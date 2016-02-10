{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, $, ply, r } = plywood

describe "typecheck", ->
  it "should throw silly ref type", ->
    expect(->
      Expression.fromJS({ op: 'ref', type: 'Corn', name: 'str' })
    ).to.throw("unsupported type 'Corn'")

  it "should throw on unbalanced IS", ->
    expect(->
      r(5).is('hello')
    ).to.throw('is must have input of type STRING (is NUMBER)')

  it "should throw on unbalanced IS (via explicit type)", ->
    expect(->
      r(5).is('$hello:STRING')
    ).to.throw('is must have input of type STRING (is NUMBER)')

  it "should throw on non numeric lessThan", ->
    expect(->
      r(5).lessThan('hello')
    ).to.throw('lessThan must have expression of type NUMBER or TIME (is STRING)')

  it "should throw on bad in", ->
    expect(->
      r(5).in('hello')
    ).to.throw('in action has a bad type combination NUMBER in STRING')

  it "should throw on mismatching fallback type", ->
    expect(->
      r(5).fallback('hello')
    ).to.throw('fallback must have input of type STRING (is NUMBER)')
