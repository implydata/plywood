{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, $, ply } = plywood

describe "substitute", ->
  it "should substitute on IS", ->
    ex = $(5).is('$hello')

    subs = (ex) ->
      if ex.op is 'literal' and ex.type is 'NUMBER'
        return Expression.fromJSLoose(ex.value + 10)
      else
        return null

    expect(ex.substitute(subs).toJS()).to.deep.equal(
      $(15).is('$hello').toJS()
    )

  it "should substitute on complex expression", ->
    ex = ply()
      .apply('num', 5)
      .apply('subData',
        ply()
          .apply('x', '$num + 1')
          .apply('y', '$foo * 2')
          .apply('z', ply().sum('$a + 3'))
          .apply('w', ply().sum('$a + 4 + $b'))
      )

    subs = (ex) ->
      if ex.op is 'literal' and ex.type is 'NUMBER'
        return Expression.fromJSLoose(ex.value + 10)
      else
        return null

    expect(ex.substitute(subs).toJS()).to.deep.equal(
      ply()
        .apply('num', 15)
        .apply('subData',
          ply()
            .apply('x', '$num + 11')
            .apply('y', '$foo * 12')
            .apply('z', ply().sum('$a + 13'))
            .apply('w', ply().sum('$a + 14 + $b'))
        )
        .toJS()
    )

  it "has sequential indexes", ->
    ex = ply()
      .apply('num', 5)
      .apply('subData',
        ply()
          .apply('x', '$num + 1')
          .apply('y', '$foo * 2')
          .apply('z', ply().sum('$a + 3'))
          .apply('w', ply().sum('$a + 4 + $b'))
      )

    indexes = []
    subs = (ex, index) ->
      indexes.push(index)
      return null

    expressionCount = ex.expressionCount()
    ex.substitute(subs)
    expect(expressionCount).to.equal(22)
    expect(indexes).to.deep.equal([0...expressionCount])

