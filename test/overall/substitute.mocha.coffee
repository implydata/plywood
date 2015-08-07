{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, $ } = plywood

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
    ex = $()
      .apply('num', 5)
      .apply('subData',
        $()
          .apply('x', '$num + 1')
          .apply('y', '$foo * 2')
          .apply('z', $().sum('$a + 3'))
          .apply('w', $().sum('$a + 4 + $b'))
      )

    subs = (ex) ->
      if ex.op is 'literal' and ex.type is 'NUMBER'
        return Expression.fromJSLoose(ex.value + 10)
      else
        return null

    expect(ex.substitute(subs).toJS()).to.deep.equal(
      $()
        .apply('num', 15)
        .apply('subData',
          $()
            .apply('x', '$num + 11')
            .apply('y', '$foo * 12')
            .apply('z', $().sum('$a + 13'))
            .apply('w', $().sum('$a + 14 + $b'))
        )
        .toJS()
    )

  it "has sequential indexes", ->
    ex = $()
      .apply('num', 5)
      .apply('subData',
        $()
          .apply('x', '$num + 1')
          .apply('y', '$foo * 2')
          .apply('z', $().sum('$a + 3'))
          .apply('w', $().sum('$a + 4 + $b'))
      )

    indexes = []
    subs = (ex, index) ->
      indexes.push(index)
      return null

    expressionCount = ex.expressionCount()
    ex.substitute(subs)
    expect(expressionCount).to.equal(22)
    expect(indexes).to.deep.equal([0...expressionCount])

