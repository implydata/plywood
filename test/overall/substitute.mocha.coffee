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

  it "has the right parameters", ->
    ex = $()
      .apply('num', 2001001)
      .apply('subData',
        $()
          .apply('x', '$num +  7003002')
          .apply('y', '$foo * 10003002')
          .apply('z', $().sum(13003003).add(14003002))
          .apply('w', $().sum('$a + 19004003 + $b'))
          .split('$x', 'X', 'data')
            .apply('x', '$num + 24003003')
            .apply('y', '$data:DATASET.sum(27003004) + 28003003')
            .apply('z', $().sum(31003004).add(32003003))
            .apply('w', '34003003 + $data:DATASET.sum(37004004)')
      )

    subs = (ex, index, depth, nestDiff) ->
      if ex.op is 'literal' and ex.type is 'NUMBER'
        expect(ex.value).to.equal(index * 1e6 + depth * 1e3 + nestDiff)
      return null

    ex.substitute(subs)
