{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, $, ply, r } = plywood

describe "every", ->
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
    everyFn = (ex, index) ->
      indexes.push(index)
      return null

    expressionCount = ex.expressionCount()
    ex.every(everyFn)
    expect(expressionCount).to.equal(22)
    expect(indexes).to.deep.equal([0...expressionCount])

  it "has the right parameters", ->
    ex = ply()
      .apply('num', 2001001)
      .apply('subData',
        ply()
          .apply('x', '$num +  7003002')
          .apply('y', '$foo * 10003002')
          .apply('z', ply().sum(13003003).add(14003002))
          .apply('w', ply().sum('$a + 19004003 + $b'))
          .split('$x', 'X', 'data')
            .apply('x', '$num + 24003002')
            .apply('y', '$data:DATASET.sum(27003003) + 28003002')
            .apply('z', ply().sum(31003003).add(32003002))
            .apply('w', '34003002 + $data:DATASET.sum(37004003) + 38003002 + 39003002 == 40003002')
      )

    everyFn = (ex, index, depth, nestDiff) ->
      if ex.op is 'literal' and ex.type is 'NUMBER'
        expect(ex.value).to.equal(index * 1e6 + depth * 1e3 + nestDiff)
      return null

    ex.every(everyFn)
