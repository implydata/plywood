{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, $, ply } = plywood

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
            .apply('x', '$num + 24003003')
            .apply('y', '$data:DATASET.sum(27003004) + 28003003')
            .apply('z', ply().sum(31003004).add(32003003))
            .apply('w', '34003003 + $data:DATASET.sum(37004004) + 38003003 + 39003003 = 40003003')
      )

    everyFn = (ex, index, depth, nestDiff) ->
      if ex.op is 'literal' and ex.type is 'NUMBER'
        expect(ex.value).to.equal(index * 1e6 + depth * 1e3 + nestDiff)
      return null

    ex.every(everyFn)
