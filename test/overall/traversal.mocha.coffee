{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, Dataset, $, ply } = plywood

describe "traversal", ->
  subs = (ex, index, depth, nestDiff) ->
    repNum = index * 1e6 + depth * 1e3 + nestDiff
    if ex.op is 'literal' and ex.type is 'NUMBER'
      expect(ex.value).to.equal(repNum)
    return null

  describe "has the right parameters", ->
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
            .apply('w', '34003002 + $data:DATASET.sum(37004003)')
      )

    it "on substitute", ->
      ex.substitute(subs)

    it "on every", ->
      ex.every(subs)


  describe "has the right parameters with dataset", ->
    data = [
      { cut: 'Good',  price: 400 }
      { cut: 'Good',  price: 300 }
      { cut: 'Great', price: 124 }
      { cut: 'Wow',   price: 160 }
      { cut: 'Wow',   price: 100 }
    ]

    ex = ply()
      .apply('Data', Dataset.fromJS(data))
      .apply('FooPlusCount', '4002001 + $Data.count()')
      .apply('CountPlusBar', '$Data.count() + 9002001')

    it "on substitute", ->
      ex.substitute(subs)

    it "on every", ->
      ex.every(subs)
