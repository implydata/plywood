{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, Dataset, $, ply, r } = plywood

describe "reference check", ->

  context = {
    seventy: 70
    diamonds: Dataset.fromJS([
      { color: 'A', cut: 'great', carat: 1.1, price: 300 }
    ])
  }

  describe "errors", ->
    it "fails to resolve a variable that does not exist", ->
      ex = ply()
        .apply('num', 5)
        .apply('subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$foo * 2')
        )

      expect(->
        ex.referenceCheck({})
      ).to.throw('could not resolve $foo')

    it "fails to resolve a variable that does not exist (in scope)", ->
      ex = ply()
        .apply('num', 5)
        .apply('subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$^x * 2')
        )

      expect(->
        ex.referenceCheck({})
      ).to.throw('could not resolve $^x')

    it "fails to when a variable goes too deep", ->
      ex = ply()
        .apply('num', 5)
        .apply('subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$^^^x * 2')
        )

      expect(->
        ex.referenceCheck({ x: 5 })
      ).to.throw('went too deep on $^^^x')

    it "fails when discovering that the types mismatch", ->
      ex = ply()
        .apply('str', 'Hello')
        .apply('subData',
          ply()
            .apply('x', '$str + 1')
        )

      expect(->
        ex.referenceCheck({ str: 'Hello World' })
      ).to.throw('add must have input of type NUMBER')

    it "fails when discovering that the types mismatch via split", ->
      ex = ply()
        .apply("diamonds", $("diamonds").filter($('color').is('D')))
        .apply('Cuts',
          $("diamonds").split("$cut", 'Cut')
            .apply('TotalPrice', '$Cut * 10')
        )

      expect(->
        ex.referenceCheck(context)
      ).to.throw('multiply must have input of type NUMBER')


  describe "resolves", ->
    it "works in a basic case", ->
      ex = ply()
        .apply('num', 5)
        .apply('subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$x * 2')
        )

      ex2 = ply()
        .apply('num', 5)
        .apply('subData',
          ply()
            .apply('x', '$^num:NUMBER + 1')
            .apply('y', '$x:NUMBER * 2')
        )

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS())

    it "works with simple context", ->
      ex = ply()
        .apply('xPlusOne', '$x + 1')

      ex2 = ply()
        .apply('xPlusOne', '$^x:NUMBER + 1')

      expect(ex.referenceCheck({ x: 70 }).toJS()).to.deep.equal(ex2.toJS())

    it "works from context 1", ->
      ex = $('diamonds')
        .apply('priceOver2', '$price / 2')

      ex2 = $('diamonds', 'DATASET')
        .apply('priceOver2', '$price:NUMBER / 2')

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS())

    it "works from context 2", ->
      ex = ply()
        .apply('Diamonds', $('diamonds'))
        .apply('countPlusSeventy', '$Diamonds.count() + $seventy')

      ex2 = ply()
        .apply('Diamonds', $('diamonds', 1, 'DATASET'))
        .apply('countPlusSeventy', '$Diamonds:DATASET.count() + $^seventy:NUMBER')

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS())

    it "works with countDistinct", ->
      ex = ply()
        .apply('DistinctColors', '$diamonds.countDistinct($color)')
        .apply('DistinctCuts', '$diamonds.countDistinct($cut)')
        .apply('Diff', '$DistinctColors - $DistinctCuts')

      ex2 = ply()
        .apply('DistinctColors', '$^diamonds:DATASET.countDistinct($color:STRING)')
        .apply('DistinctCuts', '$^diamonds:DATASET.countDistinct($cut:STRING)')
        .apply('Diff', '$DistinctColors:NUMBER - $DistinctCuts:NUMBER')

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS())

    it "a total", ->
      ex = ply()
        .apply("diamonds", $("diamonds").filter($('color').is('D')))
        .apply('Count', '$diamonds.count()')
        .apply('TotalPrice', '$diamonds.sum($price)')

      ex2 = ply()
        .apply("diamonds", $("^diamonds:DATASET").filter($('color', 'STRING').is('D')))
        .apply('Count', '$diamonds:DATASET.count()')
        .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)')

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS())

    it "a split", ->
      ex = ply()
        .apply("diamonds", $("diamonds").filter($('color').is('D')))
        .apply('Count', '$diamonds.count()')
        .apply('TotalPrice', '$diamonds.sum($price)')
        .apply('Cuts',
          $("diamonds").split("$cut", 'Cut')
            .apply('Count2', '$diamonds.count()')
            .apply('TotalPrice2', '$diamonds.sum($price)')
            .apply('AvgPrice2', '$TotalPrice2 / $Count2')
            .sort('$AvgPrice2', 'descending')
            .limit(10)
        )

      ex2 = ply()
        .apply("diamonds", $("^diamonds:DATASET").filter($('color', 'STRING').is('D')))
        .apply('Count', '$diamonds:DATASET.count()')
        .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)')
        .apply('Cuts',
          $("diamonds", "DATASET").split("$cut:STRING", 'Cut')
            .apply('Count2', '$diamonds:DATASET.count()')
            .apply('TotalPrice2', '$diamonds:DATASET.sum($price:NUMBER)')
            .apply('AvgPrice2', '$TotalPrice2:NUMBER / $Count2:NUMBER')
            .sort('$AvgPrice2:NUMBER', 'descending')
            .limit(10)
        )

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS())

    it "a base split", ->
      ex = $("diamonds").split("$cut", 'Cut')
        .apply('Count', '$diamonds.count()')
        .apply('TotalPrice', '$diamonds.sum($price)')

      ex2 = $("diamonds", "DATASET").split("$cut:STRING", 'Cut')
        .apply('Count', '$diamonds:DATASET.count()')
        .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)')

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS())

    it "a base split + filter", ->
      ex = $("diamonds").filter($('color').is('D')).split("$cut", 'Cut')
        .apply('Count', '$diamonds.count()')
        .apply('TotalPrice', '$diamonds.sum($price)')

      ex2 = $("diamonds", "DATASET").filter($('color', 'STRING').is('D')).split("$cut:STRING", 'Cut')
        .apply('Count', '$diamonds:DATASET.count()')
        .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)')

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS())

    it "two splits", ->
      ex = ply()
        .apply("diamonds", $('diamonds').filter($("color").is('D')))
        .apply('Count', $('diamonds').count())
        .apply('TotalPrice', $('diamonds').sum('$price'))
        .apply('Cuts',
          $("diamonds").split("$cut", 'Cut')
            .apply('Count', $('diamonds').count())
            .apply('PercentOfTotal', '$diamonds.sum($price) / $TotalPrice')
            .sort('$Count', 'descending')
            .limit(2)
            .apply('Carats',
              $("diamonds").split($("carat").numberBucket(0.25), 'Carat')
                .apply('Count', $('diamonds').count())
                .sort('$Count', 'descending')
                .limit(3)
            )
        )

      ex2 = ply()
        .apply("diamonds", $('diamonds', 1, 'DATASET').filter($("color", "STRING").is('D')))
        .apply('Count', $('diamonds', 'DATASET').count())
        .apply('TotalPrice', $('diamonds', 'DATASET').sum('$price:NUMBER'))
        .apply('Cuts',
          $("diamonds", "DATASET").split("$cut:STRING", 'Cut')
            .apply('Count', $('diamonds', 'DATASET').count())
            .apply('PercentOfTotal', '$diamonds:DATASET.sum($price:NUMBER) / $^TotalPrice:NUMBER')
            .sort('$Count:NUMBER', 'descending')
            .limit(2)
            .apply('Carats',
              $("diamonds", "DATASET").split($("carat", "NUMBER").numberBucket(0.25), 'Carat')
                .apply('Count', $('diamonds', 'DATASET').count())
                .sort('$Count:NUMBER', 'descending')
                .limit(3)
            )
        )

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS())

    it "a join", ->
      ex = ply()
        .apply('Data1', $('diamonds').filter($('price').in(105, 305)))
        .apply('Data2', $('diamonds').filter($('price').in(105, 305).not()))
        .apply('Cuts'
          $('Data1').split('$cut', 'Cut', 'K1').join($('Data2').split('$cut', 'Cut', 'K2'))
            .apply('Count1', '$K1.count()')
            .apply('Count2', '$K2.count()')
        )

      ex2 = ply()
        .apply('Data1', $('diamonds', 1, 'DATASET').filter($('price', 'NUMBER').in(105, 305)))
        .apply('Data2', $('diamonds', 1, 'DATASET').filter($('price', 'NUMBER').in(105, 305).not()))
        .apply('Cuts'
          $('Data1', 'DATASET').split('$cut:STRING', 'Cut', 'K1').join($('Data2', 'DATASET').split('$cut:STRING', 'Cut', 'K2'))
            .apply('Count1', '$K1:DATASET.count()')
            .apply('Count2', '$K2:DATASET.count()')
        )

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS())
