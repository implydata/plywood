{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, Dataset, $ } = plywood

describe "reference check", ->

  context = {
    seventy: 70
    diamonds: Dataset.fromJS([
      { color: 'A', cut: 'great', carat: 1.1, price: 300 }
    ])
  }

  describe "errors", ->
    it "fails to resolve a variable that does not exist", ->
      ex = $()
        .apply('num', 5)
        .apply('subData',
          $()
            .apply('x', '$num + 1')
            .apply('y', '$foo * 2')
        )

      expect(->
        ex.referenceCheck({})
      ).to.throw('could not resolve $foo')

    it "fails to resolve a variable that does not exist (in scope)", ->
      ex = $()
        .apply('num', 5)
        .apply('subData',
          $()
            .apply('x', '$num + 1')
            .apply('y', '$^x * 2')
        )

      expect(->
        ex.referenceCheck({})
      ).to.throw('could not resolve $^x')

    it "fails to when a variable goes too deep", ->
      ex = $()
        .apply('num', 5)
        .apply('subData',
          $()
            .apply('x', '$num + 1')
            .apply('y', '$^^^x * 2')
        )

      expect(->
        ex.referenceCheck({ x: 5 })
      ).to.throw('went too deep on $^^^x')

    it "fails when discovering that the types mismatch", ->
      ex = $()
        .apply('str', 'Hello')
        .apply('subData',
          $()
            .apply('x', '$str + 1')
        )

      expect(->
        ex.referenceCheck({ str: 'Hello World' })
      ).to.throw('add must have input of type NUMBER')

    it "fails when discovering that the types mismatch via split", ->
      ex = $()
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
      ex = $()
        .apply('num', 5)
        .apply('subData',
          $()
            .apply('x', '$num + 1')
            .apply('y', '$x * 2')
        )

      expect(ex.referenceCheck({}).toJS()).to.deep.equal(
        $()
          .apply('num', 5)
          .apply('subData',
            $()
              .apply('x', '$^num:NUMBER + 1')
              .apply('y', '$x:NUMBER * 2')
          )
          .toJS()
      )

    it "works with simple context", ->
      ex = $()
        .apply('xPlusOne', '$x + 1')

      expect(ex.referenceCheck({ x: 70 }).toJS()).to.deep.equal(
        $()
          .apply('xPlusOne', '$^x:NUMBER + 1')
          .toJS()
      )

    it "works from context 1", ->
      ex = $('diamonds')
        .apply('priceOver2', '$price / 2')

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(
        $('diamonds:DATASET')
          .apply('priceOver2', '$price:NUMBER / 2')
          .toJS()
      )

    it "works from context 2", ->
      ex = $()
        .apply('Diamonds', $('diamonds'))
        .apply('countPlusSeventy', '$Diamonds.count() + $seventy')

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(
        $()
          .apply('Diamonds', $('^diamonds:DATASET'))
          .apply('countPlusSeventy', '$Diamonds:DATASET.count() + $^seventy:NUMBER')
          .toJS()
      )

    it "a total", ->
      ex = $()
        .apply("diamonds", $("diamonds").filter($('color').is('D')))
        .apply('Count', '$diamonds.count()')
        .apply('TotalPrice', '$diamonds.sum($price)')

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(
        $()
          .apply("diamonds", $("^diamonds:DATASET").filter($('color:STRING').is('D')))
          .apply('Count', '$diamonds:DATASET.count()')
          .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)')
          .toJS()
      )

    it "a split", ->
      ex = $()
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

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(
        $()
          .apply("diamonds", $("^diamonds:DATASET").filter($('color:STRING').is('D')))
          .apply('Count', '$diamonds:DATASET.count()')
          .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)')
          .apply('Cuts',
            $("diamonds:DATASET").split("$cut:STRING", 'Cut')
              .apply('Count2', '$diamonds:DATASET.count()')
              .apply('TotalPrice2', '$diamonds:DATASET.sum($price:NUMBER)')
              .apply('AvgPrice2', '$TotalPrice2:NUMBER / $Count2:NUMBER')
              .sort('$AvgPrice2:NUMBER', 'descending')
              .limit(10)
          )
          .toJS()
      )

    it "a base split", ->
      ex = $("diamonds").split("$cut", 'Cut')
        .apply('Count', '$diamonds.count()')
        .apply('TotalPrice', '$diamonds.sum($price)')

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(
        $("diamonds:DATASET").split("$cut:STRING", 'Cut')
          .apply('Count', '$diamonds:DATASET.count()')
          .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)')
          .toJS()
      )

    it "a base split + filter", ->
      ex = $("diamonds").filter($('color').is('D')).split("$cut", 'Cut')
        .apply('Count', '$diamonds.count()')
        .apply('TotalPrice', '$diamonds.sum($price)')

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(
        $("diamonds:DATASET").filter($('color:STRING').is('D')).split("$cut:STRING", 'Cut')
          .apply('Count', '$diamonds:DATASET.count()')
          .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)')
          .toJS()
      )

    it "two splits", ->
      ex = $()
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

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(
        $()
          .apply("diamonds", $('^diamonds:DATASET').filter($("color:STRING").is('D')))
          .apply('Count', $('diamonds:DATASET').count())
          .apply('TotalPrice', $('diamonds:DATASET').sum('$price:NUMBER'))
          .apply('Cuts',
            $("diamonds:DATASET").split("$cut:STRING", 'Cut')
              .apply('Count', $('diamonds:DATASET').count())
              .apply('PercentOfTotal', '$diamonds:DATASET.sum($price:NUMBER) / $^TotalPrice:NUMBER')
              .sort('$Count:NUMBER', 'descending')
              .limit(2)
              .apply('Carats',
                $("diamonds:DATASET").split($("carat:NUMBER").numberBucket(0.25), 'Carat')
                  .apply('Count', $('diamonds:DATASET').count())
                  .sort('$Count:NUMBER', 'descending')
                  .limit(3)
              )
          )
          .toJS()
      )

    it "a join", ->
      ex = $()
        .apply('Data1', $('diamonds').filter($('price').in(105, 305)))
        .apply('Data2', $('diamonds').filter($('price').in(105, 305).not()))
        .apply('Cuts'
          $('Data1').split('$cut', 'Cut', 'K1').join($('Data2').split('$cut', 'Cut', 'K2'))
            .apply('Count1', '$K1.count()')
            .apply('Count2', '$K2.count()')
        )

      expect(ex.referenceCheck(context).toJS()).to.deep.equal(
        $()
          .apply('Data1', $('^diamonds:DATASET').filter($('price:NUMBER').in(105, 305)))
          .apply('Data2', $('^diamonds:DATASET').filter($('price:NUMBER').in(105, 305).not()))
          .apply('Cuts'
            $('Data1:DATASET').split('$cut:STRING', 'Cut', 'K1').join($('Data2:DATASET').split('$cut:STRING', 'Cut', 'K2'))
              .apply('Count1', '$K1:DATASET.count()')
              .apply('Count2', '$K2:DATASET.count()')
          )
          .toJS()
      )
