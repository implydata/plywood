{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, Dataset, $, ply } = plywood

describe "compute native", ->
  data = [
    { cut: 'Good',  price: 400 }
    { cut: 'Good',  price: 300 }
    { cut: 'Great', price: 124 }
    { cut: 'Wow',   price: 160 }
    { cut: 'Wow',   price: 100 }
  ]

  it "works in uber-basic case", (testComplete) ->
    ex = ply()
      .apply('five', 5)
      .apply('nine', 9)

    p = ex.compute()
    p.then((v) ->
      expect(v.toJS()).to.deep.equal([
        {
          five: 5
          nine: 9
        }
      ])
      testComplete()
    ).done()

  it "works in existing dataset case", (testComplete) ->
    ds = Dataset.fromJS([
      { cut: 'Good',  price: 400 }
      { cut: 'Great', price: 124 }
      { cut: 'Wow',   price: 160 }
    ])

    ex = $(ds)
      .apply('priceX2', $('price').multiply(2))

    p = ex.compute()
    p.then((v) ->
      expect(v.toJS()).to.deep.equal([
        { cut: 'Good',  price: 400, priceX2: 800 }
        { cut: 'Great', price: 124, priceX2: 248 }
        { cut: 'Wow',   price: 160, priceX2: 320 }
      ])
      testComplete()
    ).done()

  it "works with simple split aggregator", (testComplete) ->
    ds = Dataset.fromJS(data).hide()

    ex = ply()
      .apply('Data', $(ds))
      .apply('Cuts'
        $('Data').split('$cut', 'Cut')
      )

    p = ex.compute()
    p.then((v) ->
      expect(v.toJS()).to.deep.equal([
        {
          "Cuts": [
            { "Cut": "Good" }
            { "Cut": "Great" }
            { "Cut": "Wow" }
          ]
        }
      ])
      testComplete()
    ).done()

  it "works with simple split followed by some simple applies", (testComplete) ->
    ds = Dataset.fromJS(data).hide()

    ex = ply()
      .apply('Data', $(ds))
      .apply('Cuts'
        $('Data').split('$cut', 'Cut')
          .apply('Six', 6)
          .apply('Seven', $('Six').add(1))
      )

    p = ex.compute()
    p.then((v) ->
      expect(v.toJS()).to.deep.equal([
        {
          "Cuts": [
            {
              "Cut": "Good"
              "Six": 6
              "Seven": 7
            }
            {
              "Cut": "Great"
              "Six": 6
              "Seven": 7
            }
            {
              "Cut": "Wow"
              "Six": 6
              "Seven": 7
            }
          ]
        }
      ])
      testComplete()
    ).done()

  it "works with context", (testComplete) ->
    ds = Dataset.fromJS(data).hide()

    ex = ply()
      .apply('Data', $(ds))
      .apply('CountPlusX', '$Data.count() + $x')

    p = ex.compute({ x: 13 })
    p.then((v) ->
      expect(v.toJS()).to.deep.equal([
        {
          "CountPlusX": 18
        }
      ])
      testComplete()
    ).done()

  it "works with context and split", (testComplete) ->
    ds = Dataset.fromJS(data).hide()

    ex = ply()
      .apply('Data', $(ds))
      .apply('Cuts'
        $('Data').split('$cut', 'Cut')
          .apply('CountPlusX', '$Data.count() + $x')
      )

    p = ex.compute({ x: 13 })
    p.then((v) ->
      expect(v.toJS()).to.deep.equal([
        {
          "Cuts": [
            {
              "CountPlusX": 15
              "Cut": "Good"
            }
            {
              "CountPlusX": 14
              "Cut": "Great"
            }
            {
              "CountPlusX": 15
              "Cut": "Wow"
            }
          ]
        }
      ])
      testComplete()
    ).done()

  it "works with simple split and sub apply", (testComplete) ->
    ds = Dataset.fromJS(data).hide()

    ex = ply()
      .apply('Data', $(ds))
      .apply('Cuts'
        $('Data').split('$cut', 'Cut')
          .apply('Count', $('Data').count())
      )

    p = ex.compute()
    p.then((v) ->
      expect(v.toJS()).to.deep.equal([
        {
          "Cuts": [
            {
              "Count": 2
              "Cut": "Good"
            }
            {
              "Count": 1
              "Cut": "Great"
            }
            {
              "Count": 2
              "Cut": "Wow"
            }
          ]
        }
      ])
      testComplete()
    ).done()

  it "works with simple split and sub apply + sort + limit", (testComplete) ->
    ds = Dataset.fromJS(data).hide()

    ex = ply()
      .apply('Data', $(ds))
      .apply('Cuts'
        $('Data').split('$cut', 'Cut')
          .apply('Count', $('Data').count())
          .sort('$Cut', 'descending')
          .limit(2)
      )

    p = ex.compute()
    p.then((v) ->
      expect(v.toJS()).to.deep.equal([
        {
          "Cuts": [
            {
              "Count": 2
              "Cut": "Wow"
            }
            {
              "Count": 1
              "Cut": "Great"
            }
          ]
        }
      ])
      testComplete()
    ).done()

  it "works with simple filter", (testComplete) ->
    ds = Dataset.fromJS(data).hide()

    ex = ply()
      .apply('Data', $(ds).filter($('price').in(105, 305)))
      .apply('Count', '$Data.count()')

    p = ex.compute()
    p.then((v) ->
      expect(v.toJS()).to.deep.equal([
        {
          "Count": 3
        }
      ])
      testComplete()
    ).done()


  describe "it works with re-selects", ->
    ds = Dataset.fromJS(data).hide()
    midData = null

    it "works with simple group/label and subData filter with applies", (testComplete) ->
      ex = ply()
        .apply('Data', $(ds))
        .apply('Count', '$Data.count()')
        .apply('Price', '$Data.sum($price)')
        .apply('Cuts'
          $('Data').split('$cut', 'Cut')
            .apply('Count', '$Data.count()')
            .apply('Price', '$Data.sum($price)')
        )

      p = ex.compute()
      p.then((v) ->
        midData = v
        expect(midData.toJS()).to.deep.equal([
          {
            "Count": 5
            "Price": 1084
            "Cuts": [
              {
                "Cut": "Good"
                "Count": 2
                "Price": 700
              }
              {
                "Cut": "Great"
                "Count": 1
                "Price": 124
              }
              {
                "Cut": "Wow"
                "Count": 2
                "Price": 260
              }
            ]
          }
        ])
        testComplete()
      ).done()

    it "re-selects", (testComplete) ->
      ex = $(midData)
        .apply('CountOver2', '$Count / 2')
        .apply('Cuts'
          $('Cuts')
            .apply('AvgPrice', '$Data.sum($price) / $Data.count()')
        )

      p = ex.compute()
      p.then((v) ->
        expect(v.toJS()).to.deep.equal([
          {
            "Count": 5
            "CountOver2": 2.5
            "Cuts": [
              {
                "AvgPrice": 350
                "Count": 2
                "Cut": "Good"
                "Price": 700
              }
              {
                "AvgPrice": 124
                "Count": 1
                "Cut": "Great"
                "Price": 124
              }
              {
                "AvgPrice": 130
                "Count": 2
                "Cut": "Wow"
                "Price": 260
              }
            ]
            "Price": 1084
          }
        ])
        testComplete()
      ).done()


  describe "joins", ->
    it "does a join on split", (testComplete) ->
      ds = Dataset.fromJS(data).hide()

      ex = ply()
        .apply('Data1', $(ds).filter($('price').in(105, 305)))
        .apply('Data2', $(ds).filter($('price').in(105, 305).not()))
        .apply('Count1', '$Data1.count()')
        .apply('Count2', '$Data2.count()')
        .apply('Cuts'
          $('Data1').split('$cut', 'Cut').join($('Data2').split('$cut', 'Cut', 'K2'))
            .apply('Counts', '100 * $Data1.count() + $K2.count()')
        )

      p = ex.compute()
      p.then((v) ->
        expect(v.toJS()).to.deep.equal([
          {
            "Count1": 3
            "Count2": 2
            "Cuts": [
              {
                "Counts": 101
                "Cut": "Good"
              }
              {
                "Counts": 100
                "Cut": "Great"
              }
              {
                "Counts": 101
                "Cut": "Wow"
              }
            ]
          }
        ])
        testComplete()
      ).done()
