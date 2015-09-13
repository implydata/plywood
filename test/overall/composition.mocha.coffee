{ expect } = require("chai")

plywood = require('../../build/plywood')
{ $, ply, r } = plywood

describe "composition", ->
  it "works in blank case", ->
    ex = ply()
    expect(ex.toJS()).to.deep.equal({
      "op": "literal"
      "type": "DATASET"
      "value": [{}]
    })

  it "works in ref case", ->
    ex = $("diamonds")
    expect(ex.toJS()).to.deep.equal({
      "op": "ref"
      "name": "diamonds"
    })

  it "works in uber-basic case", ->
    ex = ply()
      .apply('five', 5)
      .apply('nine', 9)

    expect(ex.toJS()).to.deep.equal({
      "op": "chain"
      "expression": {
        "op": "literal"
        "type": "DATASET"
        "value": [{}]
      }
      "actions": [
        {
          "action": "apply"
          "name": "five"
          "expression": { "op": "literal", "value": 5 }
        }
        {
          "action": "apply"
          "name": "nine"
          "expression": { "op": "literal", "value": 9 }
        }
      ]
    })

  it "works in semi-realistic case", ->
    someDriver = {} # ToDo: fix this

    ex = ply()
      .apply("Diamonds",
        ply() # someDriver)
          .filter($('color').is('D'))
          .apply("priceOver2", $("price").divide(2))
      )
      .apply('Count', $('Diamonds').count())
      .apply('TotalPrice', $('Diamonds').sum('$priceOver2'))

    expect(ex.toJS()).to.deep.equal({
      "actions": [
        {
          "action": "apply"
          "expression": {
            "actions": [
              {
                "action": "filter"
                "expression": {
                  "actions": [
                    {
                      "action": "is"
                      "expression": {
                        "op": "literal"
                        "value": "D"
                      }
                    }
                  ]
                  "expression": {
                    "name": "color"
                    "op": "ref"
                  }
                  "op": "chain"
                }
              }
              {
                "action": "apply"
                "expression": {
                  "actions": [
                    {
                      "action": "divide"
                      "expression": {
                        "op": "literal"
                        "value": 2
                      }
                    }
                  ]
                  "expression": {
                    "name": "price"
                    "op": "ref"
                  }
                  "op": "chain"
                }
                "name": "priceOver2"
              }
            ]
            "expression": {
              "op": "literal"
              "type": "DATASET"
              "value": [
                {}
              ]
            }
            "op": "chain"
          }
          "name": "Diamonds"
        }
        {
          "action": "apply"
          "expression": {
            "actions": [
              {
                "action": "count"
              }
            ]
            "expression": {
              "name": "Diamonds"
              "op": "ref"
            }
            "op": "chain"
          }
          "name": "Count"
        }
        {
          "action": "apply"
          "expression": {
            "actions": [
              {
                "action": "sum"
                "expression": {
                  "name": "priceOver2"
                  "op": "ref"
                }
              }
            ]
            "expression": {
              "name": "Diamonds"
              "op": "ref"
            }
            "op": "chain"
          }
          "name": "TotalPrice"
        }
      ]
      "expression": {
        "op": "literal"
        "type": "DATASET"
        "value": [
          {}
        ]
      }
      "op": "chain"
    })

  it "works in semi-realistic case (using parser)", ->
    someDriver = {} # ToDo: fix this

    ex = ply()
      .apply("Diamonds",
        ply() #someDriver)
          #.filter("$color = 'D'")
          .apply("priceOver2", "$price/2")
      )
      .apply('Count', $('Diamonds').count())
      .apply('TotalPrice', $('Diamonds').sum('$priceOver2'))

    expect(ex.toJS()).to.deep.equal({
      "actions": [
        {
          "action": "apply"
          "expression": {
            "actions": [
              {
                "action": "apply"
                "expression": {
                  "actions": [
                    {
                      "action": "divide"
                      "expression": {
                        "op": "literal"
                        "value": 2
                      }
                    }
                  ]
                  "expression": {
                    "name": "price"
                    "op": "ref"
                  }
                  "op": "chain"
                }
                "name": "priceOver2"
              }
            ]
            "expression": {
              "op": "literal"
              "type": "DATASET"
              "value": [
                {}
              ]
            }
            "op": "chain"
          }
          "name": "Diamonds"
        }
        {
          "action": "apply"
          "expression": {
            "actions": [
              {
                "action": "count"
              }
            ]
            "expression": {
              "name": "Diamonds"
              "op": "ref"
            }
            "op": "chain"
          }
          "name": "Count"
        }
        {
          "action": "apply"
          "expression": {
            "actions": [
              {
                "action": "sum"
                "expression": {
                  "name": "priceOver2"
                  "op": "ref"
                }
              }
            ]
            "expression": {
              "name": "Diamonds"
              "op": "ref"
            }
            "op": "chain"
          }
          "name": "TotalPrice"
        }
      ]
      "expression": {
        "op": "literal"
        "type": "DATASET"
        "value": [
          {}
        ]
      }
      "op": "chain"
    })
