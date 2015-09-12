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
      "op": "actions"
      "operand": {
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
      "op": "actions"
      "operand": {
        "op": "literal"
        "type": "DATASET"
        "value": [{}]
      }
      "actions": [
        {
          "action": "apply"
          "name": "Diamonds"
          "expression": {
            "op": "actions"
            "operand": {
              "op": "literal"
              "type": "DATASET"
              "value": [{}]
            }
            "actions": [
              {
                "action": "filter"
                "expression": {
                  "lhs": { "name": "color", "op": "ref" }
                  "op": "is"
                  "rhs": { "op": "literal", "value": "D" }
                }
              }
              {
                "action": "apply"
                "name": "priceOver2"
                "expression": {
                  "op": "multiply"
                  "operands": [
                    { "op": "ref", "name": "price" }
                    { "op": "reciprocate", "operand": { "op": "literal", "value": 2 } }
                  ]
                }
              }
            ]
          }
        }
        {
          "action": "apply"
          "name": "Count"
          "expression": {
            "fn": "count"
            "op": "aggregate"
            "operand": {
              "name": "Diamonds"
              "op": "ref"
            }
          }
        }
        {
          "action": "apply"
          "name": "TotalPrice"
          "expression": {
            "op": "aggregate"
            "operand": { "op": "ref", "name": "Diamonds" }
            "fn": "sum"
            "attribute": { "op": "ref", "name": "priceOver2" }
          }
        }
      ]
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
                  "op": "multiply"
                  "operands": [
                    {
                      "name": "price"
                      "op": "ref"
                    }
                    {
                      "op": "reciprocate"
                      "operand": {
                        "op": "literal"
                        "value": 2
                      }
                    }
                  ]
                }
                "name": "priceOver2"
              }
            ]
            "op": "actions"
            "operand": {
              "op": "literal"
              "type": "DATASET"
              "value": [{}]
            }
          }
          "name": "Diamonds"
        }
        {
          "action": "apply"
          "expression": {
            "fn": "count"
            "op": "aggregate"
            "operand": {
              "name": "Diamonds"
              "op": "ref"
            }
          }
          "name": "Count"
        }
        {
          "action": "apply"
          "expression": {
            "attribute": {
              "name": "priceOver2"
              "op": "ref"
            }
            "fn": "sum"
            "op": "aggregate"
            "operand": {
              "name": "Diamonds"
              "op": "ref"
            }
          }
          "name": "TotalPrice"
        }
      ]
      "op": "actions"
      "operand": {
        "op": "literal"
        "type": "DATASET"
        "value": [{}]
      }
    })
