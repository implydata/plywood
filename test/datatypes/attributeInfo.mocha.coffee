{ expect } = require("chai")

{ testImmutableClass } = require("immutable-class/build/tester")

plywood = require('../../build/plywood')
{ AttributeInfo, $, ply, r } = plywood

describe "NumberRange", ->
  it "is immutable class", ->
    testImmutableClass(AttributeInfo, [
      { name: 'time', type: 'TIME' }
      { name: 'color', type: 'STRING' }
      { name: 'cut', type: 'STRING' }
      { name: 'tags', type: 'SET/STRING' }
      { name: 'carat', type: 'NUMBER' }
      #{ name: 'height_bucket', special: 'range', type: 'NUMBER_RANGE', separator: ';', rangeSize: 0.05, digitsAfterDecimal: 2 }
      #{ name: 'height_bucket', special: 'range', type: 'NUMBER_RANGE', separator: '|', rangeSize: 0.05, digitsAfterDecimal: 2 }
      { name: 'count', type: 'NUMBER', unsplitable: true, makerAction: { action: 'count' } }
      { name: 'price', type: 'NUMBER', unsplitable: true, makerAction: { action: 'sum', expression: { op: 'ref', name: 'price' } } }
      { name: 'tax', type: 'NUMBER', unsplitable: true }
      { name: 'vendor_id', special: 'unique', type: "STRING", unsplitable: true }
    ])
