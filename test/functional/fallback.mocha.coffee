{ expect } = require("chai")

{ testImmutableClass } = require("immutable-class/build/tester")

plywood = require('../../build/plywood')
{ Dataset, Action, $, ply, r, MatchAction } = plywood

describe "FallbackAction", ->
  it "works?", ->
    data = Dataset.fromJS([
      { cut: 'Good',  price: 400, time: new Date('2015-10-01T10:20:30Z') },
      { cut: 'Good',  price: 300, time: new Date('2015-10-02T10:20:30Z') }
    ])
    ex = $('data').filter('$cut == "Bad"')
    .fallback("");
    ex.compute({ data: data }).then(console.log);
    ex = r(null).fallback("fallbackValue");

    ex.compute().then(console.log);