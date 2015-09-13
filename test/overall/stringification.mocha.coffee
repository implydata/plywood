{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, Dataset, $, ply, r } = plywood

describe "stringification", ->
  it "works in advanced case", ->

    ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").is('D')))
      .apply('Count', $('diamonds').count())
      .apply('TotalPrice', $('diamonds').sum('$price'))
      .apply('Cuts',
        $("diamonds").split("$cut", 'Cut')
          .apply('diamonds', $('diamonds').filter($('cut').is('$^Cut')))
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(2)
          .apply('Carats',
            $("diamonds").split($("carat").numberBucket(0.25), 'Carat')
              .apply('diamonds', $('diamonds').filter($("carat").numberBucket(0.25).is('$^Carat')))
              .apply('Count', $('diamonds').count())
              .sort('$Count', 'descending')
              .limit(3)
          )
      )

    expect(ex.toString(2)).to.equal("""
    ply()
      .apply(
        diamonds,
        $diamonds.filter($color.is("D"))
        )
      .apply(Count, $diamonds.count())
      .apply(TotalPrice, $diamonds.sum($price))
      .apply(
        Cuts,
        $diamonds
          .split($cut, Cut, diamonds)
          .apply(
            diamonds,
            $diamonds.filter($cut.is($^Cut))
            )
          .apply(Count, $diamonds.count())
          .sort($Count, descending)
          .limit(2)
          .apply(
            Carats,
            $diamonds
              .split($carat.numberBucket(0.25), Carat, diamonds)
              .apply(
                diamonds,
                $diamonds.filter($carat.numberBucket(0.25).is($^Carat))
                )
              .apply(Count, $diamonds.count())
              .sort($Count, descending)
              .limit(3)
            )
        )
    """)
