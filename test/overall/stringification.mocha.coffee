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

  it "works with lookup", ->
    ex = $('diamonds').split("$cut.lookup('hello_lookup')", 'CutLookup')

    expect(ex.toString(2)).to.equal("$diamonds.split($cut.lookup(hello_lookup),CutLookup,diamonds)")

  it "works with timePart", ->
    ex = $('time').timePart('DAY_OF_WEEK')
    expect(ex.toString(2)).to.equal("$time.timePart(DAY_OF_WEEK)")

  it "works with timeShift", ->
    ex = $('time').timeShift('P1D', 2)
    expect(ex.toString(2)).to.equal("$time.timeShift(P1D,2)")

  it "works with timeRange", ->
    ex = $('time').timeRange('P1D', 2)
    expect(ex.toString(2)).to.equal("$time.timeRange(P1D,2)")
