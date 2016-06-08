var { expect } = require("chai");
var { sane } = require('../utils');

var plywood = require('../../build/plywood');
var { Expression, Dataset, $, ply, r } = plywood;

describe("stringification", () => {
  it("works in advanced case", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").is('D')))
      .apply('Count', $('diamonds').count())
      .apply('TotalPrice', $('diamonds').sum('$price'))
      .apply(
        'Cuts',
        $("diamonds").split("$cut", 'Cut')
          .apply('diamonds', $('diamonds').filter($('cut').is('$^Cut')))
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(2)
          .apply(
            'Carats',
            $("diamonds").split($("carat").numberBucket(0.25), 'Carat')
              .apply('diamonds', $('diamonds').filter($("carat").numberBucket(0.25).is('$^Carat')))
              .apply('Count', $('diamonds').count())
              .apply('Price :-)', $('diamonds').sum('$price'))
              .sort('$Count', 'descending')
              .limit(3)
          )
      );

    expect(ex.toString(2)).to.equal(sane`
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
                .apply("Price :-)", $diamonds.sum($price))
                .sort($Count, descending)
                .limit(3)
              )
          )
    `);
  });

  it("works with fancy ref", () => {
    var ex = $('!T_0');
    expect(ex.toString()).to.equal('${!T_0}');
  });

  it("works with lookup", () => {
    var ex = $('diamonds').split("$cut.lookup('hello_lookup')", 'CutLookup');
    expect(ex.toString(2)).to.equal("$diamonds.split($cut.lookup(hello_lookup),CutLookup,diamonds)");
  });

  it("works with timePart", () => {
    var ex = $('time').timePart('DAY_OF_WEEK');
    expect(ex.toString(2)).to.equal("$time.timePart(DAY_OF_WEEK)");
  });

  it("works with timeShift", () => {
    var ex = $('time').timeShift('P1D', 2);
    expect(ex.toString(2)).to.equal("$time.timeShift(P1D,2)");
  });

  it("works with timeRange", () => {
    var ex = $('time').timeRange('P1D', 2);
    expect(ex.toString(2)).to.equal("$time.timeRange(P1D,2)");
  });

  it("works with custom", () => {
    var ex = $('x').custom('lol');
    expect(ex.toString(2)).to.equal("$x.custom(lol)");
  });

  it("works with substr", () => {
    var ex = $('x').substr(1, 5);
    expect(ex.toString(2)).to.equal("$x.substr(1,5)");
  });
});
