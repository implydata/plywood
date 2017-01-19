/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2017 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { expect } = require("chai");
let { sane } = require('../utils');

let plywood = require('../plywood');
let { Expression, Dataset, $, i$, ply, r } = plywood;

describe("stringification", () => {
  it("works in advanced case", () => {
    let ex = ply()
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

    expect(ex.toString(0)).to.equal(sane`
      ply()
        .apply(
          diamonds,
          $diamonds.filter($color.is("D"))
        )
        .apply(Count,$diamonds.count())
        .apply(TotalPrice,$diamonds.sum($price))
        .apply(
          Cuts,
          $diamonds.split($cut,Cut,diamonds)
          .apply(
            diamonds,
            $diamonds.filter($cut.is($^Cut))
          )
          .apply(Count,$diamonds.count()).sort($Count,descending).limit(2)
          .apply(
            Carats,
            $diamonds.split($carat.numberBucket(0.25),Carat,diamonds)
              .apply(
                diamonds,
                $diamonds.filter($carat.numberBucket(0.25).is($^Carat))
              )
              .apply(Count,$diamonds.count())
              .apply("Price :-)",$diamonds.sum($price)).sort($Count,descending).limit(3)
          )
        )
    `);
  });

  it("works with fancy ref", () => {
    let ex = $('!T_0');
    expect(ex.toString()).to.equal('${!T_0}');
  });

  it("works with case insensitive refs", () => {
    let ex = i$('x').substr(1, 5);
    expect(ex.toString(2)).to.equal("i$x.substr(1,5)");
  });

  it("works with lookup", () => {
    let ex = $('diamonds').split("$cut.lookup('hello_lookup')", 'CutLookup');
    expect(ex.toString(2)).to.equal("$diamonds.split($cut.lookup(hello_lookup),CutLookup,diamonds)");
  });

  it("works with timePart", () => {
    let ex = $('time').timePart('DAY_OF_WEEK');
    expect(ex.toString(2)).to.equal("$time.timePart(DAY_OF_WEEK)");
  });

  it("works with timeShift", () => {
    let ex = $('time').timeShift('P1D', 2);
    expect(ex.toString(2)).to.equal("$time.timeShift(P1D,2)");
  });

  it("works with timeRange", () => {
    let ex = $('time').timeRange('P1D', 2);
    expect(ex.toString(2)).to.equal("$time.timeRange(P1D,2)");
  });

  it("works with customAggregate", () => {
    let ex = $('x').customAggregate('lol');
    expect(ex.toString(2)).to.equal("$x.customAggregate(lol)");
  });

  it("works with substr", () => {
    let ex = $('x').substr(1, 5);
    expect(ex.toString(2)).to.equal("$x.substr(1,5)");
  });
});
