/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2020 Imply Data, Inc.
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

const { expect } = require('chai');
const { sane } = require('../utils');

const plywood = require('../plywood');

const { Expression, Dataset, $, i$, ply, r } = plywood;

describe('stringification', () => {
  it('works in advanced case', () => {
    const ex = ply()
      .apply('diamonds', $('diamonds').filter($('color').is('D')))
      .apply('Count', $('diamonds').count())
      .apply('TotalPrice', $('diamonds').sum('$price'))
      .apply(
        'Cuts',
        $('diamonds')
          .split('$cut', 'Cut')
          .apply('diamonds', $('diamonds').filter($('cut').is('$^Cut')))
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(2)
          .apply(
            'Carats',
            $('diamonds')
              .split($('carat').numberBucket(0.25), 'Carat')
              .apply('diamonds', $('diamonds').filter($('carat').numberBucket(0.25).is('$^Carat')))
              .apply('Count', $('diamonds').count())
              .apply('Price :-)', $('diamonds').sum('$price'))
              .sort('$Count', 'descending')
              .limit(3),
          ),
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

  it('works with fancy ref', () => {
    const ex = $('!T_0');
    expect(ex.toString()).to.equal('${!T_0}');
  });

  it('works with case insensitive refs', () => {
    const ex = i$('x').substr(1, 5);
    expect(ex.toString(2)).to.equal('i$x.substr(1,5)');
  });

  it('works with lookup', () => {
    const ex = $('diamonds').split("$cut.lookup('hello_lookup')", 'CutLookup');
    expect(ex.toString(2)).to.equal(
      '$diamonds.split($cut.lookup(hello_lookup),CutLookup,diamonds)',
    );
  });

  it('works with lookup with fancy name 1', () => {
    const ex = $('diamonds').split("$cut.lookup('99hello')", 'CutLookup');
    expect(ex.toString(2)).to.equal(`$diamonds.split($cut.lookup("99hello"),CutLookup,diamonds)`);
  });

  it('works with lookup with fancy name 2', () => {
    const ex = $('diamonds').split("$cut.lookup('hello=lookup')", 'CutLookup');
    expect(ex.toString(2)).to.equal(
      `$diamonds.split($cut.lookup("hello=lookup"),CutLookup,diamonds)`,
    );
  });

  it('works with timePart', () => {
    const ex = $('time').timePart('DAY_OF_WEEK');
    expect(ex.toString(2)).to.equal('$time.timePart(DAY_OF_WEEK)');
  });

  it('works with timeShift', () => {
    const ex = $('time').timeShift('P1D', 2);
    expect(ex.toString(2)).to.equal('$time.timeShift(P1D,2)');
  });

  it('works with timeShift with timezone', () => {
    const ex = $('time').timeShift('P1D', 2, 'Etc/UTC');
    expect(ex.toString(2)).to.equal(`$time.timeShift(P1D,2,"Etc/UTC")`);
  });

  it('works with timeRange', () => {
    const ex = $('time').timeRange('P1D', 2);
    expect(ex.toString(2)).to.equal('$time.timeRange(P1D,2)');
  });

  it('works with customAggregate', () => {
    const ex = $('x').customAggregate('lol');
    expect(ex.toString(2)).to.equal('$x.customAggregate(lol)');
  });

  it('works with substr', () => {
    const ex = $('x').substr(1, 5);
    expect(ex.toString(2)).to.equal('$x.substr(1,5)');
  });

  it('works with quantile with resolution', () => {
    const ex = $('x').quantile('$hist', 0.98, 'resolution=2000');
    expect(ex.toString(2)).to.equal(`$x.quantile($hist,0.98,"resolution=2000")`);
  });
});
