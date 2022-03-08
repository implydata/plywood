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
let fs = require('fs');
let path = require('path');

let plywood = require('../plywood');
let { Expression, Dataset, $, ply, r } = plywood;

let chronoshift = require('chronoshift');

let rawData = fs.readFileSync(
  path.join(__dirname, '../../resources/wikipedia-sampled.json'),
  'utf-8',
);
let wikiDayData = Dataset.parseJSON(rawData);

wikiDayData.forEach((d, i) => {
  d['time'] = new Date(d['time']);
  d['sometimeLater'] = new Date(d['sometimeLater']);
});

describe('compute native nontrivial data', function () {
  this.timeout(20000);

  let ds = Dataset.fromJS(wikiDayData).hide();

  it('works in simple agg case', () => {
    let ex = ply().apply('Count', '$data.count()').apply('SumAdded', '$data.sum($added)');

    return ex.compute({ data: ds }).then(v => {
      expect(v.toJS().data).to.deep.equal([
        {
          Count: 39244,
          SumAdded: 9385573,
        },
      ]);
    });
  });

  it('works in with a filter == null', () => {
    let ex = $('data').filter('$countryName == null').count();

    return ex.compute({ data: ds }).then(v => {
      expect(v).to.equal(35445);
    });
  });

  it('works in with a filter overlap null', () => {
    let ex = $('data')
      .filter($('countryName').is([null]))
      .count();

    return ex.compute({ data: ds }).then(v => {
      expect(v).to.equal(35445);
    });
  });

  it('works with overlap filter', () => {
    let ex = ply()
      .apply(
        'd1',
        $('data').filter(
          $('time').overlap(new Date('2015-09-12T12:00:00Z'), new Date('2015-09-13T00:00:00Z')),
        ),
      )
      .apply('CountD1', '$d1.count()');

    return ex.compute({ data: ds }).then(v => {
      expect(v.toJS().data).to.deep.equal([
        {
          CountD1: 22732,
        },
      ]);
    });
  });

  it('works in simple split case (small dimension)', () => {
    let ex = $('data')
      .split('$countryName', 'CountryName')
      .apply('Count', '$data.count()')
      .apply('SumAdded', '$data.sum($added)')
      .sort('$SumAdded', 'descending')
      .limit(5);

    return ex.compute({ data: ds }).then(v => {
      expect(v.toJS().data).to.deep.equal([
        {
          Count: 35445,
          CountryName: null,
          SumAdded: 8761516,
        },
        {
          Count: 69,
          CountryName: 'Colombia',
          SumAdded: 60398,
        },
        {
          Count: 194,
          CountryName: 'Russia',
          SumAdded: 50561,
        },
        {
          Count: 528,
          CountryName: 'United States',
          SumAdded: 44433,
        },
        {
          Count: 256,
          CountryName: 'Italy',
          SumAdded: 41073,
        },
      ]);
    });
  });

  it('works in simple split case (large dimension)', () => {
    let ex = $('data')
      .split('$page', 'Page')
      .apply('Count', '$data.count()')
      .apply('SumAdded', '$data.sum($added)')
      .sort('$SumAdded', 'descending')
      .limit(5);

    return ex.compute({ data: ds }).then(v => {
      expect(v.toJS().data).to.deep.equal([
        {
          Count: 1,
          Page: 'User:QuackGuru/Electronic cigarettes 1',
          SumAdded: 199818,
        },
        {
          Count: 1,
          Page: 'Обсуждение участника:पाणिनि/Архив-5',
          SumAdded: 102719,
        },
        {
          Count: 1,
          Page: 'Campeche',
          SumAdded: 94187,
        },
        {
          Count: 1,
          Page: 'Équipe de Pologne de football à la Coupe du monde 1986',
          SumAdded: 92182,
        },
        {
          Count: 1,
          Page: 'Адвокат',
          SumAdded: 89385,
        },
      ]);
    });
  });

  it('works in simple timeBucket case', () => {
    let ex = $('data')
      .split('$time.timeBucket(PT1H, "Asia/Kathmandu")', 'Time') // America/Los_Angeles
      .apply('Count', '$data.count()')
      .sort('$Time', 'ascending')
      .limit(2);

    return ex.compute({ data: ds }).then(v => {
      expect(v.toJS().data).to.deep.equal([
        {
          Count: 556,
          Time: {
            end: new Date('2015-09-12T01:15:00.000Z'),
            start: new Date('2015-09-12T00:15:00.000Z'),
          },
        },
        {
          Count: 1129,
          Time: {
            end: new Date('2015-09-12T02:15:00.000Z'),
            start: new Date('2015-09-12T01:15:00.000Z'),
          },
        },
      ]);
    });
  });

  it('works in heatmap like query', () => {
    let ex = ply()
      .apply('Count', '$data.count()')
      .apply(
        'xs',
        $('data')
          .split('$userChars', 'v')
          .apply('cnt', '$data.count()')
          .sort('$cnt', 'descending')
          .limit(3),
      )
      .apply(
        'ys',
        $('data')
          .split('$channel', 'v')
          .apply('cnt', '$data.count()')
          .sort('$cnt', 'descending')
          .limit(3),
      )
      .apply(
        'cells',
        $('data')
          .filter(
            $('userChars')
              .is($('xs').collect($('v')))
              .and($('channel').in($('ys').collect($('v')))),
          )
          .split({ channel: '$channel', userChars: '$userChars' })
          .apply('cnt', '$data.count()')
          .limit(3 * 3),
      );

    return ex.compute({ data: ds }).then(v => {
      expect(v.toJS()).to.deep.equal({
        attributes: [
          {
            name: 'Count',
            type: 'NUMBER',
          },
          {
            name: 'xs',
            type: 'DATASET',
          },
          {
            name: 'ys',
            type: 'DATASET',
          },
          {
            name: 'cells',
            type: 'DATASET',
          },
        ],
        data: [
          {
            Count: 39244,
            cells: {
              attributes: [
                {
                  name: 'channel',
                  type: 'STRING',
                },
                {
                  name: 'userChars',
                  type: 'STRING',
                },
                {
                  name: 'data',
                  type: 'DATASET',
                },
                {
                  name: 'cnt',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  channel: 'en',
                  cnt: 4947,
                  userChars: 'E',
                },
                {
                  channel: 'en',
                  cnt: 1612,
                  userChars: 'G',
                },
                {
                  channel: 'en',
                  cnt: 2815,
                  userChars: 'L',
                },
                {
                  channel: 'en',
                  cnt: 3736,
                  userChars: 'N',
                },
                {
                  channel: 'en',
                  cnt: 4994,
                  userChars: 'O',
                },
                {
                  channel: 'en',
                  cnt: 3806,
                  userChars: 'R',
                },
                {
                  channel: 'en',
                  cnt: 3006,
                  userChars: 'S',
                },
                {
                  channel: 'en',
                  cnt: 4606,
                  userChars: 'T',
                },
                {
                  channel: 'vi',
                  cnt: 2513,
                  userChars: '!',
                },
              ],
              keys: ['channel', 'userChars'],
            },
            xs: {
              attributes: [
                {
                  name: 'v',
                  type: 'STRING',
                },
                {
                  name: 'data',
                  type: 'DATASET',
                },
                {
                  name: 'cnt',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  cnt: 22311,
                  v: 'A',
                },
                {
                  cnt: 22273,
                  v: 'O',
                },
                {
                  cnt: 21658,
                  v: 'T',
                },
              ],
              keys: ['v'],
            },
            ys: {
              attributes: [
                {
                  name: 'v',
                  type: 'STRING',
                },
                {
                  name: 'data',
                  type: 'DATASET',
                },
                {
                  name: 'cnt',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  cnt: 11549,
                  v: 'en',
                },
                {
                  cnt: 9747,
                  v: 'vi',
                },
                {
                  cnt: 2523,
                  v: 'de',
                },
              ],
              keys: ['v'],
            },
          },
        ],
      });
    });
  });

  it('works in with funny aggregates', () => {
    let ex = $('data')
      .split('$countryName', 'CountryName')
      .apply('LabelCountry', '"[" ++ $CountryName ++ "]"')
      .apply('Count', '$data.count()')
      .apply('CountLT1000', '$Count < 1000')
      .apply('CountGT1000', '$Count > 1000')
      .apply('CountLTE397', '$Count <= 397')
      .apply('CountGTE397', '$Count >= 397')
      .apply('SumAdded', '$data.sum($added)')
      .apply('NegSumAdded', '-$SumAdded')
      .apply('DistinctCity', '$data.countDistinct($cityName)')
      .sort('$SumAdded', 'descending')
      .limit(5);

    return ex.compute({ data: ds }).then(v => {
      expect(v.toJS().data).to.deep.equal([
        {
          Count: 35445,
          CountGT1000: true,
          CountGTE397: true,
          CountLT1000: false,
          CountLTE397: false,
          CountryName: null,
          DistinctCity: 1,
          LabelCountry: null,
          NegSumAdded: -8761516,
          SumAdded: 8761516,
        },
        {
          Count: 69,
          CountGT1000: false,
          CountGTE397: false,
          CountLT1000: true,
          CountLTE397: true,
          CountryName: 'Colombia',
          DistinctCity: 8,
          LabelCountry: '[Colombia]',
          NegSumAdded: -60398,
          SumAdded: 60398,
        },
        {
          Count: 194,
          CountGT1000: false,
          CountGTE397: false,
          CountLT1000: true,
          CountLTE397: true,
          CountryName: 'Russia',
          DistinctCity: 36,
          LabelCountry: '[Russia]',
          NegSumAdded: -50561,
          SumAdded: 50561,
        },
        {
          Count: 528,
          CountGT1000: false,
          CountGTE397: true,
          CountLT1000: true,
          CountLTE397: false,
          CountryName: 'United States',
          DistinctCity: 252,
          LabelCountry: '[United States]',
          NegSumAdded: -44433,
          SumAdded: 44433,
        },
        {
          Count: 256,
          CountGT1000: false,
          CountGTE397: false,
          CountLT1000: true,
          CountLTE397: true,
          CountryName: 'Italy',
          DistinctCity: 78,
          LabelCountry: '[Italy]',
          NegSumAdded: -41073,
          SumAdded: 41073,
        },
      ]);
    });
  });

  it('works with join timeBucket case', () => {
    let ex = $('data')
      .split('$time.timeBucket(PT6H)', 'Time', 'd1')
      .join($('data').split('$time.timeShift(PT6H, 1).timeBucket(PT6H)', 'Time', 'd2'))
      .apply('CountD1', '$d1.count()')
      .apply('CountD2', '$d2.count()')
      .sort('$Time', 'ascending');

    return ex.compute({ data: ds }).then(v => {
      expect(v.toJS().data).to.deep.equal([
        {
          CountD1: 5413,
          CountD2: 0,
          Time: {
            end: new Date('2015-09-12T06:00:00.000Z'),
            start: new Date('2015-09-12T00:00:00.000Z'),
          },
        },
        {
          CountD1: 11099,
          CountD2: 5413,
          Time: {
            end: new Date('2015-09-12T12:00:00.000Z'),
            start: new Date('2015-09-12T06:00:00.000Z'),
          },
        },
        {
          CountD1: 11879,
          CountD2: 11099,
          Time: {
            end: new Date('2015-09-12T18:00:00.000Z'),
            start: new Date('2015-09-12T12:00:00.000Z'),
          },
        },
        {
          CountD1: 10853,
          CountD2: 11879,
          Time: {
            end: new Date('2015-09-13T00:00:00.000Z'),
            start: new Date('2015-09-12T18:00:00.000Z'),
          },
        },
      ]);
    });
  });

  it('works with join timeBucket case with filters', () => {
    let ex = ply()
      .apply(
        'd1',
        $('data').filter(
          $('time').overlap(new Date('2015-09-12T12:00:00Z'), new Date('2015-09-13T00:00:00Z')),
        ),
      )
      .apply(
        'd2',
        $('data').filter(
          $('time').overlap(new Date('2015-09-12T00:00:00Z'), new Date('2015-09-12T12:00:00Z')),
        ),
      )
      .apply('CountD1', '$d1.count()')
      .apply('CountD2', '$d2.count()')
      .apply(
        'Sub',
        $('d1')
          .split('$time.timeBucket(PT3H)', 'Time')
          .join($('d2').split('$time.timeShift(PT12H, 1).timeBucket(PT3H)', 'Time'))
          .apply('CountD1', '$d1.count()')
          .apply('CountD2', '$d2.count()')
          .sort('$Time', 'ascending'),
      );

    return ex.compute({ data: ds }).then(v => {
      expect(v.toJS().data).to.deep.equal([
        {
          CountD1: 22732,
          CountD2: 16512,
          Sub: {
            attributes: [
              {
                name: 'Time',
                type: 'TIME_RANGE',
              },
              {
                name: 'd1',
                type: 'DATASET',
              },
              {
                name: 'd2',
                type: 'DATASET',
              },
              {
                name: 'CountD1',
                type: 'NUMBER',
              },
              {
                name: 'CountD2',
                type: 'NUMBER',
              },
            ],
            data: [
              {
                CountD1: 5655,
                CountD2: 2514,
                Time: {
                  end: new Date('2015-09-12T15:00:00.000Z'),
                  start: new Date('2015-09-12T12:00:00.000Z'),
                },
              },
              {
                CountD1: 6224,
                CountD2: 2899,
                Time: {
                  end: new Date('2015-09-12T18:00:00.000Z'),
                  start: new Date('2015-09-12T15:00:00.000Z'),
                },
              },
              {
                CountD1: 6015,
                CountD2: 5979,
                Time: {
                  end: new Date('2015-09-12T21:00:00.000Z'),
                  start: new Date('2015-09-12T18:00:00.000Z'),
                },
              },
              {
                CountD1: 4838,
                CountD2: 5120,
                Time: {
                  end: new Date('2015-09-13T00:00:00.000Z'),
                  start: new Date('2015-09-12T21:00:00.000Z'),
                },
              },
            ],
            keys: ['Time'],
          },
        },
      ]);
    });
  });
});
