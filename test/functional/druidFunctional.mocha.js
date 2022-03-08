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
const { Duration } = require('chronoshift');
const axios = require('axios');
let { sane } = require('../utils');

let { druidRequesterFactory } = require('plywood-druid-requester');

let plywood = require('../plywood');
let {
  External,
  DruidExternal,
  TimeRange,
  $,
  i$,
  ply,
  r,
  basicExecutorFactory,
  verboseRequesterFactory,
  Expression,
} = plywood;

let info = require('../info');

let druidRequester = druidRequesterFactory({
  host: info.druidHost,
});

// druidRequester = verboseRequesterFactory({
//   requester: druidRequester,
// });

describe('Druid Functional', function () {
  this.timeout(10000);

  let wikiAttributes = [
    {
      name: 'time',
      nativeType: '__time',
      range: {
        bounds: '[]',
        end: new Date('2015-09-12T23:59:00.000Z'),
        start: new Date('2015-09-12T00:46:00.000Z'),
      },
      type: 'TIME',
    },
    {
      maker: {
        expression: {
          name: 'added',
          op: 'ref',
        },
        op: 'sum',
      },
      name: 'added',
      nativeType: 'LONG',
      type: 'NUMBER',
      unsplitable: true,
    },
    {
      cardinality: 52,
      name: 'channel',
      nativeType: 'STRING',
      range: {
        bounds: '[]',
        end: 'zh',
        start: 'ar',
      },
      type: 'STRING',
    },
    {
      cardinality: 3719,
      name: 'cityName',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      cardinality: 138731,
      name: 'comment',
      nativeType: 'STRING',
      range: {
        bounds: '[]',
        end: "��творена сторінка: {{Пишу}} '''Jaguar Mark VIII''' [[люкс-автомобіль]] [[Велика Британія|британської]] компанії [[Jaguar]] 195...",
        start: '!',
      },
      type: 'STRING',
    },
    {
      name: 'commentLength',
      nativeType: 'LONG',
      type: 'NUMBER',
    },
    {
      cardinality: 255,
      name: 'commentLengthStr',
      nativeType: 'STRING',
      range: {
        bounds: '[]',
        end: '99',
        start: '1',
      },
      type: 'STRING',
    },
    {
      maker: {
        op: 'count',
      },
      name: 'count',
      nativeType: 'LONG',
      type: 'NUMBER',
      unsplitable: true,
    },
    {
      cardinality: 157,
      name: 'countryIsoCode',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      cardinality: 157,
      name: 'countryName',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      maker: {
        expression: {
          name: 'deleted',
          op: 'ref',
        },
        op: 'sum',
      },
      name: 'deleted',
      nativeType: 'LONG',
      type: 'NUMBER',
      unsplitable: true,
    },
    {
      maker: {
        expression: {
          name: 'delta',
          op: 'ref',
        },
        op: 'sum',
      },
      name: 'delta',
      nativeType: 'LONG',
      type: 'NUMBER',
      unsplitable: true,
    },
    {
      name: 'deltaBucket100',
      nativeType: 'FLOAT',
      type: 'NUMBER',
    },
    {
      maker: {
        expression: {
          name: 'deltaByTen',
          op: 'ref',
        },
        op: 'sum',
      },
      name: 'deltaByTen',
      nativeType: 'DOUBLE',
      type: 'NUMBER',
      unsplitable: true,
    },
    {
      name: 'delta_hist',
      nativeType: 'approximateHistogram',
      type: 'NULL',
      unsplitable: true,
    },
    {
      name: 'delta_quantilesDoublesSketch',
      nativeType: 'quantilesDoublesSketch',
      type: 'NULL',
      unsplitable: true,
    },
    {
      cardinality: 8086,
      name: 'geohash',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'isAnonymous',
      type: 'BOOLEAN',
    },
    {
      name: 'isMinor',
      type: 'BOOLEAN',
    },
    {
      name: 'isNew',
      type: 'BOOLEAN',
    },
    {
      name: 'isRobot',
      type: 'BOOLEAN',
    },
    {
      name: 'isUnpatrolled',
      type: 'BOOLEAN',
    },
    {
      maker: {
        expression: {
          name: 'max_delta',
          op: 'ref',
        },
        op: 'max',
      },
      name: 'max_delta',
      nativeType: 'LONG',
      type: 'NUMBER',
      unsplitable: true,
    },
    {
      cardinality: 167,
      name: 'metroCode',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      maker: {
        expression: {
          name: 'min_delta',
          op: 'ref',
        },
        op: 'min',
      },
      name: 'min_delta',
      nativeType: 'LONG',
      type: 'NUMBER',
      unsplitable: true,
    },
    {
      cardinality: 425,
      name: 'namespace',
      nativeType: 'STRING',
      range: {
        bounds: '[]',
        end: '��нкубатор',
        start: '2',
      },
      type: 'STRING',
    },
    {
      cardinality: 279915,
      name: 'page',
      nativeType: 'STRING',
      range: {
        bounds: '[]',
        end: '���周列国志',
        start: '!T.O.O.H.!',
      },
      type: 'STRING',
    },
    {
      name: 'page_unique',
      nativeType: 'hyperUnique',
      type: 'NULL',
      unsplitable: true,
    },
    {
      cardinality: 671,
      name: 'regionIsoCode',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      cardinality: 1069,
      name: 'regionName',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'sometimeLater',
      type: 'TIME',
    },
    {
      name: 'sometimeLaterMs',
      nativeType: 'LONG',
      type: 'NUMBER',
    },
    {
      cardinality: 38240,
      name: 'user',
      nativeType: 'STRING',
      range: {
        bounds: '[]',
        end: '���バコはマーダー',
        start: '! Bikkit !',
      },
      type: 'STRING',
    },
    {
      cardinality: 1403,
      name: 'userChars',
      nativeType: 'STRING',
      range: {
        bounds: '[]',
        end: '���',
        start: ' ',
      },
      type: 'SET/STRING',
    },
    {
      name: 'user_hll',
      nativeType: 'HLLSketch',
      type: 'NULL',
      unsplitable: true,
    },
    {
      name: 'user_theta',
      nativeType: 'thetaSketch',
      type: 'NULL',
      unsplitable: true,
    },
    {
      name: 'user_unique',
      nativeType: 'hyperUnique',
      type: 'NULL',
      unsplitable: true,
    },
  ];

  const customTransforms = {
    sliceLastChar: {
      extractionFn: {
        type: 'javascript',
        function: 'function(x) { return x.slice(-1) }',
      },
    },
    getLastChar: {
      extractionFn: {
        type: 'javascript',
        function: 'function(x) { return x.charAt(x.length - 1) }',
      },
    },
    timesTwo: {
      extractionFn: {
        type: 'javascript',
        function: 'function(x) { return x * 2 }',
      },
    },
    concatWithConcat: {
      extractionFn: {
        type: 'javascript',
        function: "function(x) { return String(x).concat('concat') }",
      },
    },
  };

  describe('source list', () => {
    it('does a source list', () => {
      return DruidExternal.getSourceList(druidRequester).then(sources => {
        expect(sources).to.deep.equal(['wikipedia', 'wikipedia-compact']);
      });
    });
  });

  describe('defined attributes in datasource', () => {
    let wiki = External.fromJS(
      {
        engine: 'druid',
        source: 'wikipedia',
        timeAttribute: 'time',
        context: info.druidContext,
        attributes: wikiAttributes,
        customTransforms,
        filter: $('time').overlap(
          new Date('2015-09-12T00:00:00Z'),
          new Date('2015-09-13T00:00:00Z'),
        ),
        version: info.druidVersion,
        allowSelectQueries: true,
      },
      druidRequester,
    );

    let wikiCompact = External.fromJS(
      {
        engine: 'druid',
        source: 'wikipedia-compact',
        timeAttribute: 'time',
        context: info.druidContext,
        attributes: [
          {
            name: 'time',
            type: 'TIME',
            maker: { action: 'timeFloor', duration: 'PT1H', timezone: 'Etc/UTC' },
          },
          { name: 'channel', type: 'STRING' },
          { name: 'isNew', type: 'BOOLEAN' },
          { name: 'isAnonymous', type: 'BOOLEAN' },
          { name: 'commentLength', type: 'NUMBER' },
          { name: 'metroCode', type: 'STRING' },
          { name: 'cityName', type: 'STRING' },

          { name: 'count', type: 'NUMBER', unsplitable: true },
          { name: 'delta', type: 'NUMBER', unsplitable: true },
          { name: 'added', type: 'NUMBER', unsplitable: true },
          { name: 'deleted', type: 'NUMBER', unsplitable: true },
          { name: 'page_unique', type: 'NULL', nativeType: 'hyperUnique', unsplitable: true },
        ],
        filter: $('time').overlap(
          TimeRange.fromJS({
            start: new Date('2015-09-12T00:00:00Z'),
            end: new Date('2015-09-13T00:00:00Z'),
          }),
        ),
        customTransforms,
        concealBuckets: true,
        version: info.druidVersion,
        allowSelectQueries: true,
      },
      druidRequester,
    );

    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: wiki.addDelegate(wikiCompact),
      },
    });

    it('works basic case to CSV', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('channel').is('en')))
        .apply(
          'Cities',
          $('wiki')
            .split('$cityName', 'City')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$TotalAdded', 'descending')
            .limit(2),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toCSV({ lineBreak: '\n' })).to.deep.equal(sane`
            City,TotalAdded
            null,31529720
            Mineola,50836
          `);
      });
    });

    it('works with basic error (non-existent lookup)', () => {
      let ex = $('wiki').split('$cityName.lookup(blah)', 'B');

      return basicExecutor(ex)
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch(e => {
          expect(e.message).to.contain('Lookup [blah] not found');
        });
    });

    it('works basic total', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('channel').is('en')))
        .apply('Count', '$wiki.count()');

      return basicExecutor(ex).then(result => {
        expect(result.toJS()).to.deep.equal({
          attributes: [
            {
              name: 'wiki',
              type: 'DATASET',
            },
            {
              name: 'Count',
              type: 'NUMBER',
            },
          ],
          data: [
            {
              Count: 35485,
            },
          ],
        });

        expect(result.flatten().toJS()).to.deep.equal({
          attributes: [
            {
              name: 'Count',
              type: 'NUMBER',
            },
          ],
          data: [
            {
              Count: 35485,
            },
          ],
        });
      });
    });

    it('works with max time 1', () => {
      let ex = ply().apply('max(time)', '$wiki.max($time)');

      return basicExecutor(ex).then(result => {
        expect(result.toJS()).to.deep.equal({
          attributes: [
            {
              name: 'max(time)',
              type: 'TIME',
            },
          ],
          data: [
            {
              'max(time)': new Date('2015-09-12T23:00:00.000Z'),
            },
          ],
        });
      });
    });

    it('works with max time 2', () => {
      let ex = $('wiki').max('$time');

      return basicExecutor(ex).then(result => {
        expect(result).to.deep.equal(new Date('2015-09-12T23:00:00.000Z'));
      });
    });

    it('aggregate and splits plus select work with ordering last split first', () => {
      let ex = $('wiki')
        .split({ isNew: '$isNew', isRobot: '$isRobot' })
        .apply('Count', $('wiki').sum('$count'))
        .apply('Page', $('wiki').split('$page', 'Page').limit(3))
        .select('Page', 'Count', 'isRobot', 'isNew')
        .limit(1);

      return basicExecutor(ex).then(result => {
        expect(result.attributes.map(c => c.name)).to.deep.equal([
          'Page',
          'Count',
          'isRobot',
          'isNew',
        ]);
      });
    });

    it('aggregate and splits plus select work with ordering 2', () => {
      let ex = $('wiki')
        .split({ isNew: '$isNew', isRobot: '$isRobot' })
        .apply('Count', $('wiki').sum('$count'))
        .apply('Page', $('wiki').split('$page', 'Page').limit(3))
        .select('isRobot', 'Page', 'isNew', 'Count')
        .limit(1);

      return basicExecutor(ex).then(result => {
        expect(result.attributes.map(c => c.name)).to.deep.equal([
          'isRobot',
          'Page',
          'isNew',
          'Count',
        ]);
      });
    });

    it('aggregate and splits plus select work with ordering, aggregate first', () => {
      let ex = $('wiki')
        .split({ isNew: '$isNew', isRobot: '$isRobot' })
        .apply('Count', $('wiki').sum('$count'))
        .apply('Page', $('wiki').split('$page', 'Page').limit(3))
        .select('Count', 'isRobot', 'Page', 'isNew')
        .limit(1);

      return basicExecutor(ex).then(result => {
        expect(result.attributes.map(c => c.name)).to.deep.equal([
          'Count',
          'isRobot',
          'Page',
          'isNew',
        ]);
      });
    });

    it('works timePart case', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('channel').is('en')))
        .apply(
          'HoursOfDay',
          $('wiki')
            .split("$time.timePart(HOUR_OF_DAY, 'Etc/UTC')", 'HourOfDay')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$TotalAdded', 'descending')
            .limit(3),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            HoursOfDay: {
              attributes: [
                {
                  name: 'HourOfDay',
                  type: 'NUMBER',
                },
                {
                  name: 'TotalAdded',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  HourOfDay: 2,
                  TotalAdded: 3045966,
                },
                {
                  HourOfDay: 17,
                  TotalAdded: 1883290,
                },
                {
                  HourOfDay: 3,
                  TotalAdded: 1825954,
                },
              ],
              keys: ['HourOfDay'],
            },
          },
        ]);
      });
    });

    it('works with quarter call case', () => {
      let ex = $('wiki')
        .filter($('channel').is('en'))
        .split("$time.timePart(QUARTER, 'Etc/UTC')", 'Quarter');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Quarter: 3,
          },
        ]);
      });
    });

    it('works with time floor + timezone', () => {
      let ex = $('wiki')
        .split({ t: $('time').timeFloor('P1D', 'Europe/Paris'), robot: '$isRobot' })
        .apply('cnt', '$wiki.sum($count)')
        .sort('$cnt', 'descending')
        .limit(10);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            cnt: 218234,
            robot: false,
            t: new Date('2015-09-11T22:00:00.000Z'),
          },
          {
            cnt: 143489,
            robot: true,
            t: new Date('2015-09-11T22:00:00.000Z'),
          },
          {
            cnt: 19328,
            robot: false,
            t: new Date('2015-09-12T22:00:00.000Z'),
          },
          {
            cnt: 11392,
            robot: true,
            t: new Date('2015-09-12T22:00:00.000Z'),
          },
        ]);
      });
    });

    it('works with yearly call case long', () => {
      let ex = $('wiki').split(i$('time').timeFloor('P3M'), 'tqr___time_ok');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            tqr___time_ok: new Date('2015-07-01T00:00:00.000Z'),
          },
        ]);
      });
    });

    it('works in advanced case', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('channel').is('en')))
        .apply('Count', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply(
          'Pages',
          $('wiki')
            .split('$page', 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(2)
            .apply(
              'Time',
              $('wiki')
                .split($('time').timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
                .apply('TotalAdded', '$wiki.sum($added)')
                .sort('$TotalAdded', 'descending')
                .limit(3),
            ),
        )
        .apply(
          'PagesHaving',
          $('wiki')
            .split('$page', 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .filter($('Count').lessThan(300))
            .limit(4),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 114711,
            Pages: {
              attributes: [
                {
                  name: 'Page',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
                {
                  name: 'Time',
                  type: 'DATASET',
                },
              ],
              data: [
                {
                  Count: 255,
                  Page: 'User:Cyde/List of candidates for speedy deletion/Subpage',
                  Time: {
                    attributes: [
                      {
                        name: 'Timestamp',
                        type: 'TIME_RANGE',
                      },
                      {
                        name: 'TotalAdded',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T13:00:00.000Z'),
                          start: new Date('2015-09-12T12:00:00.000Z'),
                        },
                        TotalAdded: 9231,
                      },
                      {
                        Timestamp: {
                          end: new Date('2015-09-13T00:00:00.000Z'),
                          start: new Date('2015-09-12T23:00:00.000Z'),
                        },
                        TotalAdded: 3956,
                      },
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T02:00:00.000Z'),
                          start: new Date('2015-09-12T01:00:00.000Z'),
                        },
                        TotalAdded: 3363,
                      },
                    ],
                    keys: ['Timestamp'],
                  },
                },
                {
                  Count: 241,
                  Page: 'Jeremy Corbyn',
                  Time: {
                    attributes: [
                      {
                        name: 'Timestamp',
                        type: 'TIME_RANGE',
                      },
                      {
                        name: 'TotalAdded',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T16:00:00.000Z'),
                          start: new Date('2015-09-12T15:00:00.000Z'),
                        },
                        TotalAdded: 28193,
                      },
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T19:00:00.000Z'),
                          start: new Date('2015-09-12T18:00:00.000Z'),
                        },
                        TotalAdded: 2419,
                      },
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T11:00:00.000Z'),
                          start: new Date('2015-09-12T10:00:00.000Z'),
                        },
                        TotalAdded: 2041,
                      },
                    ],
                    keys: ['Timestamp'],
                  },
                },
              ],
              keys: ['Page'],
            },
            PagesHaving: {
              attributes: [
                {
                  name: 'Page',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Count: 255,
                  Page: 'User:Cyde/List of candidates for speedy deletion/Subpage',
                },
                {
                  Count: 241,
                  Page: 'Jeremy Corbyn',
                },
                {
                  Count: 228,
                  Page: "Wikipedia:Administrators' noticeboard/Incidents",
                },
                {
                  Count: 146,
                  Page: 'Wikipedia:Administrator intervention against vandalism',
                },
              ],
              keys: ['Page'],
            },
            TotalAdded: 32553107,
          },
        ]);
      });
    });

    it('works in advanced case (with trim)', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('channel').is('en')))
        .apply('Count', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply(
          'Pages',
          $('wiki')
            .split('$page', 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(100)
            .apply(
              'Time',
              $('wiki')
                .split('$user', 'User')
                .apply('TotalAdded', '$wiki.sum($added)')
                .sort('$TotalAdded', 'descending')
                .limit(24),
            ),
        );

      return basicExecutor(ex, { maxRows: 5 }).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 114711,
            Pages: {
              attributes: [
                {
                  name: 'Page',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
                {
                  name: 'Time',
                  type: 'DATASET',
                },
              ],
              data: [
                {
                  Count: 255,
                  Page: 'User:Cyde/List of candidates for speedy deletion/Subpage',
                  Time: {
                    attributes: [
                      {
                        name: 'User',
                        type: 'STRING',
                      },
                      {
                        name: 'TotalAdded',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        TotalAdded: 35445,
                        User: 'Cydebot',
                      },
                    ],
                    keys: ['User'],
                  },
                },
                {
                  Count: 241,
                  Page: 'Jeremy Corbyn',
                  Time: {
                    attributes: [
                      {
                        name: 'User',
                        type: 'STRING',
                      },
                      {
                        name: 'TotalAdded',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        TotalAdded: 30035,
                        User: 'Hazhk',
                      },
                    ],
                    keys: ['User'],
                  },
                },
              ],
              keys: ['Page'],
            },
            TotalAdded: 32553107,
          },
        ]);
      });
    });

    it('works with case transform in filter split and apply', () => {
      let ex = $('wiki')
        .filter($('channel').transformCase('upperCase').is('EN'))
        .split($('page').transformCase('lowerCase'), 'page')
        .apply('SumIndexA', $('wiki').sum($('channel').transformCase('upperCase').indexOf('A')))
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            SumIndexA: -1,
            page: '!t.o.o.h.!',
          },
          {
            SumIndexA: -1,
            page: "'ajde jano",
          },
          {
            SumIndexA: -1,
            page: "'asir region",
          },
          {
            SumIndexA: -1,
            page: "'asta bowen",
          },
          {
            SumIndexA: -1,
            page: "'atika wahbi al-khazraji",
          },
        ]);
      });
    });

    it('works with custom transform in filter and split', () => {
      let ex = $('wiki')
        .filter($('page').customTransform('sliceLastChar').is('z'))
        .split($('page').customTransform('getLastChar'), 'lastChar')
        .apply('Temp', '$wiki.count()') // ToDo: Temp fix
        .limit(8);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Temp: 1984,
            lastChar: 'z',
          },
        ]);
      });
    });

    it('works with custom transform in filter and split for numeric dimension', () => {
      let ex = $('wiki')
        .filter($('commentLength').customTransform('concatWithConcat', 'STRING').is("'100concat'"))
        .split($('commentLength').customTransform('timesTwo', 'STRING'), 'Times Two')
        .apply('Temp', '$wiki.count()') // ToDo: Temp fix
        .limit(8);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            'Temp': 275,
            'Times Two': '200',
          },
        ]);
      });
    });

    it('works with uniques', () => {
      let ex = ply()
        .apply('UniqueIsRobot', $('wiki').countDistinct('$isRobot'))
        .apply('UniqueUserChars', $('wiki').countDistinct('$userChars'))
        .apply('UniquePages1', $('wiki').countDistinct('$page'))
        .apply('UniquePages2', $('wiki').countDistinct('$page_unique'))
        .apply('UniqueUsers1', $('wiki').countDistinct('$user'))
        .apply('UniqueUsers2', $('wiki').countDistinct('$user_unique'))
        .apply('UniqueUsers3', $('wiki').countDistinct('$user_theta'))
        .apply('UniqueUsers4', $('wiki').countDistinct('$user_hll'))
        .apply('Diff_Users_1_2', '$UniqueUsers1 - $UniqueUsers2')
        .apply('Diff_Users_2_3', '$UniqueUsers2 - $UniqueUsers3')
        .apply('Diff_Users_1_3', '$UniqueUsers1 - $UniqueUsers3')
        .apply('Diff_Users_3_4', '$UniqueUsers3 - $UniqueUsers4');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Diff_Users_1_2: 1555,
            Diff_Users_1_3: 1102.7885680156833,
            Diff_Users_2_3: -452.21143198431673,
            Diff_Users_3_4: -39.78856801568327,
            UniqueIsRobot: 2,
            UniquePages1: 279107,
            UniquePages2: 281588,
            UniqueUserChars: 1376,
            UniqueUsers1: 39268,
            UniqueUsers2: 37713,
            UniqueUsers3: 38165.21143198432,
            UniqueUsers4: 38205,
          },
        ]);
      });
    });

    it('works with filtered unique (in expression)', () => {
      let ex = ply()
        .apply('UniquePagesEn', $('wiki').filter('$channel == en').countDistinct('$page'))
        .apply('UniquePagesEnOver2', '$UniquePagesEn / 2');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            UniquePagesEn: 63850,
            UniquePagesEnOver2: 31925,
          },
        ]);
      });
    });

    it('works with filtered uniques', () => {
      let ex = ply()
        .apply('UniquePagesEn', $('wiki').filter('$channel == en').countDistinct('$page'))
        .apply('UniquePagesEs', $('wiki').filter('$channel == es').countDistinct('$page_unique'))
        .apply('UniquePagesChannelDiff', '$UniquePagesEn - $UniquePagesEs');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            UniquePagesEn: 63850,
            UniquePagesEs: 6870,
            UniquePagesChannelDiff: 56980,
          },
        ]);
      });
    });

    it('works with multiple columns', () => {
      let ex = $('wiki').countDistinct("$channel ++ 'lol' ++ $user");

      return basicExecutor(ex).then(result => {
        expect(result).to.deep.equal(40082);
      });
    });

    it('works with no applies in dimensions split dataset', () => {
      let ex = ply().apply(
        'Channels',
        $('wiki')
          .split('$channel', 'Channel')
          .sort('$Channel', 'descending')
          .limit(2)
          .apply(
            'Users',
            $('wiki')
              .split('$user', 'User')
              .apply('Count', $('wiki').sum('$count'))
              .sort('$Count', 'descending')
              .limit(2),
          ),
      );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channels: {
              attributes: [
                {
                  name: 'Channel',
                  type: 'STRING',
                },
                {
                  name: 'Users',
                  type: 'DATASET',
                },
              ],
              data: [
                {
                  Channel: 'zh',
                  Users: {
                    attributes: [
                      {
                        name: 'User',
                        type: 'STRING',
                      },
                      {
                        name: 'Count',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Count: 3698,
                        User: 'Antigng-bot',
                      },
                      {
                        Count: 503,
                        User: '和平-bot',
                      },
                    ],
                    keys: ['User'],
                  },
                },
                {
                  Channel: 'war',
                  Users: {
                    attributes: [
                      {
                        name: 'User',
                        type: 'STRING',
                      },
                      {
                        name: 'Count',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Count: 4,
                        User: 'JinJian',
                      },
                      {
                        Count: 3,
                        User: 'Xqbot',
                      },
                    ],
                    keys: ['User'],
                  },
                },
              ],
              keys: ['Channel'],
            },
          },
        ]);
      });
    });

    it('works with absolute', () => {
      let ex = ply()
        .apply('Count', $('wiki').filter($('channel').is('en')).sum('$count'))
        .apply('Negate', $('Count').negate())
        .apply('Abs', $('Count').negate().absolute().negate().absolute());

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Abs: 114711,
            Count: 114711,
            Negate: -114711,
          },
        ]);
      });
    });

    it('works with fancy lookup filter', () => {
      let ex = ply()
        .apply(
          'wiki',
          $('wiki').filter(
            $('channel')
              .lookup('channel-lookup')
              .fallback('"???"')
              .concat(r(' ('), '$channel', r(')'))
              .in(['English (en)', 'German (de)']),
          ),
        )
        .apply('Count', '$wiki.sum($count)');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 114711,
          },
        ]);
      });
    });

    it('works with split on a SET/STRING dimension', () => {
      let ex = ply().apply(
        'UserChars',
        $('wiki')
          .split('$userChars', 'UserChar')
          .apply('Count', $('wiki').sum('$count'))
          .sort('$Count', 'descending')
          .limit(4),
      );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            UserChars: {
              attributes: [
                {
                  name: 'UserChar',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Count: 223134,
                  UserChar: 'O',
                },
                {
                  Count: 222676,
                  UserChar: 'A',
                },
                {
                  Count: 216186,
                  UserChar: 'T',
                },
                {
                  Count: 176986,
                  UserChar: 'B',
                },
              ],
              keys: ['UserChar'],
            },
          },
        ]);
      });
    });

    it('works with split on a SET/STRING dimension + time + filter', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter('$userChars.is("O")'))
        .apply(
          'UserChars',
          $('wiki')
            .split('$userChars', 'UserChar')
            .apply('Count', $('wiki').sum('$count'))
            .sort('$Count', 'descending')
            .limit(2)
            .apply(
              'Split',
              $('wiki')
                .split('$time.timeBucket(PT12H)', 'T')
                .apply('Count', $('wiki').sum('$count'))
                .sort('$T', 'ascending'),
            ),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            UserChars: {
              attributes: [
                {
                  name: 'UserChar',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
                {
                  name: 'Split',
                  type: 'DATASET',
                },
              ],
              data: [
                {
                  Count: 223134,
                  Split: {
                    attributes: [
                      {
                        name: 'T',
                        type: 'TIME_RANGE',
                      },
                      {
                        name: 'Count',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Count: 93346,
                        T: {
                          end: new Date('2015-09-12T12:00:00.000Z'),
                          start: new Date('2015-09-12T00:00:00.000Z'),
                        },
                      },
                      {
                        Count: 129788,
                        T: {
                          end: new Date('2015-09-13T00:00:00.000Z'),
                          start: new Date('2015-09-12T12:00:00.000Z'),
                        },
                      },
                    ],
                    keys: ['T'],
                  },
                  UserChar: 'O',
                },
                {
                  Count: 173377,
                  Split: {
                    attributes: [
                      {
                        name: 'T',
                        type: 'TIME_RANGE',
                      },
                      {
                        name: 'Count',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Count: 74042,
                        T: {
                          end: new Date('2015-09-12T12:00:00.000Z'),
                          start: new Date('2015-09-12T00:00:00.000Z'),
                        },
                      },
                      {
                        Count: 99335,
                        T: {
                          end: new Date('2015-09-13T00:00:00.000Z'),
                          start: new Date('2015-09-12T12:00:00.000Z'),
                        },
                      },
                    ],
                    keys: ['T'],
                  },
                  UserChar: 'T',
                },
              ],
              keys: ['UserChar'],
            },
          },
        ]);
      });
    });

    it('works with all kinds of cool aggregates on totals level', () => {
      let ex = ply()
        .apply('NumPages', $('wiki').countDistinct('$page'))
        .apply('NumEnPages', $('wiki').filter($('channel').is('en')).countDistinct('$page'))
        .apply('ChannelAdded', $('wiki').sum('$added'))
        .apply('ChannelENAdded', $('wiki').filter($('channel').is('en')).sum('$added'))
        .apply('ChannelENishAdded', $('wiki').filter($('channel').contains('en')).sum('$added'))
        .apply('Count', $('wiki').sum('$count'))
        .apply('CountSquareRoot', $('wiki').sum('$count').power(0.5))
        .apply('CountSquared', $('wiki').sum('$count').power(2))
        .apply('One', $('wiki').sum('$count').power(0))
        .apply('AddedByDeleted', $('wiki').sum('$added').divide($('wiki').sum('$deleted')))
        .apply('Delta95th', $('wiki').quantile('$delta_hist', 0.95))
        .apply('Delta99thX2', $('wiki').quantile('$delta_hist', 0.99).multiply(2))
        .apply('Delta98thEn', $('wiki').filter($('channel').is('en')).quantile('$delta_hist', 0.98))
        .apply(
          'Delta98thDe',
          $('wiki').filter($('channel').is('de')).quantile('$delta_hist', 0.98),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            AddedByDeleted: 24.909643797343193,
            ChannelAdded: 97393743,
            ChannelENAdded: 32553107,
            ChannelENishAdded: 32553107,
            Count: 392443,
            CountSquareRoot: 626.4527117029664,
            CountSquared: 154011508249,
            NumEnPages: 63850,
            NumPages: 279107,
            One: 1,
            Delta95th: 161.95517,
            Delta99thX2: 328.9096984863281,
            Delta98thEn: 176.93568,
            Delta98thDe: 112.789635,
          },
        ]);
      });
    });

    it('works with all kinds of cool aggregates on split level', () => {
      let ex = $('wiki')
        .split('$isNew', 'isNew')
        .apply('NumPages', $('wiki').countDistinct('$page'))
        .apply('NumEnPages', $('wiki').filter($('channel').is('en')).countDistinct('$page'))
        .apply('ChannelAdded', $('wiki').sum('$added'))
        .apply('ChannelENAdded', $('wiki').filter($('channel').is('en')).sum('$added'))
        .apply('ChannelENishAdded', $('wiki').filter($('channel').contains('en')).sum('$added'))
        .apply('Count', $('wiki').sum('$count'))
        .apply('CountSquareRoot', $('wiki').sum('$count').power(0.5))
        .apply('CountSquared', $('wiki').sum('$count').power(2))
        .apply('One', $('wiki').sum('$count').power(0))
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            ChannelAdded: 53750772,
            ChannelENAdded: 23136956,
            ChannelENishAdded: 23136956,
            Count: 368841,
            CountSquareRoot: 607.3228136666694,
            CountSquared: 136043683281,
            NumEnPages: 57395,
            NumPages: 262440,
            One: 1,
            isNew: false,
          },
          {
            ChannelAdded: 43642971,
            ChannelENAdded: 9416151,
            ChannelENishAdded: 9416151,
            Count: 23602,
            CountSquareRoot: 153.62942426501507,
            CountSquared: 557054404,
            NumEnPages: 8166,
            NumPages: 22270,
            One: 1,
            isNew: true,
          },
        ]);
      });
    });

    it('works with no applies in time split dataset (+rawQueries monitoring)', () => {
      let ex = ply().apply(
        'ByHour',
        $('wiki')
          .split($('time').timeBucket('PT12H', 'Etc/UTC'), 'TimeByHour')
          .sort('$TimeByHour', 'ascending')
          .apply(
            'Users',
            $('wiki')
              .split('$page', 'Page')
              .apply('Count', $('wiki').sum('$count'))
              .sort('$Count', 'descending')
              .limit(2),
          ),
      );

      let rawQueries = [];
      return basicExecutor(ex, { rawQueries }).then(result => {
        expect(rawQueries).to.deep.equal([
          {
            engine: 'druid',
            query: {
              context: {
                populateCache: false,
                skipEmptyBuckets: 'true',
                timeout: 10000,
                useCache: false,
              },
              dataSource: 'wikipedia-compact',
              granularity: {
                period: 'PT12H',
                timeZone: 'Etc/UTC',
                type: 'period',
              },
              intervals: '2015-09-12T00Z/2015-09-13T00Z',
              queryType: 'timeseries',
            },
          },
          {
            engine: 'druid',
            query: {
              aggregations: [
                {
                  fieldName: 'count',
                  name: 'Count',
                  type: 'longSum',
                },
              ],
              context: {
                populateCache: false,
                timeout: 10000,
                useCache: false,
              },
              dataSource: 'wikipedia',
              dimension: {
                dimension: 'page',
                outputName: 'Page',
                type: 'default',
              },
              granularity: 'all',
              intervals: '2015-09-12T00Z/2015-09-12T12Z',
              metric: 'Count',
              queryType: 'topN',
              threshold: 2,
            },
          },
          {
            engine: 'druid',
            query: {
              aggregations: [
                {
                  fieldName: 'count',
                  name: 'Count',
                  type: 'longSum',
                },
              ],
              context: {
                populateCache: false,
                timeout: 10000,
                useCache: false,
              },
              dataSource: 'wikipedia',
              dimension: {
                dimension: 'page',
                outputName: 'Page',
                type: 'default',
              },
              granularity: 'all',
              intervals: '2015-09-12T12Z/2015-09-13T00Z',
              metric: 'Count',
              queryType: 'topN',
              threshold: 2,
            },
          },
        ]);

        expect(result.toJS().data).to.deep.equal([
          {
            ByHour: {
              attributes: [
                {
                  name: 'TimeByHour',
                  type: 'TIME_RANGE',
                },
                {
                  name: 'Users',
                  type: 'DATASET',
                },
              ],
              data: [
                {
                  TimeByHour: {
                    end: new Date('2015-09-12T12:00:00.000Z'),
                    start: new Date('2015-09-12T00:00:00.000Z'),
                  },
                  Users: {
                    attributes: [
                      {
                        name: 'Page',
                        type: 'STRING',
                      },
                      {
                        name: 'Count',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Count: 120,
                        Page: 'User:Cyde/List of candidates for speedy deletion/Subpage',
                      },
                      {
                        Count: 106,
                        Page: "Wikipedia:Administrators' noticeboard/Incidents",
                      },
                    ],
                    keys: ['Page'],
                  },
                },
                {
                  TimeByHour: {
                    end: new Date('2015-09-13T00:00:00.000Z'),
                    start: new Date('2015-09-12T12:00:00.000Z'),
                  },
                  Users: {
                    attributes: [
                      {
                        name: 'Page',
                        type: 'STRING',
                      },
                      {
                        name: 'Count',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Count: 227,
                        Page: 'Jeremy Corbyn',
                      },
                      {
                        Count: 138,
                        Page: 'Flavia Pennetta',
                      },
                    ],
                    keys: ['Page'],
                  },
                },
              ],
              keys: ['TimeByHour'],
            },
          },
        ]);
      });
    });

    it('does not zero fill', () => {
      let ex = $('wiki')
        .filter('$cityName == "El Paso"')
        .split($('time').timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('Count', '$wiki.sum($count)')
        .sort('$TimeByHour', 'ascending');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.have.length(2);
      });
    });

    it('works with time split with quantile', () => {
      let ex = $('wiki')
        .filter('$cityName == "El Paso"')
        .split($('time').timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
        .apply('count', '$wiki.sum($count)')
        .apply('Delta95th', $('wiki').quantile('$delta_hist', 0.95))
        .sort('$TimeByHour', 'ascending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Delta95th: -39,
            TimeByHour: {
              end: new Date('2015-09-12T07:00:00.000Z'),
              start: new Date('2015-09-12T06:00:00.000Z'),
            },
            count: 1,
          },
          {
            Delta95th: 0,
            TimeByHour: {
              end: new Date('2015-09-12T17:00:00.000Z'),
              start: new Date('2015-09-12T16:00:00.000Z'),
            },
            count: 1,
          },
        ]);
      });
    });

    it.skip('works with single apply on string column (total)', () => {
      let ex = $('wiki').apply('count', '$wiki.sum($commentLengthStr.cast("NUMBER"))');

      return basicExecutor(ex).then(result => {
        console.log('result', result);
        expect(result.toJS().data).to.deep.equal([]);
      });
    });

    it('works with applies on string columns', () => {
      let ex = $('wiki')
        .split($('channel'), 'Channel')
        .apply('sum_cl', '$wiki.sum($commentLength)')
        .apply('sum_cls', '$wiki.sum($commentLengthStr.cast("NUMBER"))')
        .apply('min_cls', '$wiki.min($commentLengthStr.cast("NUMBER"))')
        .apply('max_cls', '$wiki.max($commentLengthStr.cast("NUMBER"))')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            max_cls: 253,
            min_cls: 1,
            sum_cl: 267579,
            sum_cls: 267579,
            Channel: 'ar',
          },
          {
            max_cls: 253,
            min_cls: 4,
            sum_cl: 12192,
            sum_cls: 12192,
            Channel: 'be',
          },
          {
            max_cls: 253,
            min_cls: 1,
            sum_cl: 46398,
            sum_cls: 46398,
            Channel: 'bg',
          },
        ]);
      });
    });

    it('works with contains (case sensitive) filter', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('page').contains('wiki')))
        .apply(
          'Pages',
          $('wiki')
            .split($('page'), 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Pages: {
              attributes: [
                {
                  name: 'Page',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Count: 25,
                  Page: 'Wikipedia:Checklijst langdurig structureel vandalisme/1wikideb1',
                },
                {
                  Count: 12,
                  Page: 'Diskuse s wikipedistou:Zdenekk2',
                },
                {
                  Count: 11,
                  Page: 'Overleg gebruiker:Wwikix',
                },
              ],
              keys: ['Page'],
            },
          },
        ]);
      });
    });

    it('works with contains(ignoreCase) filter', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('page').contains('wiki', 'ignoreCase')))
        .apply(
          'Pages',
          $('wiki')
            .split($('page'), 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Pages: {
              attributes: [
                {
                  name: 'Page',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Count: 228,
                  Page: "Wikipedia:Administrators' noticeboard/Incidents",
                },
                {
                  Count: 186,
                  Page: 'Wikipedia:Vandalismusmeldung',
                },
                {
                  Count: 146,
                  Page: 'Wikipedia:Administrator intervention against vandalism',
                },
              ],
              keys: ['Page'],
            },
          },
        ]);
      });
    });

    it('works with match() filter', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('page').match('^.*Bot.*$')))
        .apply(
          'Pages',
          $('wiki')
            .split($('page'), 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(3),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Pages: {
              attributes: [
                {
                  name: 'Page',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Count: 54,
                  Page: 'Wikipedia:Usernames for administrator attention/Bot',
                },
                {
                  Count: 23,
                  Page: 'Usuari:TronaBot/log:Activitat reversors per hores',
                },
                {
                  Count: 23,
                  Page: 'Usuari:TronaBot/log:Reversions i patrullatge',
                },
              ],
              keys: ['Page'],
            },
          },
        ]);
      });
    });

    it('works with rank boosting', () => {
      let ex = $('wiki')
        .filter('$cityName.contains("An", "ignoreCase")')
        .split('$cityName', 'City')
        .apply(
          'Rank',
          '$wiki.sum($count) * $City.transformCase("lowerCase").match("\\ban").then(10).fallback(1)',
        )
        .sort('$Rank', 'descending')
        .limit(7);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          { City: 'Los Angeles', Rank: 520 },
          { City: 'Milan', Rank: 267 },
          { City: 'Hanoi', Rank: 183 },
          { City: 'Bangalore', Rank: 181 },
          { City: 'Santo Antonio de Jesus', Rank: 140 },
          { City: 'Santiago', Rank: 135 },
          { City: 'Ankara', Rank: 110 },
        ]);
      });
    });

    it('works name reassignment', () => {
      let ex = $('wiki')
        .split('$cityName.fallback("NA") ++ "-" ++ $countryIsoCode.fallback("NA")', 'cityName')
        .apply('Count', '$wiki.sum($count)')
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 354931,
            cityName: 'NA-NA',
          },
          {
            Count: 1033,
            cityName: 'NA-IT',
          },
          {
            Count: 942,
            cityName: 'NA-JP',
          },
        ]);
      });
    });

    it('works with split sort on string', () => {
      let ex = ply().apply(
        'Channels',
        $('wiki').split('$channel', 'Channel').sort('$Channel', 'ascending').limit(3),
      );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channels: {
              attributes: [
                {
                  name: 'Channel',
                  type: 'STRING',
                },
              ],
              data: [
                {
                  Channel: 'ar',
                },
                {
                  Channel: 'be',
                },
                {
                  Channel: 'bg',
                },
              ],
              keys: ['Channel'],
            },
          },
        ]);
      });
    });

    it('works with concat split', () => {
      let ex = ply().apply(
        'Pages',
        $('wiki')
          .split("'!!!<' ++ $page ++ '>!!!'", 'Page')
          .apply('Count', '$wiki.sum($count)')
          .sort('$Count', 'descending')
          .limit(3),
      );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Pages: {
              attributes: [
                {
                  name: 'Page',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Count: 318,
                  Page: '!!!<Jeremy Corbyn>!!!',
                },
                {
                  Count: 255,
                  Page: '!!!<User:Cyde/List of candidates for speedy deletion/Subpage>!!!',
                },
                {
                  Count: 228,
                  Page: "!!!<Wikipedia:Administrators' noticeboard/Incidents>!!!",
                },
              ],
              keys: ['Page'],
            },
          },
        ]);
      });
    });

    it('works with substr split', () => {
      let ex = ply().apply(
        'Pages',
        $('wiki')
          .split('$page.substr(0,2)', 'Page')
          .apply('Count', '$wiki.sum($count)')
          .sort('$Count', 'descending')
          .limit(3),
      );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Pages: {
              attributes: [
                {
                  name: 'Page',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Count: 22503,
                  Page: 'Ca',
                },
                {
                  Count: 20338,
                  Page: 'Us',
                },
                {
                  Count: 15332,
                  Page: 'Wi',
                },
              ],
              keys: ['Page'],
            },
          },
        ]);
      });
    });

    it('works with extract split', () => {
      let ex = ply().apply(
        'Pages',
        $('wiki')
          .split($('page').extract('([0-9]+\\.[0-9]+\\.[0-9]+)'), 'Page')
          .apply('Count', '$wiki.sum($count)')
          .sort('$Count', 'descending')
          .limit(3),
      );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Pages: {
              attributes: [
                {
                  name: 'Page',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Count: 387184,
                  Page: null,
                },
                {
                  Count: 22,
                  Page: '75.108.94',
                },
                {
                  Count: 14,
                  Page: '120.29.65',
                },
              ],
              keys: ['Page'],
            },
          },
        ]);
      });
    });

    it('works with constant lookup split', () => {
      let ex = $('wiki')
        .split(r('en').lookup('channel-lookup'), 'Channel')
        .apply('Count', '$wiki.sum($count)');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'English',
            Count: 392443,
          },
        ]);
      });
    });

    it('works with lookup split', () => {
      let ex = ply()
        .apply(
          'Channels',
          $('wiki')
            .split($('channel').lookup('channel-lookup'), 'Channel')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(4),
        )
        .apply(
          'ChannelFallbackLOL',
          $('wiki')
            .split($('channel').lookup('channel-lookup').fallback('LOL'), 'Channel')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(4),
        )
        .apply(
          'ChannelFallbackSelf',
          $('wiki')
            .split($('channel').lookup('channel-lookup').fallback('$channel'), 'Channel')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(4),
        )
        .apply(
          'ChannelFancy',
          $('wiki')
            .split(
              $('channel')
                .lookup('channel-lookup')
                .fallback('"???"')
                .concat(r(' ('), '$channel', r(')')),
              'Channel',
            )
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(4),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            ChannelFallbackLOL: {
              attributes: [
                {
                  name: 'Channel',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Channel: 'LOL',
                  Count: 227040,
                },
                {
                  Channel: 'English',
                  Count: 114711,
                },
                {
                  Channel: 'French',
                  Count: 21285,
                },
                {
                  Channel: 'Russian',
                  Count: 14031,
                },
              ],
              keys: ['Channel'],
            },
            ChannelFallbackSelf: {
              attributes: [
                {
                  name: 'Channel',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Channel: 'English',
                  Count: 114711,
                },
                {
                  Channel: 'vi',
                  Count: 99010,
                },
                {
                  Channel: 'de',
                  Count: 25103,
                },
                {
                  Channel: 'French',
                  Count: 21285,
                },
              ],
              keys: ['Channel'],
            },
            ChannelFancy: {
              attributes: [
                {
                  name: 'Channel',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Channel: 'English (en)',
                  Count: 114711,
                },
                {
                  Channel: '??? (vi)',
                  Count: 99010,
                },
                {
                  Channel: '??? (de)',
                  Count: 25103,
                },
                {
                  Channel: 'French (fr)',
                  Count: 21285,
                },
              ],
              keys: ['Channel'],
            },
            Channels: {
              attributes: [
                {
                  name: 'Channel',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Channel: null,
                  Count: 227040,
                },
                {
                  Channel: 'English',
                  Count: 114711,
                },
                {
                  Channel: 'French',
                  Count: 21285,
                },
                {
                  Channel: 'Russian',
                  Count: 14031,
                },
              ],
              keys: ['Channel'],
            },
          },
        ]);
      });
    });

    it('works with count distinct on lookup', () => {
      let ex = ply()
        .apply('CntDistChannelNormal', $('wiki').countDistinct($('channel')))
        .apply(
          'CntDistChannelLookup',
          $('wiki').countDistinct($('channel').lookup('channel-lookup')),
        )
        .apply(
          'CntDistChannelLookupXPage',
          $('wiki').countDistinct(
            $('channel').lookup('channel-lookup').concat('$page.substr(0, 1)'),
          ),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            CntDistChannelLookup: 5,
            CntDistChannelNormal: 53,
            CntDistChannelLookupXPage: 2612,
          },
        ]);
      });
    });

    it('works with quantiles (histogram)', () => {
      let ex = ply()
        .apply('deltaHist95', $('wiki').quantile($('delta_hist'), 0.95))
        .apply('deltaHistMedian', $('wiki').quantile($('delta_hist'), 0.5))
        .apply('deltaBucket95', $('wiki').quantile($('deltaBucket100'), 0.95))
        .apply('deltaBucketMedian', $('wiki').quantile($('deltaBucket100'), 0.5))
        .apply('commentLength95', $('wiki').quantile($('commentLength'), 0.95))
        .apply('commentLengthMedian', $('wiki').quantile($('commentLength'), 0.5));

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            commentLength95: 145.46353,
            commentLengthMedian: 28.108738,
            deltaBucket95: -500, // ToDo: find out why this is
            deltaBucketMedian: -500,
            deltaHist95: 161.95517,
            deltaHistMedian: 129.0191,
          },
        ]);
      });
    });

    it('works with quantiles (quantile doubles)', () => {
      let ex = ply()
        .apply('deltaQuantiles95', $('wiki').quantile($('delta_quantilesDoublesSketch'), 0.95))
        .apply(
          'deltaQuantilesMedian',
          $('wiki').quantile($('delta_quantilesDoublesSketch'), 0.5, 'k=256'),
        )
        .apply('commentLength95', $('wiki').quantile($('commentLength'), 0.95, 'v=2'))
        .apply('commentLengthMedian', $('wiki').quantile($('commentLength'), 0.5, 'v=2,k=256'))
        .apply(
          'DeltaDq98thEn',
          $('wiki').filter($('channel').is('en')).quantile('$delta_quantilesDoublesSketch', 0.98),
        )
        .apply(
          'DeltaDq98thDe',
          $('wiki').filter($('channel').is('de')).quantile('$delta_quantilesDoublesSketch', 0.98),
        );

      return basicExecutor(ex).then(result => {
        const datum = result.toJS().data[0];
        expect(datum).to.have.keys(
          'deltaQuantiles95',
          'commentLength95',
          'deltaQuantilesMedian',
          'commentLengthMedian',
          'DeltaDq98thEn',
          'DeltaDq98thDe',
        );

        // These quantile doubles are non-deterministic - so just check against some bounds
        const between = (k, min, max) => expect(min < datum[k] && datum[k] < max).to.equal(true);
        between('deltaQuantiles95', 500, 2000);
        between('commentLength95', 100, 200);
        between('deltaQuantilesMedian', 8, 40);
        between('commentLengthMedian', 15, 60);
      });
    });

    it('works with lookup IS filter', () => {
      let ex = $('wiki').filter($('channel').lookup('channel-lookup').is('English')).sum('$count');

      return basicExecutor(ex).then(result => {
        expect(result).to.deep.equal(114711);
      });
    });

    it('works with lookup IS filter with fallback', () => {
      let ex = $('wiki')
        .filter(
          $('channel').lookup('channel-lookup').fallback(r('LOL')).overlap(['English', 'LOL']),
        )
        .split($('channel').lookup('channel-lookup').fallback(r('LOL')), 'C')
        .apply('Count', '$wiki.sum($count)')
        .sort('$Count', 'descending')
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 227040,
            C: 'LOL',
          },
          {
            Count: 114711,
            C: 'English',
          },
        ]);
      });
    });

    it('works with lookup CONTAINS filter', () => {
      let ex = $('wiki')
        .filter($('channel').lookup('channel-lookup').contains('Eng', 'ignoreCase'))
        .sum('$count');

      return basicExecutor(ex).then(result => {
        expect(result).to.deep.equal(114711);
      });
    });

    it('works with string manipulation after cast action', () => {
      let ex = $('wiki')
        .filter($('deltaBucket100').absolute().cast('STRING').substr(0, 5).cast('NUMBER').is(1000))
        .sum('$count');

      return basicExecutor(ex).then(result => {
        expect(result).to.deep.equal(1621);
      });
    });

    it('works with numeric fallback', () => {
      let ex = $('wiki').sum('($added / ($added - $added)).fallback(10)');

      return basicExecutor(ex).then(result => {
        expect(result).to.deep.equal(109204 * 10);
      });
    });

    it('works with resplit agg on total', () => {
      let ex = ply()
        .apply('Count', $('wiki').sum('$count'))
        .apply(
          'Quantile',
          $('wiki')
            .split('$time.timeBucket(PT1H)')
            .apply('C', '$wiki.sum($count)')
            .quantile('$C', 0.95),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 392443,
            Quantile: 22238.2,
          },
        ]);
      });
    });

    it('works with resplit agg on total with average 1', () => {
      let range = TimeRange.fromJS({
        start: new Date('2015-09-12T12:00:00Z'),
        end: new Date('2015-09-13T00:00:00Z'),
      });
      let filterEx = $('time').overlap(range);

      let ex = ply()
        .apply('Count', $('wiki').sum('$count'))
        .apply(
          'HourlyCd',
          $('wiki')
            .filter(filterEx)
            .split('$time.timeBucket(PT6H)')
            .apply('C', '$wiki.countDistinct($user)')
            .average('$C'),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 392443,
            HourlyCd: 15101.5,
          },
        ]);
      });
    });

    it('works with resplit agg on total with average 2', () => {
      let range = TimeRange.fromJS({
        start: new Date('2015-09-12T12:00:00Z'),
        end: new Date('2015-09-13T00:00:00Z'),
      });
      let filterEx = $('time').overlap(range);

      let ex = ply()
        .apply('Count', $('wiki').sum('$count'))
        .apply(
          'AddedByHourlyCd',
          $('wiki')
            .filter(filterEx)
            .split('$time.timeBucket(PT1H)')
            .apply('C', '$wiki.sum($added) / $wiki.countDistinct($user)')
            .average('$C'),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            AddedByHourlyCd: 1260.6697346331532,
            Count: 392443,
          },
        ]);
      });
    });

    it('works with resplit agg on different dimension split', () => {
      let ex = $('wiki')
        .split('$channel', 'Channel')
        .apply('Count', $('wiki').sum('$count'))
        .apply(
          'Quantile',
          $('wiki')
            .split('$time.timeBucket(PT1H)')
            .apply('C', '$wiki.sum($count)')
            .quantile('$C', 0.95),
        )
        .sort('$Count', 'descending')
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'en',
            Count: 114711,
            Quantile: 6313.8,
          },
          {
            Channel: 'vi',
            Count: 99010,
            Quantile: 10748.596,
          },
          {
            Channel: 'de',
            Count: 25103,
            Quantile: 1737.9999,
          },
          {
            Channel: 'fr',
            Count: 21285,
            Quantile: 1379.4,
          },
          {
            Channel: 'ru',
            Count: 14031,
            Quantile: 898.5999,
          },
        ]);
      });
    });

    it('works with resplit agg on different dimension split with sum', () => {
      let ex = $('wiki')
        .split('$channel', 'Channel')
        .apply('Count', $('wiki').sum('$count'))
        .apply(
          'SumCountDistinct',
          $('wiki')
            .filter($('countryIsoCode').in(['US', 'IT']).and($('cityName').isnt(null)))
            .split('$time.timeBucket(PT1H)')
            .apply('C', '$wiki.countDistinct($user)')
            .sum('$C'),
        )
        .sort('$Count', 'descending')
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'en',
            Count: 114711,
            SumCountDistinct: 2342,
          },
          {
            Channel: 'vi',
            Count: 99010,
            SumCountDistinct: 2,
          },
          {
            Channel: 'de',
            Count: 25103,
            SumCountDistinct: 3,
          },
          {
            Channel: 'fr',
            Count: 21285,
            SumCountDistinct: 23,
          },
          {
            Channel: 'ru',
            Count: 14031,
            SumCountDistinct: 10,
          },
        ]);
      });
    });

    it('works with resplit agg on same dimension split', () => {
      let ex = $('wiki')
        .split('$time.timeBucket(PT1H)', 'Hour')
        .apply('Count', $('wiki').sum('$count'))
        .apply(
          'Quantile',
          $('wiki')
            .split('$time.timeBucket(PT1H)')
            .apply('C', '$wiki.sum($count)')
            .quantile('$C', 0.95),
        )
        .sort('$Count', 'descending')
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 23001,
            Hour: {
              end: new Date('2015-09-12T18:00:00.000Z'),
              start: new Date('2015-09-12T17:00:00.000Z'),
            },
            Quantile: 23001,
          },
          {
            Count: 22373,
            Hour: {
              end: new Date('2015-09-12T08:00:00.000Z'),
              start: new Date('2015-09-12T07:00:00.000Z'),
            },
            Quantile: 22373,
          },
          {
            Count: 21699,
            Hour: {
              end: new Date('2015-09-12T19:00:00.000Z'),
              start: new Date('2015-09-12T18:00:00.000Z'),
            },
            Quantile: 21699,
          },
          {
            Count: 21194,
            Hour: {
              end: new Date('2015-09-12T07:00:00.000Z'),
              start: new Date('2015-09-12T06:00:00.000Z'),
            },
            Quantile: 21194,
          },
          {
            Count: 20725,
            Hour: {
              end: new Date('2015-09-12T14:00:00.000Z'),
              start: new Date('2015-09-12T13:00:00.000Z'),
            },
            Quantile: 20725,
          },
        ]);
      });
    });

    it('works with resplit agg on more granular dimension split', () => {
      let ex = $('wiki')
        .split('$time.timeBucket(PT6H)', 'Hour')
        .apply('Count', $('wiki').sum('$count'))
        .apply(
          'Quantile',
          $('wiki')
            .split('$time.timeBucket(PT1H)')
            .apply('C', '$wiki.sum($count)')
            .quantile('$C', 0.95),
        )
        .sort('$Count', 'descending')
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 118793,
            Hour: {
              end: new Date('2015-09-12T18:00:00.000Z'),
              start: new Date('2015-09-12T12:00:00.000Z'),
            },
            Quantile: 22318.2,
          },
          {
            Count: 110986,
            Hour: {
              end: new Date('2015-09-12T12:00:00.000Z'),
              start: new Date('2015-09-12T06:00:00.000Z'),
            },
            Quantile: 22019.299,
          },
          {
            Count: 108525,
            Hour: {
              end: new Date('2015-09-13T00:00:00.000Z'),
              start: new Date('2015-09-12T18:00:00.000Z'),
            },
            Quantile: 21228,
          },
          {
            Count: 54139,
            Hour: {
              end: new Date('2015-09-12T06:00:00.000Z'),
              start: new Date('2015-09-12T00:00:00.000Z'),
            },
            Quantile: 12258.2,
          },
        ]);
      });
    });

    it('works with fractional bucketing', () => {
      let ex = $('wiki')
        .split($('commentLength').divide(13).numberBucket(0.1), 'Point1')
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Point1', 'ascending')
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 734,
            Point1: {
              end: 0.1,
              start: 0,
            },
          },
          {
            Count: 1456,
            Point1: {
              end: 0.2,
              start: 0.1,
            },
          },
          {
            Count: 1976,
            Point1: {
              end: 0.3,
              start: 0.2,
            },
          },
          {
            Count: 4041,
            Point1: {
              end: 0.4,
              start: 0.3,
            },
          },
          {
            Count: 2336,
            Point1: {
              end: 0.5,
              start: 0.4,
            },
          },
        ]);
      });
    });

    it('works with resplit agg on more granular dimension split (+filters)', () => {
      let ex = $('wiki')
        .split('$time.timeBucket(PT6H)', 'Hour')
        .apply('Count', $('wiki').sum('$count'))
        .apply(
          'QuantileEn',
          $('wiki')
            .filter('$channel == "en"')
            .split('$time.timeBucket(PT1H)')
            .apply('C', '$wiki.sum($count)')
            .quantile('$C', 0.95),
        )
        .apply(
          'QuantileHe',
          $('wiki')
            .filter('$channel == "he"')
            .split('$time.timeBucket(PT1H)')
            .apply('C', '$wiki.sum($count)')
            .quantile('$C', 0.95),
        )
        .sort('$Count', 'descending')
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 118793,
            Hour: {
              end: new Date('2015-09-12T18:00:00.000Z'),
              start: new Date('2015-09-12T12:00:00.000Z'),
            },
            QuantileEn: 6117.6,
            QuantileHe: 186.09999,
          },
          {
            Count: 110986,
            Hour: {
              end: new Date('2015-09-12T12:00:00.000Z'),
              start: new Date('2015-09-12T06:00:00.000Z'),
            },
            QuantileEn: 4333.8,
            QuantileHe: 175.3,
          },
          {
            Count: 108525,
            Hour: {
              end: new Date('2015-09-13T00:00:00.000Z'),
              start: new Date('2015-09-12T18:00:00.000Z'),
            },
            QuantileEn: 6544.1,
            QuantileHe: 172.8,
          },
          {
            Count: 54139,
            Hour: {
              end: new Date('2015-09-12T06:00:00.000Z'),
              start: new Date('2015-09-12T00:00:00.000Z'),
            },
            QuantileEn: 4995.7,
            QuantileHe: 51.1,
          },
        ]);
      });
    });

    it('works with resplit Hourly-Active-Users agg', () => {
      let ex = $('wiki')
        .split('$channel', 'Channel')
        .apply('Count', $('wiki').sum('$count'))
        .apply('Unique Users', $('wiki').countDistinct($('user')))
        .apply(
          'HAU',
          $('wiki')
            .split($('time').timeBucket('P1D'))
            .apply('U', '$wiki.countDistinct($user)')
            .average('$U'),
        )
        .sort('$Count', 'descending')
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            'Channel': 'en',
            'Count': 114711,
            'HAU': 16609,
            'Unique Users': 16609,
          },
          {
            'Channel': 'vi',
            'Count': 99010,
            'HAU': 201,
            'Unique Users': 201,
          },
          {
            'Channel': 'de',
            'Count': 25103,
            'HAU': 2950,
            'Unique Users': 2950,
          },
          {
            'Channel': 'fr',
            'Count': 21285,
            'HAU': 2757,
            'Unique Users': 2757,
          },
          {
            'Channel': 'ru',
            'Count': 14031,
            'HAU': 2184,
            'Unique Users': 2184,
          },
        ]);
      });
    });

    it('works with absolute number split', () => {
      let ex = ply()
        .apply(
          'AbsSplitAsc',
          $('wiki')
            .split($('commentLength').absolute(), 'AbsCommentLength')
            .apply('Count', '$wiki.sum($count)')
            .sort('$AbsCommentLength', 'ascending')
            .limit(3),
        )
        .apply(
          'AbsSplitDesc',
          $('wiki')
            .split($('commentLength').absolute(), 'AbsCommentLength')
            .apply('Count', '$wiki.sum($count)')
            .sort('$AbsCommentLength', 'descending')
            .limit(3),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            AbsSplitAsc: {
              attributes: [
                {
                  name: 'AbsCommentLength',
                  type: 'NUMBER',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  AbsCommentLength: 1,
                  Count: 734,
                },
                {
                  AbsCommentLength: 2,
                  Count: 1456,
                },
                {
                  AbsCommentLength: 3,
                  Count: 1976,
                },
              ],
              keys: ['AbsCommentLength'],
            },
            AbsSplitDesc: {
              attributes: [
                {
                  name: 'AbsCommentLength',
                  type: 'NUMBER',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  AbsCommentLength: 255,
                  Count: 193,
                },
                {
                  AbsCommentLength: 254,
                  Count: 59,
                },
                {
                  AbsCommentLength: 253,
                  Count: 243,
                },
              ],
              keys: ['AbsCommentLength'],
            },
          },
        ]);
      });
    });

    it('works with bucketed number split', () => {
      let ex = ply()
        .apply(
          'BucketSplitAsc',
          $('wiki')
            .split($('commentLength').numberBucket(5), 'Bucket')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Bucket', 'ascending')
            .limit(3),
        )
        .apply(
          'BucketSplitDesc',
          $('wiki')
            .split($('commentLength').numberBucket(5), 'Bucket')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Bucket', 'descending')
            .limit(3),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            BucketSplitAsc: {
              attributes: [
                {
                  name: 'Bucket',
                  type: 'NUMBER_RANGE',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Bucket: {
                    end: 5,
                    start: 0,
                  },
                  Count: 6522,
                },
                {
                  Bucket: {
                    end: 10,
                    start: 5,
                  },
                  Count: 15003,
                },
                {
                  Bucket: {
                    end: 15,
                    start: 10,
                  },
                  Count: 70628,
                },
              ],
              keys: ['Bucket'],
            },
            BucketSplitDesc: {
              attributes: [
                {
                  name: 'Bucket',
                  type: 'NUMBER_RANGE',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Bucket: {
                    end: 260,
                    start: 255,
                  },
                  Count: 193,
                },
                {
                  Bucket: {
                    end: 255,
                    start: 250,
                  },
                  Count: 556,
                },
                {
                  Bucket: {
                    end: 250,
                    start: 245,
                  },
                  Count: 1687,
                },
              ],
              keys: ['Bucket'],
            },
          },
        ]);
      });
    });

    it('works with bucketed split on derived column', () => {
      let ex = ply()
        .apply(
          'BucketSplitAsc',
          $('wiki')
            .split($('comment').length().numberBucket(5), 'Bucket')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Bucket', 'ascending')
            .limit(3),
        )
        .apply(
          'BucketSplitDesc',
          $('wiki')
            .split($('comment').length().numberBucket(5), 'Bucket')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Bucket', 'descending')
            .limit(3),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            BucketSplitAsc: {
              attributes: [
                {
                  name: 'Bucket',
                  type: 'NUMBER_RANGE',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Bucket: {
                    end: 5,
                    start: 0,
                  },
                  Count: 6521,
                },
                {
                  Bucket: {
                    end: 10,
                    start: 5,
                  },
                  Count: 15004,
                },
                {
                  Bucket: {
                    end: 15,
                    start: 10,
                  },
                  Count: 70626,
                },
              ],
              keys: ['Bucket'],
            },
            BucketSplitDesc: {
              attributes: [
                {
                  name: 'Bucket',
                  type: 'NUMBER_RANGE',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Bucket: {
                    end: 260,
                    start: 255,
                  },
                  Count: 193,
                },
                {
                  Bucket: {
                    end: 255,
                    start: 250,
                  },
                  Count: 556,
                },
                {
                  Bucket: {
                    end: 250,
                    start: 245,
                  },
                  Count: 1687,
                },
              ],
              keys: ['Bucket'],
            },
          },
        ]);
      });
    });

    it('can timeBucket a primary time column', () => {
      let ex = ply().apply(
        'Time',
        $('wiki')
          .split($('time').timeBucket('PT1H', 'Etc/UTC'), 'TimeCol')
          .sort('$TimeCol', 'descending')
          .limit(2),
      );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Time: {
              attributes: [
                {
                  name: 'TimeCol',
                  type: 'TIME_RANGE',
                },
              ],
              data: [
                {
                  TimeCol: {
                    end: new Date('2015-09-13T00:00:00.000Z'),
                    start: new Date('2015-09-12T23:00:00.000Z'),
                  },
                },
                {
                  TimeCol: {
                    end: new Date('2015-09-12T23:00:00.000Z'),
                    start: new Date('2015-09-12T22:00:00.000Z'),
                  },
                },
              ],
              keys: ['TimeCol'],
            },
          },
        ]);
      });
    });

    it('can timeBucket a secondary time column', () => {
      let ex = ply().apply(
        'TimeLater',
        $('wiki').split($('sometimeLater').timeBucket('PT1H', 'Etc/UTC'), 'SometimeLater').limit(5),
      );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            TimeLater: {
              attributes: [
                {
                  name: 'SometimeLater',
                  type: 'TIME_RANGE',
                },
              ],
              data: [
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T01:00:00.000Z'),
                    start: new Date('2016-09-12T00:00:00.000Z'),
                  },
                },
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T02:00:00.000Z'),
                    start: new Date('2016-09-12T01:00:00.000Z'),
                  },
                },
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T03:00:00.000Z'),
                    start: new Date('2016-09-12T02:00:00.000Z'),
                  },
                },
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T04:00:00.000Z'),
                    start: new Date('2016-09-12T03:00:00.000Z'),
                  },
                },
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T05:00:00.000Z'),
                    start: new Date('2016-09-12T04:00:00.000Z'),
                  },
                },
              ],
              keys: ['SometimeLater'],
            },
          },
        ]);
      });
    });

    it('can timeBucket a secondary time column (complex duration, tz - Asia/Kolkata)', () => {
      let ex = ply().apply(
        'TimeLater',
        $('wiki')
          .split($('sometimeLater').timeBucket('PT3H', 'Asia/Kolkata'), 'SometimeLater')
          .limit(5),
      );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            TimeLater: {
              attributes: [
                {
                  name: 'SometimeLater',
                  type: 'TIME_RANGE',
                },
              ],
              data: [
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T03:30:00.000Z'),
                    start: new Date('2016-09-12T00:30:00.000Z'),
                  },
                },
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T06:30:00.000Z'),
                    start: new Date('2016-09-12T03:30:00.000Z'),
                  },
                },
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T09:30:00.000Z'),
                    start: new Date('2016-09-12T06:30:00.000Z'),
                  },
                },
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T12:30:00.000Z'),
                    start: new Date('2016-09-12T09:30:00.000Z'),
                  },
                },
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T15:30:00.000Z'),
                    start: new Date('2016-09-12T12:30:00.000Z'),
                  },
                },
              ],
              keys: ['SometimeLater'],
            },
          },
        ]);
      });
    });

    it.skip('can timeBucket a secondary time column (complex duration, tz - Kathmandu)', () => {
      // ToDo: wait for https://github.com/druid-io/druid/issues/4073
      let ex = ply().apply(
        'TimeLater',
        $('wiki')
          .split($('sometimeLater').timeBucket('PT3H', 'Asia/Kathmandu'), 'SometimeLater')
          .limit(5),
      );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            TimeLater: {
              attributes: [
                {
                  name: 'SometimeLater',
                  type: 'TIME_RANGE',
                },
              ],
              data: [
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T03:15:00.000Z'),
                    start: new Date('2016-09-12T00:15:00.000Z'),
                  },
                },
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T06:15:00.000Z'),
                    start: new Date('2016-09-12T03:15:00.000Z'),
                  },
                },
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T09:15:00.000Z'),
                    start: new Date('2016-09-12T06:15:00.000Z'),
                  },
                },
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T12:15:00.000Z'),
                    start: new Date('2016-09-12T09:15:00.000Z'),
                  },
                },
                {
                  SometimeLater: {
                    end: new Date('2016-09-12T15:15:00.000Z'),
                    start: new Date('2016-09-12T12:15:00.000Z'),
                  },
                },
              ],
              keys: ['SometimeLater'],
            },
          },
        ]);
      });
    });

    it('can do compare column', () => {
      let prevRange = TimeRange.fromJS({
        start: new Date('2015-09-12T00:00:00Z'),
        end: new Date('2015-09-12T12:00:00Z'),
      });
      let mainRange = TimeRange.fromJS({
        start: new Date('2015-09-12T12:00:00Z'),
        end: new Date('2015-09-13T00:00:00Z'),
      });
      let ex = $('wiki')
        .split($('channel'), 'Channel')
        .apply('CountPrev', $('wiki').filter($('time').overlap(prevRange)).sum('$count'))
        .apply('CountMain', $('wiki').filter($('time').overlap(mainRange)).sum('$count'))
        .sort($('CountMain'), 'descending')
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'en',
            CountMain: 68606,
            CountPrev: 46105,
          },
          {
            Channel: 'vi',
            CountMain: 48521,
            CountPrev: 50489,
          },
          {
            Channel: 'de',
            CountMain: 15857,
            CountPrev: 9246,
          },
          {
            Channel: 'fr',
            CountMain: 14779,
            CountPrev: 6506,
          },
          {
            Channel: 'uz',
            CountMain: 10064,
            CountPrev: 8,
          },
        ]);
      });
    });

    it('can do compare column with having', () => {
      let prevRange = TimeRange.fromJS({
        start: new Date('2015-09-12T00:00:00Z'),
        end: new Date('2015-09-12T12:00:00Z'),
      });
      let mainRange = TimeRange.fromJS({
        start: new Date('2015-09-12T12:00:00Z'),
        end: new Date('2015-09-13T00:00:00Z'),
      });
      let ex = $('wiki')
        .split($('channel'), 'Channel')
        .apply('CountPrev', $('wiki').filter($('time').overlap(prevRange)).sum('$count'))
        .apply('CountMain', $('wiki').filter($('time').overlap(mainRange)).sum('$count'))
        .sort($('CountMain'), 'descending')
        .filter($('CountMain').greaterThan(48520))
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'en',
            CountMain: 68606,
            CountPrev: 46105,
          },
          {
            Channel: 'vi',
            CountMain: 48521,
            CountPrev: 50489,
          },
        ]);
      });
    });

    it('can timeBucket on joined column', () => {
      let prevRange = TimeRange.fromJS({
        start: new Date('2015-09-12T00:00:00Z'),
        end: new Date('2015-09-12T12:00:00Z'),
      });
      let mainRange = TimeRange.fromJS({
        start: new Date('2015-09-12T12:00:00Z'),
        end: new Date('2015-09-13T00:00:00Z'),
      });
      let ex = $('wiki')
        .split(
          $('time')
            .overlap(mainRange)
            .then($('time'))
            .fallback($('time').timeShift(Duration.fromJS('PT12H')))
            .timeBucket('PT2H'),
          'TimeJoin',
        )
        .apply('CountPrev', $('wiki').filter($('time').overlap(prevRange)).sum('$count'))
        .apply('CountMain', $('wiki').filter($('time').overlap(mainRange)).sum('$count'));

      return basicExecutor(ex).then(result => {
        expect(result.toJS()).to.deep.equal({
          attributes: [
            {
              name: 'TimeJoin',
              type: 'TIME_RANGE',
            },
            {
              name: 'CountPrev',
              type: 'NUMBER',
            },
            {
              name: 'CountMain',
              type: 'NUMBER',
            },
          ],
          data: [
            {
              CountMain: 37816,
              CountPrev: 14123,
              TimeJoin: {
                end: new Date('2015-09-12T14:00:00.000Z'),
                start: new Date('2015-09-12T12:00:00.000Z'),
              },
            },
            {
              CountMain: 38388,
              CountPrev: 19168,
              TimeJoin: {
                end: new Date('2015-09-12T16:00:00.000Z'),
                start: new Date('2015-09-12T14:00:00.000Z'),
              },
            },
            {
              CountMain: 42589,
              CountPrev: 20848,
              TimeJoin: {
                end: new Date('2015-09-12T18:00:00.000Z'),
                start: new Date('2015-09-12T16:00:00.000Z'),
              },
            },
            {
              CountMain: 41828,
              CountPrev: 43567,
              TimeJoin: {
                end: new Date('2015-09-12T20:00:00.000Z'),
                start: new Date('2015-09-12T18:00:00.000Z'),
              },
            },
            {
              CountMain: 35977,
              CountPrev: 33259,
              TimeJoin: {
                end: new Date('2015-09-12T22:00:00.000Z'),
                start: new Date('2015-09-12T20:00:00.000Z'),
              },
            },
            {
              CountMain: 30720,
              CountPrev: 34160,
              TimeJoin: {
                end: new Date('2015-09-13T00:00:00.000Z'),
                start: new Date('2015-09-12T22:00:00.000Z'),
              },
            },
          ],
          keys: ['TimeJoin'],
        });
      });
    });

    it('can timeBucket on joined column with limit', () => {
      let prevRange = TimeRange.fromJS({
        start: new Date('2015-09-12T00:00:00Z'),
        end: new Date('2015-09-12T12:00:00Z'),
      });
      let mainRange = TimeRange.fromJS({
        start: new Date('2015-09-12T12:00:00Z'),
        end: new Date('2015-09-13T00:00:00Z'),
      });
      let ex = $('wiki')
        .split(
          $('time')
            .overlap(mainRange)
            .then($('time'))
            .fallback($('time').timeShift(Duration.fromJS('PT12H')))
            .timeBucket('PT2H'),
          'TimeJoin',
        )
        .apply('CountAll', $('wiki').sum('$count'))
        .apply('CountPrev', $('wiki').filter($('time').overlap(prevRange)).sum('$count'))
        .apply('CountMain', $('wiki').filter($('time').overlap(mainRange)).sum('$count'))
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            CountAll: 51939,
            CountMain: 37816,
            CountPrev: 14123,
            TimeJoin: {
              end: new Date('2015-09-12T14:00:00.000Z'),
              start: new Date('2015-09-12T12:00:00.000Z'),
            },
          },
          {
            CountAll: 57556,
            CountMain: 38388,
            CountPrev: 19168,
            TimeJoin: {
              end: new Date('2015-09-12T16:00:00.000Z'),
              start: new Date('2015-09-12T14:00:00.000Z'),
            },
          },
          {
            CountAll: 63437,
            CountMain: 42589,
            CountPrev: 20848,
            TimeJoin: {
              end: new Date('2015-09-12T18:00:00.000Z'),
              start: new Date('2015-09-12T16:00:00.000Z'),
            },
          },
        ]);
      });
    });

    it('can timeBucket on joined and overlapping column with limit', () => {
      let prevRange = TimeRange.fromJS({
        start: new Date('2015-09-12T00:00:00Z'),
        end: new Date('2015-09-12T22:00:00Z'),
      });
      let mainRange = TimeRange.fromJS({
        start: new Date('2015-09-12T02:00:00Z'),
        end: new Date('2015-09-13T00:00:00Z'),
      });
      let ex = $('wiki')
        .split(
          $('time')
            .overlap(mainRange)
            .then($('time'))
            .fallback($('time').timeShift(Duration.fromJS('PT2H')))
            .timeBucket('PT1H'),
          'TimeJoin',
        )
        .apply('CountPrev', $('wiki').filter($('time').overlap(prevRange)).sum('$count'))
        .apply('CountMain', $('wiki').filter($('time').overlap(mainRange)).sum('$count'))
        .limit(6);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            CountMain: 11020,
            CountPrev: 2681,
            TimeJoin: {
              end: new Date('2015-09-12T03:00:00.000Z'),
              start: new Date('2015-09-12T02:00:00.000Z'),
            },
          },
          {
            CountMain: 8148,
            CountPrev: 11442,
            TimeJoin: {
              end: new Date('2015-09-12T04:00:00.000Z'),
              start: new Date('2015-09-12T03:00:00.000Z'),
            },
          },
          {
            CountMain: 8240,
            CountPrev: 11020,
            TimeJoin: {
              end: new Date('2015-09-12T05:00:00.000Z'),
              start: new Date('2015-09-12T04:00:00.000Z'),
            },
          },
          {
            CountMain: 12608,
            CountPrev: 8148,
            TimeJoin: {
              end: new Date('2015-09-12T06:00:00.000Z'),
              start: new Date('2015-09-12T05:00:00.000Z'),
            },
          },
          {
            CountMain: 21194,
            CountPrev: 8240,
            TimeJoin: {
              end: new Date('2015-09-12T07:00:00.000Z'),
              start: new Date('2015-09-12T06:00:00.000Z'),
            },
          },
          {
            CountMain: 22373,
            CountPrev: 12608,
            TimeJoin: {
              end: new Date('2015-09-12T08:00:00.000Z'),
              start: new Date('2015-09-12T07:00:00.000Z'),
            },
          },
        ]);
      });
    });

    it('can timeBucket on joined and overlapping column with multiple splits and limit', () => {
      let prevRange = TimeRange.fromJS({
        start: new Date('2015-09-12T00:00:00Z'),
        end: new Date('2015-09-12T22:00:00Z'),
      });
      let mainRange = TimeRange.fromJS({
        start: new Date('2015-09-12T02:00:00Z'),
        end: new Date('2015-09-13T00:00:00Z'),
      });
      let ex = $('wiki')
        .split({
          TimeJoin: $('time')
            .overlap(mainRange)
            .then($('time'))
            .fallback($('time').timeShift(Duration.fromJS('PT2H')))
            .timeBucket('PT1H'),
          isRobot: $('isRobot'),
        })
        .apply('CountPrev', $('wiki').filter($('time').overlap(prevRange)).sum('$count'))
        .apply('CountMain', $('wiki').filter($('time').overlap(mainRange)).sum('$count'))
        .limit(6);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            CountMain: 5982,
            CountPrev: 1557,
            TimeJoin: {
              end: new Date('2015-09-12T03:00:00.000Z'),
              start: new Date('2015-09-12T02:00:00.000Z'),
            },
            isRobot: false,
          },
          {
            CountMain: 5038,
            CountPrev: 1124,
            TimeJoin: {
              end: new Date('2015-09-12T03:00:00.000Z'),
              start: new Date('2015-09-12T02:00:00.000Z'),
            },
            isRobot: true,
          },
          {
            CountMain: 5915,
            CountPrev: 6566,
            TimeJoin: {
              end: new Date('2015-09-12T04:00:00.000Z'),
              start: new Date('2015-09-12T03:00:00.000Z'),
            },
            isRobot: false,
          },
          {
            CountMain: 2233,
            CountPrev: 4876,
            TimeJoin: {
              end: new Date('2015-09-12T04:00:00.000Z'),
              start: new Date('2015-09-12T03:00:00.000Z'),
            },
            isRobot: true,
          },
          {
            CountMain: 6098,
            CountPrev: 5982,
            TimeJoin: {
              end: new Date('2015-09-12T05:00:00.000Z'),
              start: new Date('2015-09-12T04:00:00.000Z'),
            },
            isRobot: false,
          },
          {
            CountMain: 2142,
            CountPrev: 5038,
            TimeJoin: {
              end: new Date('2015-09-12T05:00:00.000Z'),
              start: new Date('2015-09-12T04:00:00.000Z'),
            },
            isRobot: true,
          },
        ]);
      });
    });

    it('can timeBucket on joined and overlapping column with limit and sort', () => {
      let prevRange = TimeRange.fromJS({
        start: new Date('2015-09-12T00:00:00Z'),
        end: new Date('2015-09-12T22:00:00Z'),
      });
      let mainRange = TimeRange.fromJS({
        start: new Date('2015-09-12T02:00:00Z'),
        end: new Date('2015-09-13T00:00:00Z'),
      });
      let ex = $('wiki')
        .split(
          $('time')
            .overlap(mainRange)
            .then($('time'))
            .fallback($('time').timeShift(Duration.fromJS('PT2H')))
            .timeBucket('PT1H'),
          'TimeJoin',
        )
        .apply('CountPrev', $('wiki').filter($('time').overlap(prevRange)).sum('$count'))
        .apply('CountMain', $('wiki').filter($('time').overlap(mainRange)).sum('$count'))
        .sort('$CountMain', 'descending')
        .limit(6);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            CountMain: 23001,
            CountPrev: 19655,
            TimeJoin: {
              end: new Date('2015-09-12T18:00:00.000Z'),
              start: new Date('2015-09-12T17:00:00.000Z'),
            },
          },
          {
            CountMain: 22373,
            CountPrev: 12608,
            TimeJoin: {
              end: new Date('2015-09-12T08:00:00.000Z'),
              start: new Date('2015-09-12T07:00:00.000Z'),
            },
          },
          {
            CountMain: 21699,
            CountPrev: 19588,
            TimeJoin: {
              end: new Date('2015-09-12T19:00:00.000Z'),
              start: new Date('2015-09-12T18:00:00.000Z'),
            },
          },
          {
            CountMain: 21194,
            CountPrev: 8240,
            TimeJoin: {
              end: new Date('2015-09-12T07:00:00.000Z'),
              start: new Date('2015-09-12T06:00:00.000Z'),
            },
          },
          {
            CountMain: 20725,
            CountPrev: 16240,
            TimeJoin: {
              end: new Date('2015-09-12T14:00:00.000Z'),
              start: new Date('2015-09-12T13:00:00.000Z'),
            },
          },
          {
            CountMain: 20129,
            CountPrev: 23001,
            TimeJoin: {
              end: new Date('2015-09-12T20:00:00.000Z'),
              start: new Date('2015-09-12T19:00:00.000Z'),
            },
          },
        ]);
      });
    });

    it('can timeBucket on joined column with sub-split', () => {
      let prevRange = TimeRange.fromJS({
        start: new Date('2015-09-12T00:00:00Z'),
        end: new Date('2015-09-12T12:00:00Z'),
      });
      let mainRange = TimeRange.fromJS({
        start: new Date('2015-09-12T12:00:00Z'),
        end: new Date('2015-09-13T00:00:00Z'),
      });
      let ex = $('wiki')
        .split(
          $('time')
            .overlap(mainRange)
            .then($('time'))
            .fallback($('time').timeShift(Duration.fromJS('PT12H')))
            .timeBucket('PT2H'),
          'TimeJoin',
        )
        .apply('CountAll', $('wiki').sum('$count'))
        .apply('CountPrev', $('wiki').filter($('time').overlap(prevRange)).sum('$count'))
        .apply('CountMain', $('wiki').filter($('time').overlap(mainRange)).sum('$count'))
        .limit(2)
        .apply(
          'Channels',
          $('wiki')
            .split('$channel', 'Channel')
            .apply('CountPrev', $('wiki').filter($('time').overlap(prevRange)).sum('$count'))
            .apply('CountMain', $('wiki').filter($('time').overlap(mainRange)).sum('$count'))
            .sort('$CountMain', 'descending')
            .limit(2),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channels: {
              attributes: [
                {
                  name: 'Channel',
                  type: 'STRING',
                },
                {
                  name: 'CountPrev',
                  type: 'NUMBER',
                },
                {
                  name: 'CountMain',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Channel: 'en',
                  CountMain: 10698,
                  CountPrev: 5906,
                },
                {
                  Channel: 'vi',
                  CountMain: 7650,
                  CountPrev: 3771,
                },
              ],
              keys: ['Channel'],
            },
            CountAll: 51939,
            CountMain: 37816,
            CountPrev: 14123,
            TimeJoin: {
              end: new Date('2015-09-12T14:00:00.000Z'),
              start: new Date('2015-09-12T12:00:00.000Z'),
            },
          },
          {
            Channels: {
              attributes: [
                {
                  name: 'Channel',
                  type: 'STRING',
                },
                {
                  name: 'CountPrev',
                  type: 'NUMBER',
                },
                {
                  name: 'CountMain',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Channel: 'en',
                  CountMain: 10844,
                  CountPrev: 9439,
                },
                {
                  Channel: 'vi',
                  CountMain: 9258,
                  CountPrev: 2969,
                },
              ],
              keys: ['Channel'],
            },
            CountAll: 57556,
            CountMain: 38388,
            CountPrev: 19168,
            TimeJoin: {
              end: new Date('2015-09-12T16:00:00.000Z'),
              start: new Date('2015-09-12T14:00:00.000Z'),
            },
          },
        ]);
      });
    });

    it('can timeBucket on joined column (sort by delta)', () => {
      let prevRange = TimeRange.fromJS({
        start: new Date('2015-09-12T00:00:00Z'),
        end: new Date('2015-09-12T12:00:00Z'),
      });
      let mainRange = TimeRange.fromJS({
        start: new Date('2015-09-12T12:00:00Z'),
        end: new Date('2015-09-13T00:00:00Z'),
      });
      let ex = $('wiki')
        .split(
          $('time')
            .overlap(mainRange)
            .then($('time'))
            .fallback($('time').timeShift(Duration.fromJS('PT12H')))
            .timeBucket('PT2H'),
          'TimeJoin',
        )
        .apply('CountPrev', $('wiki').filter($('time').overlap(prevRange)).sum('$count'))
        .apply('CountMain', $('wiki').filter($('time').overlap(mainRange)).sum('$count'))
        .apply('Delta', '$CountMain - $CountPrev')
        .sort('$Delta', 'descending');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            CountMain: 37816,
            CountPrev: 14123,
            Delta: 23693,
            TimeJoin: {
              end: new Date('2015-09-12T14:00:00.000Z'),
              start: new Date('2015-09-12T12:00:00.000Z'),
            },
          },
          {
            CountMain: 42589,
            CountPrev: 20848,
            Delta: 21741,
            TimeJoin: {
              end: new Date('2015-09-12T18:00:00.000Z'),
              start: new Date('2015-09-12T16:00:00.000Z'),
            },
          },
          {
            CountMain: 38388,
            CountPrev: 19168,
            Delta: 19220,
            TimeJoin: {
              end: new Date('2015-09-12T16:00:00.000Z'),
              start: new Date('2015-09-12T14:00:00.000Z'),
            },
          },
          {
            CountMain: 35977,
            CountPrev: 33259,
            Delta: 2718,
            TimeJoin: {
              end: new Date('2015-09-12T22:00:00.000Z'),
              start: new Date('2015-09-12T20:00:00.000Z'),
            },
          },
          {
            CountMain: 41828,
            CountPrev: 43567,
            Delta: -1739,
            TimeJoin: {
              end: new Date('2015-09-12T20:00:00.000Z'),
              start: new Date('2015-09-12T18:00:00.000Z'),
            },
          },
          {
            CountMain: 30720,
            CountPrev: 34160,
            Delta: -3440,
            TimeJoin: {
              end: new Date('2015-09-13T00:00:00.000Z'),
              start: new Date('2015-09-12T22:00:00.000Z'),
            },
          },
        ]);
      });
    });

    it.skip('can do a sub-query', () => {
      // ToDo: solve this mystery
      let ex = ply()
        .apply(
          'data1',
          $('wiki')
            .split($('time').timeFloor('PT1H', 'Etc/UTC'), 'TimeCol')
            .apply('Count', '$wiki.sum($count)')
            .sort('$TimeCol', 'descending')
            .limit(2),
        )
        .apply('MinCount', '$data1.min($Count)')
        .apply('MaxCount', '$data1.max($Count)');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            MaxCount: 15906,
            MinCount: 14814,
            data1: {
              attributes: [
                {
                  name: 'TimeCol',
                  type: 'TIME',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Count: 14814,
                  TimeCol: new Date('2015-09-12T23:00:00.000Z'),
                },
                {
                  Count: 15906,
                  TimeCol: new Date('2015-09-12T22:00:00.000Z'),
                },
              ],
              keys: ['TimeCol'],
            },
          },
        ]);
      });
    });

    it.skip('can do a sub-split in aggregator', () => {
      let ex = $('wiki')
        .split('$channel', 'Channel')
        .apply('Count', '$wiki.sum($count)')
        .apply('MinByRobot', '$wiki.split($isRobot, Blah).apply(Cnt, $wiki.sum($count)).min($Cnt)')
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([]);
      });
    });

    it('works multi-dimensional GROUP BYs', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('channel').isnt('en')))
        .apply(
          'Groups',
          $('wiki')
            .split({
              Channel: '$channel',
              TimeByHour: '$time.timeBucket(PT2H)',
              IsNew: '$isNew',
              ChannelIsDE: "$channel == 'de'",
            })
            .apply('Count', $('wiki').sum('$count'))
            .sort('$Count', 'descending')
            .limit(4),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Groups: {
              attributes: [
                {
                  name: 'Channel',
                  type: 'STRING',
                },
                {
                  name: 'ChannelIsDE',
                  type: 'BOOLEAN',
                },
                {
                  name: 'IsNew',
                  type: 'BOOLEAN',
                },
                {
                  name: 'TimeByHour',
                  type: 'TIME_RANGE',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Channel: 'vi',
                  ChannelIsDE: false,
                  Count: 24258,
                  IsNew: false,
                  TimeByHour: {
                    end: new Date('2015-09-12T08:00:00.000Z'),
                    start: new Date('2015-09-12T06:00:00.000Z'),
                  },
                },
                {
                  Channel: 'vi',
                  ChannelIsDE: false,
                  Count: 11215,
                  IsNew: false,
                  TimeByHour: {
                    end: new Date('2015-09-12T18:00:00.000Z'),
                    start: new Date('2015-09-12T16:00:00.000Z'),
                  },
                },
                {
                  Channel: 'vi',
                  ChannelIsDE: false,
                  Count: 9246,
                  IsNew: false,
                  TimeByHour: {
                    end: new Date('2015-09-12T16:00:00.000Z'),
                    start: new Date('2015-09-12T14:00:00.000Z'),
                  },
                },
                {
                  Channel: 'vi',
                  ChannelIsDE: false,
                  Count: 8917,
                  IsNew: false,
                  TimeByHour: {
                    end: new Date('2015-09-12T10:00:00.000Z'),
                    start: new Date('2015-09-12T08:00:00.000Z'),
                  },
                },
              ],
              keys: ['Channel', 'ChannelIsDE', 'IsNew', 'TimeByHour'],
            },
          },
        ]);
      });
    });

    it('works multi-dimensional GROUP BYs (no sort)', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('channel').isnt('en')))
        .apply(
          'Groups',
          $('wiki')
            .split({
              Channel: '$channel',
              TimeByHour: '$time.timeBucket(PT2H)',
              IsNew: '$isNew',
              ChannelIsDE: "$channel == 'de'",
            })
            .apply('Count', $('wiki').sum('$count'))
            .limit(4),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Groups: {
              attributes: [
                {
                  name: 'Channel',
                  type: 'STRING',
                },
                {
                  name: 'ChannelIsDE',
                  type: 'BOOLEAN',
                },
                {
                  name: 'IsNew',
                  type: 'BOOLEAN',
                },
                {
                  name: 'TimeByHour',
                  type: 'TIME_RANGE',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Channel: 'ar',
                  ChannelIsDE: false,
                  Count: 168,
                  IsNew: false,
                  TimeByHour: {
                    end: new Date('2015-09-12T02:00:00.000Z'),
                    start: new Date('2015-09-12T00:00:00.000Z'),
                  },
                },
                {
                  Channel: 'ar',
                  ChannelIsDE: false,
                  Count: 252,
                  IsNew: false,
                  TimeByHour: {
                    end: new Date('2015-09-12T04:00:00.000Z'),
                    start: new Date('2015-09-12T02:00:00.000Z'),
                  },
                },
                {
                  Channel: 'ar',
                  ChannelIsDE: false,
                  Count: 277,
                  IsNew: false,
                  TimeByHour: {
                    end: new Date('2015-09-12T06:00:00.000Z'),
                    start: new Date('2015-09-12T04:00:00.000Z'),
                  },
                },
                {
                  Channel: 'ar',
                  ChannelIsDE: false,
                  Count: 344,
                  IsNew: false,
                  TimeByHour: {
                    end: new Date('2015-09-12T08:00:00.000Z'),
                    start: new Date('2015-09-12T06:00:00.000Z'),
                  },
                },
              ],
              keys: ['Channel', 'ChannelIsDE', 'IsNew', 'TimeByHour'],
            },
          },
        ]);
      });
    });

    it('works multi-dimensional GROUP BYs with time', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('channel').isnt('en')))
        .apply(
          'Groups',
          $('wiki')
            .split({
              channel: '$channel',
              __time: '$time.timeBucket(PT2H)',
            })
            .apply('Count', $('wiki').sum('$count'))
            .sort('$Count', 'descending')
            .limit(4),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Groups: {
              attributes: [
                {
                  name: '__time',
                  type: 'TIME_RANGE',
                },
                {
                  name: 'channel',
                  type: 'STRING',
                },
                {
                  name: 'Count',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Count: 24276,
                  __time: {
                    end: new Date('2015-09-12T08:00:00.000Z'),
                    start: new Date('2015-09-12T06:00:00.000Z'),
                  },
                  channel: 'vi',
                },
                {
                  Count: 11223,
                  __time: {
                    end: new Date('2015-09-12T18:00:00.000Z'),
                    start: new Date('2015-09-12T16:00:00.000Z'),
                  },
                  channel: 'vi',
                },
                {
                  Count: 9258,
                  __time: {
                    end: new Date('2015-09-12T16:00:00.000Z'),
                    start: new Date('2015-09-12T14:00:00.000Z'),
                  },
                  channel: 'vi',
                },
                {
                  Count: 8928,
                  __time: {
                    end: new Date('2015-09-12T10:00:00.000Z'),
                    start: new Date('2015-09-12T08:00:00.000Z'),
                  },
                  channel: 'vi',
                },
              ],
              keys: ['__time', 'channel'],
            },
          },
        ]);
      });
    });

    it('works with FALSE filter', () => {
      let ex = $('wiki')
        .filter(Expression.FALSE)
        .split({ isNew: '$isNew', isRobot: '$isRobot' })
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .split('$isNew', 'isNew', 'data')
        .apply('SumTotalEdits', '$data.sum($TotalEdits)');

      return basicExecutor(ex).then(result => {
        expect(result.toJS()).to.deep.equal({
          attributes: [
            {
              name: 'SumTotalEdits',
              type: 'NUMBER',
            },
          ],
          data: [],
          keys: ['isNew'],
        });
      });
    });

    it('works nested GROUP BYs', () => {
      let ex = $('wiki')
        .split({ isNew: '$isNew', isRobot: '$isRobot' })
        .apply('TotalEdits', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .split('$isNew', 'isNew', 'data')
        .apply('SumTotalEdits', '$data.sum($TotalEdits)');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            SumTotalEdits: 368841,
            isNew: false,
          },
          {
            SumTotalEdits: 23602,
            isNew: true,
          },
        ]);
      });
    });

    it('works string range', () => {
      let ex = $('wiki')
        .filter('$cityName > "nice"')
        .filter('$comment < "zebra"')
        .filter('$page >= "car"')
        .filter('$countryName <= "mauritius"')
        .split({
          cityName: '$cityName',
          page: '$page',
          comment: '$comment',
          country: '$countryName',
        });

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            cityName: 'Ōita',
            comment: '/* 1982年（昭和57年） */',
            country: 'Japan',
            page: '日本のテレビアニメ作品一覧 (1980年代)',
          },
          {
            cityName: 'Ōita',
            comment: '/* 劇場版 */',
            country: 'Japan',
            page: 'ドクタースランプ',
          },
        ]);
      });
    });

    it('works with division by 0', () => {
      let ex = $('wiki')
        .split('$countryName', 'CountryName')
        .apply('AddedNyDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .sort('$AddedNyDeleted', 'descending')
        .limit(7);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            AddedNyDeleted: 804,
            CountryName: 'Zimbabwe',
          },
          {
            AddedNyDeleted: 353.45454545454544,
            CountryName: 'Iraq',
          },
          {
            AddedNyDeleted: 328,
            CountryName: 'Malta',
          },
          {
            AddedNyDeleted: 278,
            CountryName: 'Myanmar [Burma]',
          },
          {
            AddedNyDeleted: 130.8679245283019,
            CountryName: 'Costa Rica',
          },
          {
            AddedNyDeleted: 116,
            CountryName: 'Jersey',
          },
          {
            AddedNyDeleted: 113.30950378469302,
            CountryName: 'Romania',
          },
        ]);
      });
    });

    it('works with raw (SELECT) + filter', () => {
      let ex = $('wiki').filter('$cityName == "El Paso"');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            added: 0,
            channel: 'en',
            cityName: 'El Paso',
            comment: '/* Clubs and organizations */',
            commentLength: 29,
            commentLengthStr: '29',
            commentTerms: null,
            count: 1,
            countryIsoCode: 'US',
            countryName: 'United States',
            deleted: 39,
            delta: -39,
            deltaBucket100: -100,
            deltaByTen: -3.9,
            delta_hist: '/84BwhwAAA==',
            delta_quantilesDoublesSketch:
              'AgMIGoAAAAABAAAAAAAAAAAAAAAAgEPAAAAAAACAQ8AAAAAAAIBDwA==',
            isAnonymous: true,
            isMinor: false,
            isNew: false,
            isRobot: false,
            isUnpatrolled: false,
            max_delta: -39,
            metroCode: '765',
            min_delta: -39,
            namespace: 'Main',
            page: 'Clint High School',
            page_unique: 'AQAAAQAAAADYAQ==',
            regionIsoCode: 'TX',
            regionName: 'Texas',
            sometimeLater: new Date('2016-09-12T06:05:00.000Z'),
            sometimeLaterMs: 1473660300000,
            time: new Date('2015-09-12T06:05:00.000Z'),
            user: '104.58.160.128',
            userChars: {
              elements: ['.', '0', '1', '2', '4', '5', '6', '8'],
              setType: 'STRING',
            },
            user_hll: 'AgEHDAMIAQDnuDoG',
            user_theta: 'AQMDAAA6zJOC2CoG9CWFMQ==',
            user_unique: 'AQAAAQAAAAFzBQ==',
          },
          {
            added: 0,
            channel: 'en',
            cityName: 'El Paso',
            comment: '/* Early life */ spelling',
            commentLength: 25,
            commentLengthStr: '25',
            commentTerms: null,
            count: 1,
            countryIsoCode: 'US',
            countryName: 'United States',
            deleted: 0,
            delta: 0,
            deltaBucket100: 0,
            deltaByTen: 0,
            delta_hist: '/84BAAAAAA==',
            delta_quantilesDoublesSketch:
              'AgMIGoAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==',
            isAnonymous: true,
            isMinor: false,
            isNew: false,
            isRobot: false,
            isUnpatrolled: false,
            max_delta: 0,
            metroCode: '765',
            min_delta: 0,
            namespace: 'Main',
            page: 'Reggie Williams (linebacker)',
            page_unique: 'AQAAAQAAAAOhEA==',
            regionIsoCode: 'TX',
            regionName: 'Texas',
            sometimeLater: new Date('2016-09-12T16:14:00.000Z'),
            sometimeLaterMs: 1473696840000,
            time: new Date('2015-09-12T16:14:00.000Z'),
            user: '67.10.203.15',
            userChars: {
              elements: ['.', '0', '1', '2', '3', '5', '6', '7'],
              setType: 'STRING',
            },
            user_hll: 'AgEHDAMIAQC8oGoY',
            user_theta: 'AQMDAAA6zJMpBk2uirJRPw==',
            user_unique: 'AQAAAQAAAAOIQA==',
          },
        ]);
      });
    });

    it('works with raw (SELECT) + limit', () => {
      let ex = $('wiki').limit(1);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            added: 0,
            channel: 'ca',
            cityName: null,
            comment: '/* Enllaços externs */',
            commentLength: 22,
            commentLengthStr: '22',
            commentTerms: null,
            count: 1,
            countryIsoCode: null,
            countryName: null,
            deleted: 1,
            delta: -1,
            deltaBucket100: -100,
            deltaByTen: -0.1,
            delta_hist: '/84Bv4AAAA==',
            delta_quantilesDoublesSketch:
              'AgMIGoAAAAABAAAAAAAAAAAAAAAAAPC/AAAAAAAA8L8AAAAAAADwvw==',
            isAnonymous: false,
            isMinor: false,
            isNew: false,
            isRobot: false,
            isUnpatrolled: true,
            max_delta: -1,
            metroCode: null,
            min_delta: -1,
            namespace: 'Main',
            page: 'Israel Ballet',
            page_unique: 'AQAAAQAAAAHHIA==',
            regionIsoCode: null,
            regionName: null,
            sometimeLater: new Date('2016-09-12T00:46:00.000Z'),
            sometimeLaterMs: 1473641160000,
            time: new Date('2015-09-12T00:46:00.000Z'),
            user: 'ChandraHelsinky',
            userChars: {
              elements: ['A', 'C', 'D', 'E', 'H', 'I', 'K', 'L', 'N', 'R', 'S', 'Y'],
              setType: 'STRING',
            },
            user_hll: 'AgEHDAMIAQAsNv0H',
            user_theta: 'AQMDAAA6zJOcUskA1pEMGA==',
            user_unique: 'AQAAAQAAAALGBA==',
          },
        ]);
      });
    });

    it('gets the right number of results in a big raw (SELECT ascending)', () => {
      let limit = 15001;
      let ex = $('wiki')
        .filter('$cityName == null')
        .select('time', 'cityName')
        .sort('$time', 'ascending')
        .limit(limit);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data.length).to.deep.equal(limit);
      });
    });

    it('gets the right number of results in a big raw (SELECT descending)', () => {
      let limit = 15001;
      let ex = $('wiki')
        .filter('$cityName == null')
        .select('time', 'cityName')
        .sort('$time', 'descending')
        .limit(limit);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data.length).to.deep.equal(limit);
      });
    });

    it('works with multi-value dimension regexp having filter', () => {
      let ex = $('wiki')
        .filter('$userChars.match("[ABN]")')
        .split('$userChars', 'userChar')
        .filter('$userChar.match("B|N")')
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([{ userChar: 'B' }, { userChar: 'N' }]);
      });
    });

    it('works with multi-value dimension list (in) having filter', () => {
      let ex = $('wiki')
        .filter('$userChars.match("[ABN]")')
        .split('$userChars', 'userChar')
        .filter('$userChar == "B" or $userChar == "N"')
        .limit(5);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([{ userChar: 'B' }, { userChar: 'N' }]);
      });
    });

    it('works with basic collect', () => {
      let ex = $('wiki').split('$channel', 'channel').collect('$channel');

      return basicExecutor(ex).then(result => {
        expect(result.toJS()).to.deep.equal({
          elements: [
            'ar',
            'be',
            'bg',
            'ca',
            'ce',
            'ceb',
            'cs',
            'da',
            'de',
            'el',
            'en',
            'eo',
            'es',
            'et',
            'eu',
            'fa',
            'fi',
            'fr',
            'gl',
            'he',
            'hi',
            'hr',
            'hu',
            'hy',
            'id',
            'it',
            'ja',
            'kk',
            'ko',
            'la',
            'lt',
            'min',
            'ms',
            'nl',
            'nn',
            'no',
            'pl',
            'pt',
            'ro',
            'ru',
            'sh',
            'simple',
            'sk',
            'sl',
            'sr',
            'sv',
            'tr',
            'uk',
            'uz',
            'vi',
            'war',
            'zh',
          ],
          setType: 'STRING',
        });
      });
    });

    it('works with advanced collect', () => {
      let ex = $('wiki')
        .split('$channel', 'channel')
        .apply('Edits', '$wiki.sum($count)')
        .sort('$Edits', 'descending')
        .limit(5)
        .collect('$channel');

      return basicExecutor(ex).then(result => {
        expect(result.toJS()).to.deep.equal({
          elements: ['en', 'vi', 'de', 'fr', 'ru'],
          setType: 'STRING',
        });
      });
    });

    it('works with collect as a sub-filter', () => {
      let ex = ply()
        .apply(
          'wiki',
          $('wiki').filter(
            $('channel').in(
              $('wiki')
                .split('$channel', 'channel')
                .apply('Edits', '$wiki.sum($count)')
                .sort('$Edits', 'descending')
                .limit(5)
                .collect('$channel'),
            ),
          ),
        )
        .apply('Count', '$wiki.sum($count)')
        .apply('Added', '$wiki.sum($added)');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Added: 54157728,
            Count: 274140,
          },
        ]);
      });
    });

    it('works with filtered double split', () => {
      let ex = ply()
        .apply(
          'wiki',
          $('wiki').filter(
            $('time').overlap(new Date('2015-09-11T23:59:00Z'), new Date('2015-09-12T23:59:00Z')),
          ),
        )
        .apply('count', '$wiki.sum($count)')
        .apply(
          'SPLIT',
          $('wiki')
            .split('$page', 'page')
            .filter($('page').overlap(['Jeremy Corbyn', 'KalyeSerye']))
            .apply('count', '$wiki.sum($count)')
            .sort('$count', 'descending')
            .limit(2)
            .apply(
              'SPLIT',
              $('wiki')
                .split('$time.timeBucket(PT1H)', 'time')
                .apply('count', '$wiki.sum($count)')
                .sort('$time', 'ascending')
                .limit(2),
            ),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            SPLIT: {
              attributes: [
                {
                  name: 'page',
                  type: 'STRING',
                },
                {
                  name: 'count',
                  type: 'NUMBER',
                },
                {
                  name: 'SPLIT',
                  type: 'DATASET',
                },
              ],
              data: [
                {
                  SPLIT: {
                    attributes: [
                      {
                        name: 'time',
                        type: 'TIME_RANGE',
                      },
                      {
                        name: 'count',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        count: 1,
                        time: {
                          end: new Date('2015-09-12T02:00:00.000Z'),
                          start: new Date('2015-09-12T01:00:00.000Z'),
                        },
                      },
                      {
                        count: 1,
                        time: {
                          end: new Date('2015-09-12T08:00:00.000Z'),
                          start: new Date('2015-09-12T07:00:00.000Z'),
                        },
                      },
                    ],
                    keys: ['time'],
                  },
                  count: 318,
                  page: 'Jeremy Corbyn',
                },
                {
                  SPLIT: {
                    attributes: [
                      {
                        name: 'time',
                        type: 'TIME_RANGE',
                      },
                      {
                        name: 'count',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        count: 1,
                        time: {
                          end: new Date('2015-09-12T02:00:00.000Z'),
                          start: new Date('2015-09-12T01:00:00.000Z'),
                        },
                      },
                      {
                        count: 1,
                        time: {
                          end: new Date('2015-09-12T03:00:00.000Z'),
                          start: new Date('2015-09-12T02:00:00.000Z'),
                        },
                      },
                    ],
                    keys: ['time'],
                  },
                  count: 69,
                  page: 'KalyeSerye',
                },
              ],
              keys: ['page'],
            },
            count: 392239,
          },
        ]);
      });
    });

    describe('more tests', () => {
      it('works with two datasets totals only', () => {
        let ex = ply()
          .apply(
            'wikiA',
            $('wiki').filter(
              $('time').overlap(new Date('2015-09-12T12:00:00Z'), new Date('2015-09-13T00:00:00Z')),
            ),
          )
          .apply(
            'wikiB',
            $('wiki').filter(
              $('time').overlap(new Date('2015-09-12T00:00:00Z'), new Date('2015-09-12T12:00:00Z')),
            ),
          )
          .apply('CountA', '$wikiA.sum($count)')
          .apply('TotalAddedA', '$wikiA.sum($added)')
          .apply('CountB', '$wikiB.sum($count)');

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal([
            {
              CountA: 227318,
              CountB: 165125,
              TotalAddedA: 55970642,
            },
          ]);
        });
      });

      it.skip('works with two datasets with split', () => {
        let ex = ply()
          .apply(
            'wikiA',
            $('wiki').filter(
              $('time').overlap(new Date('2015-09-12T12:00:00Z'), new Date('2015-09-13T00:00:00Z')),
            ),
          )
          .apply(
            'wikiB',
            $('wiki').filter(
              $('time').overlap(new Date('2015-09-12T00:00:00Z'), new Date('2015-09-12T12:00:00Z')),
            ),
          )
          .apply('CountA', '$wikiA.sum($count)')
          .apply('TotalAddedA', '$wikiA.sum($added)')
          .apply('CountB', '$wikiB.sum($count)')
          .apply(
            'Sub',
            $('wikiA')
              .split('$user', 'User')
              .join($('wikiB').split('$user', 'User'))
              .apply('CountA', '$wikiA.sum($count)')
              .apply('CountB', '$wikiB.sum($count)'),
          );

        return basicExecutor(ex).then(result => {
          expect(result.toJS().data).to.deep.equal({});
        });
      });
    });
  });

  describe('incorrect user chars', () => {
    let wikiUserCharAsNumber = External.fromJS(
      {
        engine: 'druid',
        source: 'wikipedia',
        timeAttribute: 'time',
        allowEternity: true,
        attributes: [
          { name: 'time', type: 'TIME' },
          { name: 'channel', type: 'STRING' },
          { name: 'userChars', type: 'NUMBER' }, // This is incorrect
          { name: 'count', type: 'NUMBER', unsplitable: true },
        ],
      },
      druidRequester,
    );

    it('works with number addition', () => {
      let ex = $('wiki')
        .split('$userChars + 10', 'U')
        .apply('Count', '$wiki.sum($count)')
        .sort('$Count', 'descending')
        .limit(3);

      return ex.compute({ wiki: wikiUserCharAsNumber }).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 392442,
            U: 10,
          },
          {
            Count: 1,
            U: 15,
          },
        ]);
      });

      // This is technically wrong as multi-value dimensions do not work with expressions
      // The correct answer is:
      // [
      //   {
      //     "Count": 2658542
      //     "U": 0
      //   }
      //   {
      //     "Count": 68663
      //     "U": 11
      //   }
      //   {
      //     "Count": 45822
      //     "U": 12
      //   }
      // ]
    });

    it('works with number bucketing', () => {
      let ex = $('wiki')
        .split('$userChars.numberBucket(5, 2.5)', 'U')
        .apply('Count', '$wiki.sum($count)')
        .sort('$Count', 'descending')
        .limit(3);

      return ex.compute({ wiki: wikiUserCharAsNumber }).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 2658542,
            U: null,
          },
          {
            Count: 151159,
            U: {
              end: 7.5,
              start: 2.5,
            },
          },
          {
            Count: 150305,
            U: {
              end: 2.5,
              start: -2.5,
            },
          },
        ]);
      });
    });
  });

  describe('introspection', () => {
    let wikiExternal = External.fromJS(
      {
        engine: 'druid',
        source: 'wikipedia',
        timeAttribute: 'time',
        filter: $('time').overlap(
          TimeRange.fromJS({
            start: new Date('2015-09-12T00:00:00Z'),
            end: new Date('2015-09-13T00:00:00Z'),
          }),
        ),
        attributeOverrides: [
          { name: 'sometimeLater', type: 'TIME' },
          { name: 'isAnonymous', type: 'BOOLEAN' },
          { name: 'isMinor', type: 'BOOLEAN' },
          { name: 'isNew', type: 'BOOLEAN' },
          { name: 'isRobot', type: 'BOOLEAN' },
          { name: 'isUnpatrolled', type: 'BOOLEAN' },
        ],
      },
      druidRequester,
    );

    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: wikiExternal,
      },
    });

    it('introspects version and attributes', () => {
      return wikiExternal.introspect().then(introspectedExternal => {
        expect(introspectedExternal.version).to.deep.equal(info.druidVersion);
        expect(introspectedExternal.toJS().attributes.slice(0, 3)).to.deep.equal([
          {
            name: 'time',
            nativeType: '__time',
            range: {
              bounds: '[]',
              end: new Date('2015-09-12T23:59:00.000Z'),
              start: new Date('2015-09-12T00:46:00.000Z'),
            },
            type: 'TIME',
          },
          {
            maker: {
              expression: {
                name: 'added',
                op: 'ref',
              },
              op: 'sum',
            },
            name: 'added',
            nativeType: 'LONG',
            type: 'NUMBER',
            unsplitable: true,
          },
          {
            name: 'channel',
            nativeType: 'STRING',
            type: 'STRING',
          },
        ]);
      });
    });

    it('introspects attributes (shallow)', () => {
      return wikiExternal.introspect({ depth: 'shallow' }).then(introspectedExternal => {
        expect(introspectedExternal.toJS().attributes.slice(0, 3)).to.deep.equal([
          {
            name: 'time',
            nativeType: '__time',
            type: 'TIME',
          },
          {
            maker: {
              expression: {
                name: 'added',
                op: 'ref',
              },
              op: 'sum',
            },
            name: 'added',
            nativeType: 'LONG',
            type: 'NUMBER',
            unsplitable: true,
          },
          {
            name: 'channel',
            nativeType: 'STRING',
            type: 'STRING',
          },
        ]);
      });
    });

    it('introspects attributes (deep)', () => {
      return wikiExternal.introspect({ depth: 'deep' }).then(introspectedExternal => {
        expect(introspectedExternal.toJS().attributes).to.deep.equal(wikiAttributes);
      });
    });

    it('works with introspection', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('channel').is('en')))
        .apply('Count', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply(
          'Time',
          $('wiki')
            .split($('time').timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$Timestamp', 'ascending')
            .limit(3)
            .apply(
              'Pages',
              $('wiki')
                .split('$page', 'Page')
                .apply('Count', '$wiki.sum($count)')
                .sort('$Count', 'descending')
                .limit(2),
            ),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 114711,
            Time: {
              attributes: [
                {
                  name: 'Timestamp',
                  type: 'TIME_RANGE',
                },
                {
                  name: 'TotalAdded',
                  type: 'NUMBER',
                },
                {
                  name: 'Pages',
                  type: 'DATASET',
                },
              ],
              data: [
                {
                  Pages: {
                    attributes: [
                      {
                        name: 'Page',
                        type: 'STRING',
                      },
                      {
                        name: 'Count',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Count: 12,
                        Page: 'User talk:Dudeperson176123',
                      },
                      {
                        Count: 8,
                        Page: 'User:Attar-Aram syria/sandbox',
                      },
                    ],
                    keys: ['Page'],
                  },
                  Timestamp: {
                    end: new Date('2015-09-12T01:00:00.000Z'),
                    start: new Date('2015-09-12T00:00:00.000Z'),
                  },
                  TotalAdded: 331925,
                },
                {
                  Pages: {
                    attributes: [
                      {
                        name: 'Page',
                        type: 'STRING',
                      },
                      {
                        name: 'Count',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Count: 17,
                        Page: 'John Adams',
                      },
                      {
                        Count: 17,
                        Page: 'User:King Lui',
                      },
                    ],
                    keys: ['Page'],
                  },
                  Timestamp: {
                    end: new Date('2015-09-12T02:00:00.000Z'),
                    start: new Date('2015-09-12T01:00:00.000Z'),
                  },
                  TotalAdded: 1418072,
                },
                {
                  Pages: {
                    attributes: [
                      {
                        name: 'Page',
                        type: 'STRING',
                      },
                      {
                        name: 'Count',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Count: 28,
                        Page: "Wikipedia:Administrators' noticeboard/Incidents",
                      },
                      {
                        Count: 18,
                        Page: '2015 World Wrestling Championships',
                      },
                    ],
                    keys: ['Page'],
                  },
                  Timestamp: {
                    end: new Date('2015-09-12T03:00:00.000Z'),
                    start: new Date('2015-09-12T02:00:00.000Z'),
                  },
                  TotalAdded: 3045966,
                },
              ],
              keys: ['Timestamp'],
            },
            TotalAdded: 32553107,
          },
        ]);
      });
    });
  });

  describe('introspection (union dataSource)', () => {
    let doubleWikiExternal = External.fromJS(
      {
        engine: 'druid',
        source: ['wikipedia', 'wikipedia-compact'],
        timeAttribute: 'time',
        filter: $('time').overlap(
          TimeRange.fromJS({
            start: new Date('2015-09-12T00:00:00Z'),
            end: new Date('2015-09-13T00:00:00Z'),
          }),
        ),
      },
      druidRequester,
    );

    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: doubleWikiExternal,
      },
    });

    it('works with introspection', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('channel').is('en')))
        .apply('Count', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply(
          'Time',
          $('wiki')
            .split($('time').timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$Timestamp', 'ascending')
            .limit(3),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 229422,
            Time: {
              attributes: [
                {
                  name: 'Timestamp',
                  type: 'TIME_RANGE',
                },
                {
                  name: 'TotalAdded',
                  type: 'NUMBER',
                },
              ],
              data: [
                {
                  Timestamp: {
                    end: new Date('2015-09-12T01:00:00.000Z'),
                    start: new Date('2015-09-12T00:00:00.000Z'),
                  },
                  TotalAdded: 663850,
                },
                {
                  Timestamp: {
                    end: new Date('2015-09-12T02:00:00.000Z'),
                    start: new Date('2015-09-12T01:00:00.000Z'),
                  },
                  TotalAdded: 2836144,
                },
                {
                  Timestamp: {
                    end: new Date('2015-09-12T03:00:00.000Z'),
                    start: new Date('2015-09-12T02:00:00.000Z'),
                  },
                  TotalAdded: 6091932,
                },
              ],
              keys: ['Timestamp'],
            },
            TotalAdded: 65106214,
          },
        ]);
      });
    });
  });

  describe('introspection on non existent dataSource', () => {
    let wikiExternal = External.fromJS(
      {
        engine: 'druid',
        source: 'wikipedia_borat',
        timeAttribute: 'time',
        filter: $('time').overlap(
          TimeRange.fromJS({
            start: new Date('2015-09-12T00:00:00Z'),
            end: new Date('2015-09-13T00:00:00Z'),
          }),
        ),
      },
      druidRequester,
    );

    it('fail correctly', () => {
      return wikiExternal
        .introspect()
        .then(() => {
          throw new Error('DID_NOT_ERROR');
        })
        .catch(e => {
          expect(e.message).to.contain('No such datasource');
        });
    });
  });
});
