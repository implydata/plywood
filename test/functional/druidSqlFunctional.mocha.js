/*
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
let { sane } = require('../utils');

let { druidRequesterFactory } = require('plywood-druid-requester');

let plywood = require('../plywood');
let {
  External,
  DruidSQLExternal,
  TimeRange,
  $,
  s$,
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

let context = {
  priority: -23,
};

describe('DruidSQL Functional', function() {
  this.timeout(10000);

  let wikiAttributes = [
    {
      name: '__time',
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
    {
      name: 'cityName',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'comment',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'commentLength',
      nativeType: 'LONG',
      type: 'NUMBER',
    },
    {
      name: 'commentLengthStr',
      nativeType: 'STRING',
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
      name: 'countryIsoCode',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
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
      name: 'geohash',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'isAnonymous',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'isMinor',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'isNew',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'isRobot',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'isUnpatrolled',
      nativeType: 'STRING',
      type: 'STRING',
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
      name: 'namespace',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'page',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'page_unique',
      nativeType: 'hyperUnique',
      type: 'NULL',
      unsplitable: true,
    },
    {
      name: 'regionIsoCode',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'regionName',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'sometimeLater',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'sometimeLaterMs',
      nativeType: 'LONG',
      type: 'NUMBER',
    },
    {
      name: 'user',
      nativeType: 'STRING',
      type: 'STRING',
    },
    {
      name: 'userChars',
      nativeType: 'STRING',
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

  let wikiDerivedAttributes = {
    pageInBrackets: "'[' ++ $page ++ ']'",
  };

  describe('source list', () => {
    it('does a source list', async () => {
      expect(await DruidSQLExternal.getSourceList(druidRequester)).to.deep.equal([
        'wikipedia',
        'wikipedia-compact',
      ]);
    });
  });

  describe('custom SQL', () => {
    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS(
          {
            engine: 'druidsql',
            source: 'wikipedia',
            attributes: wikiAttributes,
            derivedAttributes: wikiDerivedAttributes,
            context,
          },
          druidRequester,
        ),
      },
    });

    it('works in simple aggregate case', () => {
      let ex = $('wiki')
        .split(s$(`CONCAT(channel, '~')`), 'Channel')
        .apply('Count', $('wiki').sqlAggregate(r(`SUM(t."count")`)))
        .apply('Fancy', $('wiki').sqlAggregate(r(`SQRT(SUM(t."added" * t."added"))`)))
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'en~',
            Count: 114711,
            Fancy: 717274.3671253002,
          },
          {
            Channel: 'vi~',
            Count: 99010,
            Fancy: 70972.1877005352,
          },
          {
            Channel: 'de~',
            Count: 25103,
            Fancy: 284404.77477356105,
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
        .apply(
          'CountPrev',
          $('wiki')
            .filter($('__time').overlap(prevRange))
            .sqlAggregate(r(`SUM(t."count")`)),
        )
        .apply(
          'CountMain',
          $('wiki')
            .filter($('__time').overlap(mainRange))
            .sqlAggregate(r(`SUM(t."count")`)),
        )
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
  });

  describe('custom SQL with WITH', () => {
    let basicExecutor = basicExecutorFactory({
      datasets: {
        wikiWith: External.fromJS(
          {
            engine: 'druidsql',
            source: 'wikipedia_zzz',
            attributes: wikiAttributes,
            derivedAttributes: wikiDerivedAttributes,
            context,
            withQuery: `SELECT *, CONCAT("channel", '-lol') AS "channelLol" FROM wikipedia WHERE channel = 'en'`,
          },
          druidRequester,
        ),
      },
    });

    it('works in simple aggregate case', () => {
      let ex = $('wikiWith')
        .split(s$(`t.channelLol`), 'ChannelLol')
        .apply('Count', $('wikiWith').sqlAggregate(r(`SUM(t."count")`)))
        .apply('Fancy', $('wikiWith').sqlAggregate(r(`SQRT(SUM(t."added" * t."added"))`)))
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            ChannelLol: 'en-lol',
            Count: 114711,
            Fancy: 717274.3671253002,
          },
        ]);
      });
    });
  });

  describe('defined attributes in datasource', () => {
    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS(
          {
            engine: 'druidsql',
            source: 'wikipedia',
            attributes: wikiAttributes,
            derivedAttributes: wikiDerivedAttributes,
            context,
          },
          druidRequester,
        ),
      },
    });

    it('works in simple case', () => {
      let ex = $('wiki')
        .split('$channel', 'Channel')
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Channel: 'en',
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
        ]);
      });
    });

    it('works in advanced case', () => {
      let ex = ply()
        .apply('wiki', $('wiki').filter($('channel').is('en')))
        .apply('Count', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply(
          'Namespaces',
          $('wiki')
            .split('$namespace', 'Namespace')
            .apply('Added', '$wiki.sum($added)')
            .sort('$Added', 'descending')
            .limit(2)
            .apply(
              'Time',
              $('wiki')
                .split($('__time').timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
                .apply('TotalAdded', '$wiki.sum($added)')
                .sort('$TotalAdded', 'descending')
                .limit(3),
            ),
        );
      // .apply(
      //   'PagesHaving',
      //   $("wiki").split("$page", 'Page')
      //     .apply('Count', '$wiki.sum($count)')
      //     .sort('$Count', 'descending')
      //     .filter($('Count').lessThan(30))
      //     .limit(3)
      // );

      let rawQueries = [];
      return basicExecutor(ex, { rawQueries }).then(result => {
        expect(rawQueries).to.deep.equal([
          {
            engine: 'druidsql',
            query: {
              context: {
                priority: -23,
              },
              query:
                'SELECT\nSUM("count") AS "Count",\nSUM("added") AS "TotalAdded"\nFROM "wikipedia" AS t\nWHERE ("channel"=\'en\')\nGROUP BY ()',
            },
          },
          {
            engine: 'druidsql',
            query: {
              context: {
                priority: -23,
              },
              query:
                'SELECT\n"namespace" AS "Namespace",\nSUM("added") AS "Added"\nFROM "wikipedia" AS t\nWHERE ("channel"=\'en\')\nGROUP BY 1\nORDER BY "Added" DESC\nLIMIT 2',
            },
          },
          {
            engine: 'druidsql',
            query: {
              context: {
                priority: -23,
              },
              query:
                'SELECT\nTIME_FLOOR("__time", \'PT1H\', NULL, \'Etc/UTC\') AS "Timestamp",\nSUM("added") AS "TotalAdded"\nFROM "wikipedia" AS t\nWHERE (("channel"=\'en\') AND ("namespace"=\'Main\'))\nGROUP BY 1\nORDER BY "TotalAdded" DESC\nLIMIT 3',
            },
          },
          {
            engine: 'druidsql',
            query: {
              context: {
                priority: -23,
              },
              query:
                'SELECT\nTIME_FLOOR("__time", \'PT1H\', NULL, \'Etc/UTC\') AS "Timestamp",\nSUM("added") AS "TotalAdded"\nFROM "wikipedia" AS t\nWHERE (("channel"=\'en\') AND ("namespace"=\'User talk\'))\nGROUP BY 1\nORDER BY "TotalAdded" DESC\nLIMIT 3',
            },
          },
        ]);

        expect(result.toJS().data).to.deep.equal([
          {
            Count: 114711,
            Namespaces: {
              attributes: [
                {
                  name: 'Namespace',
                  type: 'STRING',
                },
                {
                  name: 'Added',
                  type: 'NUMBER',
                },
                {
                  name: 'Time',
                  type: 'DATASET',
                },
              ],
              data: [
                {
                  Added: 11594002,
                  Namespace: 'Main',
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
                          end: new Date('2015-09-12T15:00:00.000Z'),
                          start: new Date('2015-09-12T14:00:00.000Z'),
                        },
                        TotalAdded: 740968,
                      },
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T19:00:00.000Z'),
                          start: new Date('2015-09-12T18:00:00.000Z'),
                        },
                        TotalAdded: 739956,
                      },
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T23:00:00.000Z'),
                          start: new Date('2015-09-12T22:00:00.000Z'),
                        },
                        TotalAdded: 708543,
                      },
                    ],
                    keys: ['Timestamp'],
                  },
                },
                {
                  Added: 9210976,
                  Namespace: 'User talk',
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
                        TotalAdded: 693571,
                      },
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T18:00:00.000Z'),
                          start: new Date('2015-09-12T17:00:00.000Z'),
                        },
                        TotalAdded: 634804,
                      },
                      {
                        Timestamp: {
                          end: new Date('2015-09-12T03:00:00.000Z'),
                          start: new Date('2015-09-12T02:00:00.000Z'),
                        },
                        TotalAdded: 573768,
                      },
                    ],
                    keys: ['Timestamp'],
                  },
                },
              ],
              keys: ['Namespace'],
            },
            TotalAdded: 32553107,
          },
        ]);
      });
    });

    it('works with all kinds of cool aggregates on totals level', () => {
      let ex = ply()
        .apply('NumPages', $('wiki').countDistinct('$page'))
        .apply(
          'NumEnPages',
          $('wiki')
            .filter($('channel').is('en'))
            .countDistinct('$page'),
        )
        .apply('ChannelAdded', $('wiki').sum('$added'))
        .apply(
          'ChannelENAdded',
          $('wiki')
            .filter($('channel').is('en'))
            .sum('$added'),
        )
        .apply(
          'ChannelENishAdded',
          $('wiki')
            .filter($('channel').contains('en'))
            .sum('$added'),
        )
        .apply('Count', $('wiki').sum('$count'))
        .apply(
          'CountSquareRoot',
          $('wiki')
            .sum('$count')
            .power(0.5),
        )
        .apply(
          'CountSquared',
          $('wiki')
            .sum('$count')
            .power(2),
        )
        .apply(
          'One',
          $('wiki')
            .sum('$count')
            .power(0),
        )
        .apply(
          'AddedByDeleted',
          $('wiki')
            .sum('$added')
            .divide($('wiki').sum('$deleted')),
        )
        .apply('Delta95th', $('wiki').quantile('$delta_hist', 0.95))
        .apply(
          'Delta99thX2',
          $('wiki')
            .quantile('$delta_hist', 0.99)
            .multiply(2),
        );
      // .apply(
      //   'Delta98thEn',
      //   $('wiki')
      //     .filter($('channel').is('en'))
      //     .quantile('$delta_hist', 0.98),
      // )
      // .apply(
      //   'Delta98thDe',
      //   $('wiki')
      //     .filter($('channel').is('de'))
      //     .quantile('$delta_hist', 0.98),
      // );

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
            Delta95th: 161.95516967773438,
            Delta99thX2: 328.9096984863281,
          },
        ]);
      });
    });

    it.skip('works with boolean GROUP BYs', () => {
      let ex = $('wiki')
        .split($('channel').is('en'), 'ChannelIsEn')
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Count', 'descending');

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            ChannelIsEn: false,
            Count: 277732,
          },
          {
            ChannelIsEn: true,
            Count: 114711,
          },
        ]);
      });
    });

    it('works string range', () => {
      let ex = $('wiki')
        .filter($('cityName').greaterThan('Eagleton'))
        .split('$cityName', 'CityName')
        .sort('$CityName', 'descending')
        .limit(10);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            CityName: 'Ōita',
          },
          {
            CityName: 'Łódź',
          },
          {
            CityName: 'İzmit',
          },
          {
            CityName: 'České Budějovice',
          },
          {
            CityName: 'Ürümqi',
          },
          {
            CityName: 'Ústí nad Labem',
          },
          {
            CityName: 'Évry',
          },
          {
            CityName: 'Épinay-sur-Seine',
          },
          {
            CityName: 'Épernay',
          },
          {
            CityName: 'Élancourt',
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

    it('works with non __time time expression', () => {
      let ex = $('wiki')
        .split(s$('MILLIS_TO_TIMESTAMP(t.added)').timeBucket('PT1S', 'Etc/UTC'), 'addedAsTime')
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Count', 'descending')
        .limit(2);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            Count: 373662,
            addedAsTime: {
              end: new Date('1970-01-01T00:00:01.000Z'),
              start: new Date('1970-01-01T00:00:00.000Z'),
            },
          },
          {
            Count: 9744,
            addedAsTime: {
              end: new Date('1970-01-01T00:00:02.000Z'),
              start: new Date('1970-01-01T00:00:01.000Z'),
            },
          },
        ]);
      });
    });

    it('works with cast', () => {
      let ex = $('wiki')
        .split(s$('commentLengthStr').cast('NUMBER'), 'CommentLength')
        .apply('Count', $('wiki').sum('$count'))
        .sort('$Count', 'descending')
        .limit(3);

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
            CommentLength: 13,
            Count: 42013,
          },
          {
            CommentLength: 34,
            Count: 24469,
          },
          {
            CommentLength: 29,
            Count: 22526,
          },
        ]);
      });
    });
  });

  describe('incorrect commentLength and comment', () => {
    let wikiUserCharAsNumber = External.fromJS(
      {
        engine: 'druidsql',
        source: 'wikipedia',
        timeAttribute: 'time',
        allowEternity: true,
        context,
        attributes: [
          { name: 'time', type: 'TIME' },
          { name: 'comment', type: 'STRING' },
          { name: 'page', type: 'NUMBER' }, // This is incorrect
          { name: 'count', type: 'NUMBER', unsplitable: true },
        ],
      },
      druidRequester,
    );
  });

  describe('introspection', () => {
    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: External.fromJS(
          {
            engine: 'druidsql',
            source: 'wikipedia',
            context,
          },
          druidRequester,
        ),
      },
    });

    it('introspects table', async () => {
      const external = await External.fromJS(
        {
          engine: 'druidsql',
          source: 'wikipedia',
          context,
        },
        druidRequester,
      ).introspect();

      expect(external.version).to.equal(info.druidVersion);
      expect(external.toJS().attributes).to.deep.equal(wikiAttributes);
    });

    it('introspects withQuery (no star)', async () => {
      const external = await External.fromJS(
        {
          engine: 'druidsql',
          source: 'wikipedia',
          withQuery: `SELECT __time, page, "user" = 'vad' as is_vad, count(*) as cnt FROM wikipedia GROUP BY 1, 2, 3`,
          context,
        },
        druidRequester,
      ).introspect();

      expect(external.version).to.equal(info.druidVersion);
      expect(external.toJS().attributes).to.deep.equal([
        {
          name: '__time',
          nativeType: 'TIMESTAMP',
          type: 'TIME',
        },
        {
          name: 'page',
          nativeType: 'STRING',
          type: 'STRING',
        },
        {
          name: 'is_vad',
          nativeType: 'BOOLEAN',
          type: 'BOOLEAN',
        },
        {
          name: 'cnt',
          nativeType: 'LONG',
          type: 'NUMBER',
        },
      ]);
    });

    it('introspects withQuery (with star)', async () => {
      const external = await External.fromJS(
        {
          engine: 'druidsql',
          source: 'wikipedia',
          withQuery: `SELECT page || 'lol' AS pageLol, added + 1, * FROM wikipedia`,
          context,
        },
        druidRequester,
      ).introspect();

      expect(external.version).to.equal(info.druidVersion);
      expect(external.toJS().attributes.slice(0, 5)).to.deep.equal([
        {
          name: 'pageLol',
          nativeType: 'STRING',
          type: 'STRING',
        },
        {
          name: '__time',
          nativeType: 'TIMESTAMP',
          type: 'TIME',
        },
        {
          name: 'added',
          nativeType: 'LONG',
          type: 'NUMBER',
        },
        {
          name: 'channel',
          nativeType: 'STRING',
          type: 'STRING',
        },
        {
          name: 'cityName',
          nativeType: 'STRING',
          type: 'STRING',
        },
      ]);
    });

    it('introspects withQuery (with join)', async () => {
      const external = await External.fromJS(
        {
          engine: 'druidsql',
          source: 'wikipedia',
          withQuery: sane`
            SELECT
              __time,
              added,
              channel,
              lookup."channel-lookup".v AS "channelName",
              "user",
              user_hll,
              user_theta,
              user_unique
            FROM wikipedia
            LEFT JOIN lookup."channel-lookup" ON lookup."channel-lookup".k = wikipedia.channel
          `,
          context,
        },
        druidRequester,
      ).introspect();

      expect(external.version).to.equal(info.druidVersion);
      expect(external.toJS().attributes).to.deep.equal([
        {
          name: '__time',
          nativeType: 'TIMESTAMP',
          type: 'TIME',
        },
        {
          name: 'added',
          nativeType: 'LONG',
          type: 'NUMBER',
        },
        {
          name: 'channel',
          nativeType: 'STRING',
          type: 'STRING',
        },
        {
          name: 'channelName',
          nativeType: 'STRING',
          type: 'STRING',
        },
        {
          name: 'user',
          nativeType: 'STRING',
          type: 'STRING',
        },
        {
          name: 'user_hll',
          nativeType: 'COMPLEX',
          type: 'NULL',
        },
        {
          name: 'user_theta',
          nativeType: 'COMPLEX',
          type: 'NULL',
        },
        {
          name: 'user_unique',
          nativeType: 'COMPLEX',
          type: 'NULL',
        },
      ]);
    });

    it('works with introspected uniques', () => {
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
            Diff_Users_1_3: 1102,
            Diff_Users_2_3: -452,
            Diff_Users_3_4: -39,
            UniqueIsRobot: 2,
            UniquePages1: 279107,
            UniquePages2: 281588,
            UniqueUserChars: 1376,
            UniqueUsers1: 39268,
            UniqueUsers2: 37713,
            UniqueUsers3: 38165,
            UniqueUsers4: 38205,
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
            commentLength95: 145.46353149414062,
            commentLengthMedian: 28.10873794555664,
            deltaBucket95: -500,
            deltaBucketMedian: -500,
            deltaHist95: 161.95516967773438,
            deltaHistMedian: 129.01910400390625,
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
          $('wiki')
            .filter($('channel').is('en'))
            .quantile('$delta_quantilesDoublesSketch', 0.98),
        )
        .apply(
          'DeltaDq98thDe',
          $('wiki')
            .filter($('channel').is('de'))
            .quantile('$delta_quantilesDoublesSketch', 0.98),
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

    it.skip('works with introspection', () => {
      // ToDo: needs null check correction
      let ex = ply()
        .apply('wiki', $('wiki').filter($('channel').is('en')))
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply(
          'Time',
          $('wiki')
            .split($('__time').timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
            .apply('TotalAdded', '$wiki.sum($added)')
            .sort('$Timestamp', 'ascending')
            .limit(3)
            .apply(
              'Pages',
              $('wiki')
                .split('$regionName', 'RegionName')
                .apply('Deleted', '$wiki.sum($deleted)')
                .sort('$Deleted', 'descending')
                .limit(2),
            ),
        );

      return basicExecutor(ex).then(result => {
        expect(result.toJS().data).to.deep.equal([
          {
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
                        name: 'RegionName',
                        type: 'STRING',
                      },
                      {
                        name: 'Deleted',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Deleted: 11807,
                        RegionName: null,
                      },
                      {
                        Deleted: 848,
                        RegionName: 'Ontario',
                      },
                    ],
                    keys: ['RegionName'],
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
                        name: 'RegionName',
                        type: 'STRING',
                      },
                      {
                        name: 'Deleted',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Deleted: 109934,
                        RegionName: null,
                      },
                      {
                        Deleted: 474,
                        RegionName: 'Indiana',
                      },
                    ],
                    keys: ['RegionName'],
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
                        name: 'RegionName',
                        type: 'STRING',
                      },
                      {
                        name: 'Deleted',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        Deleted: 124999,
                        RegionName: null,
                      },
                      {
                        Deleted: 449,
                        RegionName: 'Georgia',
                      },
                    ],
                    keys: ['RegionName'],
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
});
