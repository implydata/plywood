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

const plywood = require('../plywood');

const { Expression, External, Dataset, TimeRange, $, ply, r, s$ } = plywood;

const attributes = [
  { name: 'time', type: 'TIME' },
  { name: 'some_other_time', type: 'TIME' },
  { name: 'some_other_time_long', type: 'TIME', nativeType: 'LONG' },
  { name: 'color', type: 'STRING' },
  { name: 'cut', type: 'STRING' },
  { name: 'isNice', type: 'BOOLEAN' },
  { name: 'tags', type: 'SET/STRING' },
  { name: 'pugs', type: 'SET/STRING' },
  { name: 'carat', type: 'NUMBER', nativeType: 'STRING' },
  { name: 'carat_n', nativeType: 'STRING' },
  { name: 'height_bucket', type: 'NUMBER' },
  { name: 'price', type: 'NUMBER', unsplitable: true },
  { name: 'tax', type: 'NUMBER', unsplitable: true },
  { name: 'vendor_id', type: 'NULL', nativeType: 'hyperUnique', unsplitable: true },

  { name: 'try', type: 'NUMBER', nativeType: 'STRING' }, // Added here because 'try' is a JS keyword
  { name: 'a+b', type: 'NUMBER', nativeType: 'STRING' }, // Added here because it is invalid JS without escaping
];

describe('simulate DruidSql', () => {
  it('works in basic case', () => {
    const ex = ply()
      .apply('diamonds', $('diamonds').filter('$tags.overlap(["tagA", "tagB"])'))
      .apply(
        'Tags',
        $('diamonds')
          .split('$tags', 'Tag')
          .sort('$Tag', 'descending')
          .limit(10)
          .apply(
            'Cuts',
            $('diamonds')
              .split('$cut', 'Cut')
              .apply('Count', $('diamonds').count())
              .sort('$Count', 'descending')
              .limit(10),
          ),
      );

    const queryPlan = ex.simulateQueryPlan({
      diamonds: External.fromJS({
        engine: 'druidsql',
        version: '0.20.0',
        source: 'diamonds',
        timeAttribute: 'time',
        attributes,
        allowSelectQueries: true,
        filter: $('time').overlap({
          start: new Date('2015-03-12T00:00:00Z'),
          end: new Date('2015-03-19T00:00:00Z'),
        }),
      }),
    });
    expect(queryPlan.length).to.equal(2);
    expect(queryPlan).to.deep.equal([
      [
        {
          query:
            'SELECT\n"tags" AS "Tag"\nFROM "diamonds" AS t\nWHERE ((TIMESTAMP \'2015-03-12 00:00:00\'<="time" AND "time"<TIMESTAMP \'2015-03-19 00:00:00\') AND "tags" IN (\'tagA\',\'tagB\'))\nGROUP BY 1\nORDER BY "Tag" DESC\nLIMIT 10',
        },
      ],
      [
        {
          query:
            'SELECT\n"cut" AS "Cut",\nCOUNT(*) AS "Count"\nFROM "diamonds" AS t\nWHERE (((TIMESTAMP \'2015-03-12 00:00:00\'<="time" AND "time"<TIMESTAMP \'2015-03-19 00:00:00\') AND "tags" IN (\'tagA\',\'tagB\')) AND ("tags"=\'some_tags\'))\nGROUP BY 1\nORDER BY "Count" DESC\nLIMIT 10',
        },
      ],
    ]);
  });

  it('works with . in the datasource', () => {
    const ex = ply()
      .apply('diamonds', $('diamonds').filter('$tags.overlap(["tagA", "tagB"])'))
      .apply('Tags', $('diamonds').split('$tags', 'Tag').sort('$Tag', 'descending').limit(10));

    const queryPlan = ex.simulateQueryPlan({
      diamonds: External.fromJS({
        engine: 'druidsql',
        version: '0.20.0',
        source: 'dia.monds',
        timeAttribute: 'time',
        attributes,
        allowSelectQueries: true,
        filter: $('time').overlap({
          start: new Date('2015-03-12T00:00:00Z'),
          end: new Date('2015-03-19T00:00:00Z'),
        }),
      }),
    });
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan).to.deep.equal([
      [
        {
          query:
            'SELECT\n"tags" AS "Tag"\nFROM "dia.monds" AS t\nWHERE ((TIMESTAMP \'2015-03-12 00:00:00\'<="time" AND "time"<TIMESTAMP \'2015-03-19 00:00:00\') AND "tags" IN (\'tagA\',\'tagB\'))\nGROUP BY 1\nORDER BY "Tag" DESC\nLIMIT 10',
        },
      ],
    ]);
  });

  it('works with sqlRefExpression', () => {
    const ex = ply().apply(
      'Tags',
      $('diamonds')
        .split(s$('t.tags'), 'Tag')
        .apply('count', $('diamonds').count())
        .sort('$count', 'descending')
        .limit(10)
        .select('Tag', 'count'),
    );

    const queryPlan = ex.simulateQueryPlan({
      diamonds: External.fromJS({
        engine: 'druidsql',
        version: '0.20.0',
        source: 'diamonds',
        timeAttribute: 'time',
        attributes,
        allowSelectQueries: true,
        mode: 'raw',
        filter: $('time').overlap({
          start: new Date('2015-03-12T00:00:00Z'),
          end: new Date('2015-03-19T00:00:00Z'),
        }),
      }),
    });
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan).to.deep.equal([
      [
        {
          query:
            'SELECT\n(t.tags) AS "Tag",\nCOUNT(*) AS "count"\nFROM "diamonds" AS t\nWHERE (TIMESTAMP \'2015-03-12 00:00:00\'<="time" AND "time"<TIMESTAMP \'2015-03-19 00:00:00\')\nGROUP BY 1\nORDER BY "count" DESC\nLIMIT 10',
        },
      ],
    ]);
  });

  it('works with mvOverlapExpression', () => {
    const ex = ply()
      .apply('diamonds', $('diamonds'))
      .apply(
        'Tags',
        $('diamonds')
          .filter($('tags').mvOverlap(['tagA', 'tagB']))
          .split(s$('t.tags'), 'Tag')
          .apply('count', $('diamonds').count())
          .sort('$count', 'descending')
          .limit(10)
          .select('Tag', 'count'),
      );

    const queryPlan = ex.simulateQueryPlan({
      diamonds: External.fromJS({
        engine: 'druidsql',
        version: '0.20.0',
        source: 'diamonds',
        timeAttribute: 'time',
        attributes,
        allowSelectQueries: true,
        mode: 'raw',
      }),
    });

    expect(queryPlan.length).to.equal(1);
    expect(queryPlan).to.deep.equal([
      [
        {
          query:
            'SELECT\n(t.tags) AS "Tag",\nCOUNT(*) AS "count"\nFROM "diamonds" AS t\nWHERE MV_OVERLAP("tags", ARRAY[\'tagA\',\'tagB\'])\nGROUP BY 1\nORDER BY "count" DESC\nLIMIT 10',
        },
      ],
    ]);
  });

  it('works with mvContainsExpression', () => {
    const ex = ply()
      .apply('diamonds', $('diamonds').filter($('tags').mvContains(['tagA', 'tagB'])))
      .apply(
        'Tags',
        $('diamonds')
          .split(s$('t.tags'), 'Tag')
          .apply('count', $('diamonds').count())
          .sort('$count', 'descending')
          .limit(10)
          .select('Tag', 'count'),
      );

    const queryPlan = ex.simulateQueryPlan({
      diamonds: External.fromJS({
        engine: 'druidsql',
        version: '0.20.0',
        source: 'diamonds',
        timeAttribute: 'time',
        attributes,
        allowSelectQueries: true,
        mode: 'raw',
      }),
    });

    expect(queryPlan.length).to.equal(1);
    expect(queryPlan).to.deep.equal([
      [
        {
          query:
            'SELECT\n(t.tags) AS "Tag",\nCOUNT(*) AS "count"\nFROM "diamonds" AS t\nWHERE MV_CONTAINS("tags", ARRAY[\'tagA\',\'tagB\'])\nGROUP BY 1\nORDER BY "count" DESC\nLIMIT 10',
        },
      ],
    ]);
  });

  it('works with mvFilterOnly and mvOverlap', () => {
    const ex = ply()
      .apply('diamonds', $('diamonds'))
      .apply(
        'Tags',
        $('diamonds')
          .filter($('tags').mvOverlap(['tagA', 'tagB', 'tagC']))
          .split($('tags').mvFilterOnly(['tagA', 'tagB']), 'Tag')
          .sort('$Tag', 'descending'),
      );

    const queryPlan = ex.simulateQueryPlan({
      diamonds: External.fromJS({
        engine: 'druidsql',
        version: '0.20.0',
        source: 'diamonds',
        timeAttribute: 'time',
        attributes,
        allowSelectQueries: true,
        filter: $('time').overlap({
          start: new Date('2015-03-12T00:00:00Z'),
          end: new Date('2015-03-19T00:00:00Z'),
        }),
      }),
    });
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan).to.deep.equal([
      [
        {
          query:
            'SELECT\nMV_FILTER_ONLY("tags", ARRAY[\'tagA\',\'tagB\']) AS "Tag"\nFROM "diamonds" AS t\nWHERE ((TIMESTAMP \'2015-03-12 00:00:00\'<="time" AND "time"<TIMESTAMP \'2015-03-19 00:00:00\') AND MV_OVERLAP("tags", ARRAY[\'tagA\',\'tagB\',\'tagC\']))\nGROUP BY 1\nORDER BY "Tag" DESC',
        },
      ],
    ]);
  });
});
