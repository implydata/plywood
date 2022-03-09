/*
 * Copyright 2017-2020 Imply Data, Inc.
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
const { PassThrough } = require('readable-stream');

const { sane } = require('../utils');

const plywood = require('../plywood');

const { External, TimeRange, $, ply, r, AttributeInfo } = plywood;

const timeFilter = $('time').overlap(
  TimeRange.fromJS({
    start: new Date('2013-02-26T00:00:00Z'),
    end: new Date('2013-02-27T00:00:00Z'),
  }),
);

const context = {
  wiki: External.fromJS({
    engine: 'druid',
    source: 'wikipedia',
    timeAttribute: 'time',
    attributes: [
      { name: 'time', type: 'NULL' },
      { name: 'sometimeLater', type: 'NULL' },
      { name: 'language', type: 'NULL' },
      { name: 'page', type: 'NULL' },
      { name: 'tags', type: 'NULL' },
      { name: 'commentLength', type: 'NULL' },
      { name: 'isRobot', type: 'NULL' },
      { name: 'count', type: 'NULL' },
      { name: 'added', type: 'NULL' },
      { name: 'deleted', type: 'NULL' },
      { name: 'inserted', type: 'NULL' },
      { name: 'delta_hist', type: 'NULL', nativeType: 'approximateHistogram' },
    ],
    derivedAttributes: {
      pageInBrackets: "'[' ++ $page ++ ']'",
      page3: '$page.substr(0, 3)',
    },
    filter: timeFilter,
    allowSelectQueries: true,
    version: '0.20.0',
    customAggregations: {
      crazy: {
        accessType: 'getSomeCrazy',
        aggregation: {
          type: 'crazy',
          the: 'borg will rise again',
          activate: false,
        },
      },
      stupid: {
        accessType: 'iAmWithStupid',
        aggregation: {
          type: 'stoopid',
          onePlusOne: 3,
          globalWarming: 'hoax',
        },
      },
    },
    customTransforms: {
      makeFrenchCanadian: {
        type: 'extraction',
        outputName: 'sometimeLater',
        extractionFn: {
          type: 'timeFormat',
          format: 'EEEE',
          timeZone: 'America/Montreal',
          locale: 'fr',
        },
      },
      makeExcited: {
        extractionFn: {
          type: 'javascript',
          function: "function(str) { return str + '!!!'; }",
        },
        injective: true,
      },
    },
  }),
};

describe('DruidExternal Null Type', () => {
  describe('simplifies / digests', () => {
    it('a (timeBoundary) total', () => {
      let ex = ply()
        .apply('maximumTime', '$wiki.max($time)')
        .apply('minimumTime', '$wiki.min($time)');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('literal');
      const druidExternal = ex.value.getReadyExternals()[0].external;

      expect(druidExternal.getQueryAndPostTransform().query).to.deep.equal({
        dataSource: 'wikipedia',
        queryType: 'timeBoundary',
      });
    });

    it('should properly process a simple value query', () => {
      let ex = $('wiki').filter($('language').is('en')).sum('$added');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;

      expect(druidExternal.getQueryAndPostTransform().query).to.deep.equal({
        aggregations: [
          {
            fieldName: 'added',
            name: '__VALUE__',
            type: 'doubleSum',
          },
        ],
        dataSource: 'wikipedia',
        filter: {
          dimension: 'language',
          type: 'selector',
          value: 'en',
        },
        granularity: 'all',
        intervals: '2013-02-26T00Z/2013-02-27T00Z',
        queryType: 'timeseries',
      });
    });

    it('should properly process a complex value query', () => {
      let ex = $('wiki')
        .filter($('language').is('en'))
        .sum('$added')
        .add($('wiki').sum('$deleted'));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;

      expect(druidExternal.getQueryAndPostTransform().query).to.deep.equal({
        aggregations: [
          {
            aggregator: {
              fieldName: 'added',
              name: '!T_0',
              type: 'doubleSum',
            },
            filter: {
              dimension: 'language',
              type: 'selector',
              value: 'en',
            },
            name: '!T_0',
            type: 'filtered',
          },
          {
            fieldName: 'deleted',
            name: '!T_1',
            type: 'doubleSum',
          },
        ],
        dataSource: 'wikipedia',
        granularity: 'all',
        intervals: '2013-02-26T00Z/2013-02-27T00Z',
        postAggregations: [
          {
            expression: '("!T_0"+"!T_1")',
            name: '__VALUE__',
            type: 'expression',
          },
        ],
        queryType: 'timeseries',
      });
    });

    it('should properly process a total', () => {
      let ex = ply()
        .apply(
          'wiki',
          $('wiki', 1).apply('addedTwice', '$added * 2').filter($('language').is('en')),
        )
        .apply('Count', '$wiki.count()')
        .apply('TotalAdded', '$wiki.sum($added)');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('literal');
      const druidExternal = ex.value.getReadyExternals()[0].external;

      expect(druidExternal.getQueryAndPostTransform().query).to.deep.equal({
        aggregations: [
          {
            name: 'Count',
            type: 'count',
          },
          {
            fieldName: 'added',
            name: 'TotalAdded',
            type: 'doubleSum',
          },
        ],
        dataSource: 'wikipedia',
        filter: {
          dimension: 'language',
          type: 'selector',
          value: 'en',
        },
        granularity: 'all',
        intervals: '2013-02-26T00Z/2013-02-27T00Z',
        queryType: 'timeseries',
      });
    });

    it('inlines a total with no explicit dataset apply', () => {
      let ex = ply()
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply('TotalAddedX2', '$TotalAdded * 2');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('literal');
      const druidExternal = ex.value.getReadyExternals()[0].external;

      const queryAndPostTransform = druidExternal.getQueryAndPostTransform();
      expect(queryAndPostTransform.query).to.deep.equal({
        aggregations: [
          {
            fieldName: 'added',
            name: 'TotalAdded',
            type: 'doubleSum',
          },
        ],
        dataSource: 'wikipedia',
        granularity: 'all',
        intervals: '2013-02-26T00Z/2013-02-27T00Z',
        postAggregations: [
          {
            expression: '("TotalAdded"*2)',
            name: 'TotalAddedX2',
            type: 'expression',
          },
        ],
        queryType: 'timeseries',
      });
    });

    it('processes a simple split', () => {
      let ex = $('wiki')
        .split('$page', 'Page')
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5);

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query).to.deep.equal({
        aggregations: [
          {
            name: 'Count',
            type: 'count',
          },
          {
            fieldName: 'added',
            name: 'Added',
            type: 'doubleSum',
          },
        ],
        dataSource: 'wikipedia',
        dimension: {
          dimension: 'page',
          outputName: 'Page',
          type: 'default',
        },
        granularity: 'all',
        intervals: '2013-02-26T00Z/2013-02-27T00Z',
        metric: 'Count',
        queryType: 'topN',
        threshold: 5,
      });
    });

    it('processes a split with custom aggregations', () => {
      let ex = $('wiki')
        .split('$page', 'Page')
        .apply('CrazyStupid', '$wiki.customAggregate(crazy) * $wiki.customAggregate(stupid)')
        .apply('CrazyStupidBackCompat', '$wiki.custom(crazy) * $wiki.custom(stupid)')
        .sort('$CrazyStupid', 'descending')
        .limit(5);

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query).to.deep.equal({
        aggregations: [
          {
            activate: false,
            name: '!T_0',
            the: 'borg will rise again',
            type: 'crazy',
          },
          {
            globalWarming: 'hoax',
            name: '!T_1',
            onePlusOne: 3,
            type: 'stoopid',
          },
        ],
        dataSource: 'wikipedia',
        dimension: {
          dimension: 'page',
          outputName: 'Page',
          type: 'default',
        },
        granularity: 'all',
        intervals: '2013-02-26T00Z/2013-02-27T00Z',
        metric: 'CrazyStupid',
        postAggregations: [
          {
            expression: '("!T_0"*"!T_1")',
            name: 'CrazyStupid',
            type: 'expression',
          },
          {
            expression: '("!T_0"*"!T_1")',
            name: 'CrazyStupidBackCompat',
            type: 'expression',
          },
        ],
        queryType: 'topN',
        threshold: 5,
      });
    });

    it('works with complex aggregate expressions', () => {
      let ex = ply()
        .apply('SumAbs', '$wiki.sum($added.absolute())')
        .apply('SumComplex', '$wiki.sum($added.power(2) * $deleted / $added.absolute())');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('literal');
      const druidExternal = ex.value.getReadyExternals()[0].external;

      expect(druidExternal.getQueryAndPostTransform().query.aggregations).to.deep.equal([
        {
          expression: 'abs("added")',
          name: 'SumAbs',
          type: 'doubleSum',
        },
        {
          expression:
            'if(abs("added")!=0,(cast((pow("added",2)*"deleted"),\'DOUBLE\')/abs("added")),null)',
          name: 'SumComplex',
          type: 'doubleSum',
        },
      ]);
    });

    it('works with filtered complex aggregate expressions', () => {
      let ex = $('wiki')
        .split('$page', 'Page')
        .apply('FilteredSumDeleted', '$wiki.filter($page.contains("wikipedia")).sum($deleted)')
        .apply('Filtered2', '$wiki.filter($page.match("^wiki")).sum($deleted)')
        .sort('$FilteredSumDeleted', 'descending')
        .limit(5);

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;

      expect(druidExternal.getQueryAndPostTransform().query).to.deep.equal({
        aggregations: [
          {
            aggregator: {
              fieldName: 'deleted',
              name: 'FilteredSumDeleted',
              type: 'doubleSum',
            },
            filter: {
              dimension: 'page',
              query: {
                caseSensitive: true,
                type: 'contains',
                value: 'wikipedia',
              },
              type: 'search',
            },
            name: 'FilteredSumDeleted',
            type: 'filtered',
          },
          {
            aggregator: {
              fieldName: 'deleted',
              name: 'Filtered2',
              type: 'doubleSum',
            },
            filter: {
              dimension: 'page',
              pattern: '^wiki',
              type: 'regex',
            },
            name: 'Filtered2',
            type: 'filtered',
          },
        ],
        dataSource: 'wikipedia',
        dimension: {
          dimension: 'page',
          outputName: 'Page',
          type: 'default',
        },
        granularity: 'all',
        intervals: '2013-02-26T00Z/2013-02-27T00Z',
        metric: 'FilteredSumDeleted',
        queryType: 'topN',
        threshold: 5,
      });
    });

    it('works in simple cases with power and absolute', () => {
      let ex = $('wiki')
        .split('$page', 'Page')
        .apply('Count', '$wiki.count()')
        .apply('Abs', '$wiki.sum($added).absolute()')
        .apply('Abs2', '$wiki.sum($added).power(2)')
        .sort('$Abs', 'descending')
        .limit(5);

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query).to.deep.equal({
        aggregations: [
          {
            name: 'Count',
            type: 'count',
          },
          {
            fieldName: 'added',
            name: '!T_0',
            type: 'doubleSum',
          },
        ],
        dataSource: 'wikipedia',
        dimension: {
          dimension: 'page',
          outputName: 'Page',
          type: 'default',
        },
        granularity: 'all',
        intervals: '2013-02-26T00Z/2013-02-27T00Z',
        metric: 'Abs',
        postAggregations: [
          {
            expression: 'abs("!T_0")',
            name: 'Abs',
            type: 'expression',
          },
          {
            expression: 'pow("!T_0",2)',
            name: 'Abs2',
            type: 'expression',
          },
        ],
        queryType: 'topN',
        threshold: 5,
      });
    });

    it('works with complex absolute and power expressions', () => {
      let ex = $('wiki')
        .split('$page', 'Page')
        .apply('Count', '$wiki.count()')
        .apply(
          'Abs',
          '(($wiki.sum($added)/$wiki.count().absolute().power(0.5) + 100 * $wiki.countDistinct($page)).absolute()).power(2) + $wiki.custom(crazy)',
        )
        .sort('$Count', 'descending')
        .limit(5);

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query).to.deep.equal({
        aggregations: [
          {
            name: 'Count',
            type: 'count',
          },
          {
            fieldName: 'added',
            name: '!T_0',
            type: 'doubleSum',
          },
          {
            fields: ['page'],
            name: '!T_1',
            round: true,
            type: 'cardinality',
          },
          {
            activate: false,
            name: '!T_2',
            the: 'borg will rise again',
            type: 'crazy',
          },
        ],
        dataSource: 'wikipedia',
        dimension: {
          dimension: 'page',
          outputName: 'Page',
          type: 'default',
        },
        granularity: 'all',
        intervals: '2013-02-26T00Z/2013-02-27T00Z',
        metric: 'Count',
        postAggregations: [
          {
            expression:
              '(pow(abs((if(pow(abs("Count"),0.5)!=0,(cast("!T_0",\'DOUBLE\')/pow(abs("Count"),0.5)),null)+("!T_1"*100))),2)+"!T_2")',
            name: 'Abs',
            type: 'expression',
          },
        ],
        queryType: 'topN',
        threshold: 5,
      });
    });

    it('works in simple cases with string comparisons', () => {
      let ex = $('wiki').filter("$page < 'moon'", 'Page').limit(5);

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'page',
        type: 'bound',
        upper: 'moon',
        upperStrict: true,
      });
    });

    it.skip('should work with error bound calculation', () => {
      let ex = ply().apply(
        'DistPagesWithinLimits',
        '($wiki.countDistinct($page) - 279893).absolute() < 10',
      );

      ex = ex.referenceCheck(context).resolve(context).simplify();

      console.log('ex.toString()', ex.toString());

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query).to.deep.equal({});
    });
  });

  describe('filters', () => {
    it('works with ref filter', () => {
      let ex = $('wiki').filter($('isRobot'));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'isRobot',
        type: 'selector',
        value: true,
      });
    });

    it('works with ref.not() filter', () => {
      let ex = $('wiki').filter($('isRobot').not());

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        field: {
          dimension: 'isRobot',
          type: 'selector',
          value: true,
        },
        type: 'not',
      });
    });

    it('works with .in(1 thing)', () => {
      let ex = $('wiki').filter($('language').in(['en']));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'language',
        type: 'selector',
        value: 'en',
      });
    });

    it('works with .in(3 things)', () => {
      let ex = $('wiki').filter($('language').in(['en', 'es', 'fr']));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'language',
        type: 'in',
        values: ['en', 'es', 'fr'],
      });
    });

    it('works with .in([null])', () => {
      let ex = $('wiki').filter($('language').in([null]));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'language',
        type: 'selector',
        value: null,
      });
    });

    it('works with .lookup().in(3 things)', () => {
      let ex = $('wiki').filter($('language').lookup('language_lookup').in(['en', 'es', 'fr']));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'language',
        extractionFn: {
          lookup: 'language_lookup',
          type: 'registeredLookup',
        },
        type: 'in',
        values: ['en', 'es', 'fr'],
      });
    });

    it('works with .overlap([null])', () => {
      let ex = $('wiki').filter($('language').overlap([null]));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'language',
        type: 'selector',
        value: null,
      });
    });

    it('works with .lookup().overlap(blah, null) (on SET/STRING)', () => {
      let ex = $('wiki').filter($('tags').lookup('tag_lookup').overlap(['Good', null]));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'tags',
        extractionFn: {
          lookup: 'tag_lookup',
          type: 'registeredLookup',
        },
        type: 'in',
        values: ['Good', null],
      });
    });

    it('works with .extract().overlap(blah, null) (on SET/STRING)', () => {
      let ex = $('wiki').filter($('tags').extract('[0-9]+').overlap(['Good', null]));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'tags',
        extractionFn: {
          expr: '[0-9]+',
          replaceMissingValue: true,
          type: 'regex',
        },
        type: 'in',
        values: ['Good', null],
      });
    });

    it('works with .substr().overlap(blah, null) (on SET/STRING)', () => {
      let ex = $('wiki').filter($('tags').substr(1, 3).overlap(['Good', null]));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'tags',
        extractionFn: {
          index: 1,
          length: 3,
          type: 'substring',
        },
        type: 'in',
        values: ['Good', null],
      });
    });

    it('works with .overlap(NUMBER_RANGE)', () => {
      let ex = $('wiki').filter($('commentLength').overlap(10, 30));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        ordering: 'numeric',
        dimension: 'commentLength',
        lower: 10,
        type: 'bound',
        upper: 30,
        upperStrict: true,
      });
    });

    it('works with .in(SET/NUMBER)', () => {
      let ex = $('wiki').filter($('commentLength').in([10, 30]));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'commentLength',
        type: 'in',
        values: [10, 30],
      });
    });

    it('works with .contains()', () => {
      let ex = $('wiki').filter($('language').contains('en', 'ignoreCase'));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'language',
        query: {
          caseSensitive: false,
          type: 'contains',
          value: 'en',
        },
        type: 'search',
      });
    });

    it('works with SET/STRING.contains()', () => {
      let ex = $('wiki').filter($('tags').contains('good', 'ignoreCase'));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'tags',
        query: {
          caseSensitive: false,
          type: 'contains',
          value: 'good',
        },
        type: 'search',
      });
    });

    it('works with .lookup().contains()', () => {
      let ex = $('wiki').filter(
        $('language').lookup('language_lookup').contains('eN', 'ignoreCase'),
      );

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'language',
        extractionFn: {
          lookup: 'language_lookup',
          type: 'registeredLookup',
        },
        query: {
          caseSensitive: false,
          type: 'contains',
          value: 'eN',
        },
        type: 'search',
      });
    });

    it('works with .lookup().contains().not()', () => {
      let ex = $('wiki').filter(
        $('language').lookup('language_lookup').contains('eN', 'ignoreCase').not(),
      );

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        field: {
          dimension: 'language',
          extractionFn: {
            lookup: 'language_lookup',
            type: 'registeredLookup',
          },
          query: {
            caseSensitive: false,
            type: 'contains',
            value: 'eN',
          },
          type: 'search',
        },
        type: 'not',
      });
    });

    it('works with .concat().concat().contains()', () => {
      let ex = $('wiki').filter("('[' ++ $language ++ ']').contains('eN', 'ignoreCase')");

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'language',
        extractionFn: {
          format: '[%s]',
          nullHandling: 'returnNull',
          type: 'stringFormat',
        },
        query: {
          caseSensitive: false,
          type: 'contains',
          value: 'eN',
        },
        type: 'search',
      });
    });

    it('works with .match()', () => {
      let ex = $('wiki').filter($('language').match('en+'));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'language',
        pattern: 'en+',
        type: 'regex',
      });
    });

    it('works with SET/STRING.match()', () => {
      let ex = $('wiki').filter($('tags').match('goo+d'));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'tags',
        pattern: 'goo+d',
        type: 'regex',
      });
    });

    it('works with .timePart().in()', () => {
      let ex = $('wiki').filter($('time').timePart('HOUR_OF_DAY').is([3, 5]));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: '__time',
        extractionFn: {
          format: 'H',
          locale: 'en-US',
          timeZone: 'Etc/UTC',
          type: 'timeFormat',
        },
        type: 'in',
        values: [3, 5],
      });
    });

    it('works with derived .in()', () => {
      let ex = $('wiki').filter('$pageInBrackets == "[wiki]"');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          format: '[%s]',
          nullHandling: 'returnNull',
          type: 'stringFormat',
        },
        type: 'selector',
        value: '[wiki]',
      });
    });

    it('works with dynamic derived .in()', () => {
      let ex = $('wiki').apply('page3', '$page.substr(0, 3)').filter('$page3 == wik');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostTransform().query.filter).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          index: 0,
          length: 3,
          type: 'substring',
        },
        type: 'selector',
        value: 'wik',
      });
    });
  });

  describe('splits (makes correct dimension extractionFns)', () => {
    it('works with default', () => {
      let ex = $('wiki').split('$page', 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        outputName: 'Split',
        type: 'default',
      });
    });

    it('works with simple STRING', () => {
      let ex = $('wiki').split('$page', 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        outputName: 'Split',
        type: 'default',
      });
    });

    it('works with dynamic derived column STRING', () => {
      let ex = $('wiki').apply('page3', '$page.substr(0, 3)').split('$page3', 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          index: 0,
          length: 3,
          type: 'substring',
        },
        outputName: 'Split',
        type: 'extraction',
      });
    });

    it('works with .concat()', () => {
      let ex = $('wiki').split('"[%]" ++ $page ++ "[%]"', 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          format: '[\\%]%s[\\%]',
          nullHandling: 'returnNull',
          type: 'stringFormat',
        },
        outputName: 'Split',
        type: 'extraction',
      });
    });

    it('works with SET/STRING.concat()', () => {
      let ex = $('wiki').split('"[%]" ++ $page ++ "[%]"', 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          format: '[\\%]%s[\\%]',
          nullHandling: 'returnNull',
          type: 'stringFormat',
        },
        outputName: 'Split',
        type: 'extraction',
      });
    });

    it('works with .substr()', () => {
      let ex = $('wiki').split('$page.substr(3, 5)', 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          type: 'substring',
          index: 3,
          length: 5,
        },
        outputName: 'Split',
        type: 'extraction',
      });
    });

    it('works with .substr().extract()', () => {
      let ex = $('wiki').split('$page.substr(3, 5).extract("\\d+")', 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          extractionFns: [
            {
              index: 3,
              length: 5,
              type: 'substring',
            },
            {
              expr: '\\d+',
              replaceMissingValue: true,
              type: 'regex',
            },
          ],
          type: 'cascade',
        },
        outputName: 'Split',
        type: 'extraction',
      });
    });

    it('works with .extract() (no fallback)', () => {
      let ex = $('wiki').split($('page').extract('^Cat(.+)$'), 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          type: 'regex',
          expr: '^Cat(.+)$',
          replaceMissingValue: true,
        },
        outputName: 'Split',
        type: 'extraction',
      });
    });

    it('works with .extract() with custom .fallback()', () => {
      let ex = $('wiki').split($('page').extract('^Cat(.+)$').fallback('noMatch'), 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          type: 'regex',
          expr: '^Cat(.+)$',
          replaceMissingValue: true,
          replaceMissingValueWith: 'noMatch',
        },
        outputName: 'Split',
        type: 'extraction',
      });
    });

    it('works with .extract() with self .fallback()', () => {
      let ex = $('wiki').split($('page').extract('^Cat(.+)$').fallback('$page'), 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          type: 'regex',
          expr: '^Cat(.+)$',
        },
        outputName: 'Split',
        type: 'extraction',
      });
    });

    it('works with .lookup() (no fallback)', () => {
      let ex = $('wiki').split($('page').lookup('wikipedia-page-lookup'), 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          lookup: 'wikipedia-page-lookup',
          type: 'registeredLookup',
        },
        outputName: 'Split',
        type: 'extraction',
      });
    });

    it('works with .lookup() with custom .fallback()', () => {
      let ex = $('wiki').split(
        $('page').lookup('wikipedia-page-lookup').fallback('missing'),
        'Split',
      );

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          lookup: 'wikipedia-page-lookup',
          replaceMissingValueWith: 'missing',
          type: 'registeredLookup',
        },
        outputName: 'Split',
        type: 'extraction',
      });
    });

    it('works with .lookup() with self .fallback()', () => {
      let ex = $('wiki').split(
        $('page').lookup('wikipedia-page-lookup').fallback('$page'),
        'Split',
      );

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          lookup: 'wikipedia-page-lookup',
          retainMissingValue: true,
          type: 'registeredLookup',
        },
        outputName: 'Split',
        type: 'extraction',
      });
    });

    it('works with .lookup().fallback().extract()', () => {
      let ex = $('wiki').split(
        $('page').lookup('wikipedia-page-lookup').fallback('$page').extract('\\d+'),
        'Split',
      );

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          extractionFns: [
            {
              lookup: 'wikipedia-page-lookup',
              retainMissingValue: true,
              type: 'registeredLookup',
            },
            {
              expr: '\\d+',
              replaceMissingValue: true,
              type: 'regex',
            },
          ],
          type: 'cascade',
        },
        outputName: 'Split',
        type: 'extraction',
      });
    });

    it('works with .lookup().fallback().contains()', () => {
      let ex = $('wiki').split(
        $('page').lookup('wikipedia-page-lookup').fallback('$page').contains('lol'),
        'Split',
      );

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('topN');
      expect(query.virtualColumns).to.deep.equal([
        {
          expression: "like(nvl(lookup(\"page\",'wikipedia-page-lookup'),\"page\"),'%lol%','~')",
          name: 'v:Split',
          outputType: 'STRING',
          type: 'expression',
        },
      ]);
      expect(query.dimension).to.deep.equal({
        dimension: 'v:Split',
        outputName: 'Split',
        outputType: 'STRING',
        type: 'default',
      });
    });

    it('works with SET/STRING.lookup()', () => {
      let ex = $('wiki').split($('tags').lookup('tag-lookup'), 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'tags',
        extractionFn: {
          lookup: 'tag-lookup',
          type: 'registeredLookup',
        },
        outputName: 'Split',
        type: 'extraction',
      });
    });

    it('works with SET/STRING.lookup().contains()', () => {
      let ex = $('wiki').split(
        $('tags').lookup('tag-lookup').contains('lol', 'ignoreCase'),
        'Split',
      );

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('topN');
      expect(query.virtualColumns).to.deep.equal([
        {
          expression: "like(lower(lookup(\"tags\",'tag-lookup')),'%lol%','~')",
          name: 'v:Split',
          outputType: 'STRING',
          type: 'expression',
        },
      ]);
      expect(query.dimension).to.deep.equal({
        dimension: 'v:Split',
        outputName: 'Split',
        outputType: 'STRING',
        type: 'default',
      });
    });

    it('works with .power()', () => {
      let ex = $('wiki').split('$commentLength.power(2)', 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query).to.deep.equal({
        dataSource: 'wikipedia',
        dimensions: [
          {
            dimension: 'v:Split',
            outputName: 'Split',
            outputType: 'DOUBLE',
            type: 'default',
          },
        ],
        granularity: 'all',
        intervals: '2013-02-26T00Z/2013-02-27T00Z',
        queryType: 'groupBy',
        virtualColumns: [
          {
            expression: 'pow("commentLength",2)',
            name: 'v:Split',
            outputType: 'DOUBLE',
            type: 'expression',
          },
        ],
      });
    });

    it('works with .numberBucket()', () => {
      let ex = $('wiki').split('$commentLength.numberBucket(10, 1)', 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'commentLength',
        extractionFn: {
          size: 10,
          offset: 1,
          type: 'bucket',
        },
        outputName: 'Split',
        type: 'extraction',
      });
    });

    it('works with .timeBucket()', () => {
      let ex = $('wiki').split({
        Split1: '$time.timeBucket(P1D)',
        Split2: '$sometimeLater.timeBucket(P1D)',
      });

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions).to.deep.equal([
        {
          dimension: '__time',
          extractionFn: {
            format: "yyyy-MM-dd'T'HH:mm:ss'Z",
            granularity: {
              period: 'P1D',
              timeZone: 'Etc/UTC',
              type: 'period',
            },
            timeZone: 'Etc/UTC',
            type: 'timeFormat',
          },
          outputName: 'Split1',
          type: 'extraction',
        },
        {
          dimension: 'sometimeLater',
          extractionFn: {
            format: "yyyy-MM-dd'T'HH:mm:ss'Z",
            granularity: {
              period: 'P1D',
              timeZone: 'Etc/UTC',
              type: 'period',
            },
            timeZone: 'Etc/UTC',
            type: 'timeFormat',
          },
          outputName: 'Split2',
          type: 'extraction',
        },
      ]);
    });

    it('works with .timePart()', () => {
      let ex = $('wiki').split({
        Split1: "$time.timePart(DAY_OF_WEEK, 'Etc/UTC')",
        Split2: "$sometimeLater.timePart(DAY_OF_WEEK, 'Etc/UTC')",
      });

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: '__time',
        extractionFn: {
          format: 'e',
          locale: 'en-US',
          timeZone: 'Etc/UTC',
          type: 'timeFormat',
        },
        outputName: 'Split1',
        outputType: 'LONG',
        type: 'extraction',
      });
    });

    it('works with derived attr split', () => {
      let ex = $('wiki').split('$page3', 'P3');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        dimension: 'page',
        extractionFn: {
          index: 0,
          length: 3,
          type: 'substring',
        },
        outputName: 'P3',
        type: 'extraction',
      });
    });

    it('works with custom transform split with time format extraction fn', () => {
      let ex = $('wiki').split(
        $('time').customTransform('makeFrenchCanadian').cast('STRING'),
        'FrenchCanadian',
      );

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query).to.deep.equal({
        dataSource: 'wikipedia',
        dimensions: [
          {
            dimension: '__time',
            extractionFn: {
              format: 'EEEE',
              locale: 'fr',
              timeZone: 'America/Montreal',
              type: 'timeFormat',
            },
            outputName: 'FrenchCanadian',
            type: 'extraction',
          },
        ],
        granularity: 'all',
        intervals: '2013-02-26T00Z/2013-02-27T00Z',
        queryType: 'groupBy',
      });
    });

    it('works with custom transform split with javascript extraction fn', () => {
      let ex = $('wiki').split($('time').customTransform('makeExcited'), 'Excited');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      const query = ex.external.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query).to.deep.equal({
        dataSource: 'wikipedia',
        dimensions: [
          {
            dimension: '__time',
            extractionFn: {
              function: "function(str) { return str + '!!!'; }",
              type: 'javascript',
            },
            outputName: 'Excited',
            type: 'extraction',
          },
        ],
        granularity: 'all',
        intervals: '2013-02-26T00Z/2013-02-27T00Z',
        queryType: 'groupBy',
      });
    });
  });

  describe('applies', () => {
    it('works with ref filtered agg', () => {
      let ex = ply()
        .apply('Count', $('wiki').sum('$count'))
        .apply('Test', $('wiki').filter('$isRobot').sum('$count'));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('literal');
      const druidExternal = ex.value.getReadyExternals()[0].external;

      const query = druidExternal.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('timeseries');
      expect(query.aggregations[1]).to.deep.equal({
        aggregator: {
          fieldName: 'count',
          name: 'Test',
          type: 'doubleSum',
        },
        filter: {
          dimension: 'isRobot',
          type: 'selector',
          value: true,
        },
        name: 'Test',
        type: 'filtered',
      });
    });

    it('works with quantile agg', () => {
      let ex = ply()
        .apply('P95', $('wiki').quantile('$delta_hist', 0.95))
        .apply('P99by2', $('wiki').quantile('$delta_hist', 0.99).divide(2));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('literal');
      const druidExternal = ex.value.getReadyExternals()[0].external;

      const query = druidExternal.getQueryAndPostTransform().query;
      expect(query.queryType).to.equal('timeseries');
      expect(query.aggregations).to.deep.equal([
        {
          fieldName: 'delta_hist',
          name: '!H_P95',
          type: 'approxHistogramFold',
        },
        {
          fieldName: 'delta_hist',
          name: '!H_!T_0',
          type: 'approxHistogramFold',
        },
      ]);

      expect(query.postAggregations).to.deep.equal([
        {
          fieldName: '!H_P95',
          name: 'P95',
          probability: 0.95,
          type: 'quantile',
        },
        {
          fieldName: '!H_!T_0',
          name: '!T_0',
          probability: 0.99,
          type: 'quantile',
        },
        {
          expression: '(cast("!T_0",\'DOUBLE\')/2)',
          name: 'P99by2',
          type: 'expression',
        },
      ]);
    });
  });

  describe('should work when getting back no data', () => {
    const emptyExternal = External.fromJS(
      {
        engine: 'druid',
        source: 'wikipedia',
        timeAttribute: 'time',
        allowSelectQueries: true,
        attributes: [
          { name: 'time', type: 'TIME' },
          { name: 'language', type: 'STRING' },
          { name: 'page', type: 'STRING' },
          { name: 'added', type: 'NUMBER' },
        ],
        filter: timeFilter,
      },
      () => {
        const stream = new PassThrough({ objectMode: true });
        setTimeout(() => {
          stream.end();
        }, 1);
        return stream;
      },
    );

    it('should return null correctly on a totals query', () => {
      const ex = ply().apply('Count', '$wiki.count()');

      return ex.compute({ wiki: emptyExternal }).then(result => {
        expect(result.toJS().data).to.deep.equal([{ Count: 0 }]);
      });
    });

    it('should return null correctly on a timeseries query', () => {
      const ex = $('wiki')
        .split("$time.timeBucket(P1D, 'Etc/UTC')", 'Time')
        .apply('Count', '$wiki.count()')
        .sort('$Time', 'ascending');

      return ex.compute({ wiki: emptyExternal }).then(result => {
        expect(result.toJS().data).to.deep.equal([]);
      });
    });

    it('should return null correctly on a topN query', () => {
      const ex = $('wiki')
        .split('$page', 'Page')
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5);

      return ex.compute({ wiki: emptyExternal }).then(result => {
        expect(result.toJS().data).to.deep.equal([]);
      });
    });

    it('should return null correctly on a select query', () => {
      const ex = $('wiki');

      return ex.compute({ wiki: emptyExternal }).then(result => {
        expect(AttributeInfo.toJSs(result.attributes)).to.deep.equal([
          { name: 'time', type: 'TIME' },
          { name: 'language', type: 'STRING' },
          { name: 'page', type: 'STRING' },
          { name: 'added', type: 'NUMBER' },
        ]);

        expect(result.toJS().data).to.deep.equal([]);
        expect(result.toCSV()).to.equal('time,language,page,added');
      });
    });
  });
});
