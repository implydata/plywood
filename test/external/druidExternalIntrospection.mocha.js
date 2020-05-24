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
const { PassThrough } = require('readable-stream');

let plywood = require('../plywood');
let { Expression, External, TimeRange, $, ply, r } = plywood;

function promiseFnToStream(promiseRq) {
  return rq => {
    const stream = new PassThrough({ objectMode: true });

    promiseRq(rq).then(
      res => {
        if (res) stream.write(res);
        stream.end();
      },
      e => {
        stream.emit('error', e);
        stream.end();
      },
    );

    return stream;
  };
}

describe('DruidExternal Introspection', () => {
  let requesterFail = promiseFnToStream(({ query }) => {
    return Promise.reject(new Error('Bad status code'));
  });

  let requesterDruid_0_10_0 = promiseFnToStream(({ query }) => {
    if (query.queryType === 'status') return Promise.resolve({ version: '0.10.0' });
    expect(query.dataSource).to.equal('wikipedia');

    if (query.queryType === 'segmentMetadata') {
      expect(query.merge).to.equal(true);
      expect(query.analysisTypes).to.be.an('array');
      expect(query.lenientAggregatorMerge).to.equal(true);

      let merged = {
        id: 'merged',
        intervals: null,
        size: 0,
        numRows: 654321,
        columns: {
          __time: {
            type: 'LONG',
            hasMultipleValues: false,
            size: 0,
            cardinality: null,
            errorMessage: null,
          },
          added: {
            type: 'FLOAT',
            hasMultipleValues: false,
            size: 0,
            cardinality: null,
            errorMessage: null,
          },
          anonymous: {
            type: 'STRING',
            hasMultipleValues: false,
            size: 0,
            cardinality: 0,
            errorMessage: null,
          },
          count: {
            type: 'LONG',
            hasMultipleValues: false,
            size: 0,
            cardinality: null,
            errorMessage: null,
          },
          delta: {
            type: 'FLOAT',
            hasMultipleValues: false,
            size: 0,
            cardinality: null,
            errorMessage: null,
          },
          delta_hist: {
            type: 'approximateHistogram',
            hasMultipleValues: false,
            size: 0,
            cardinality: null,
            errorMessage: null,
          },
          language: {
            type: 'STRING',
            hasMultipleValues: false,
            size: 0,
            cardinality: 0,
            errorMessage: null,
          },
          namespace: {
            type: 'STRING',
            hasMultipleValues: true,
            size: 0,
            cardinality: 0,
            errorMessage: null,
          },
          newPage: {
            type: 'STRING',
            hasMultipleValues: false,
            size: 0,
            cardinality: 0,
            errorMessage: 'lol wtf',
          },
          newUser: {
            type: 'STRING',
            hasMultipleValues: false,
            size: -1,
            cardinality: 0,
            errorMessage: null,
          },
          page: {
            type: 'STRING',
            hasMultipleValues: false,
            size: 0,
            cardinality: 0,
            errorMessage: null,
          },
          time: {
            type: 'STRING',
            hasMultipleValues: false,
            size: 0,
            cardinality: 0,
            errorMessage: null,
          },
          user_unique: {
            type: 'hyperUnique',
            hasMultipleValues: false,
            size: 0,
            cardinality: null,
            errorMessage: null,
          },
        },
      };

      if (query.analysisTypes.indexOf('aggregators') !== -1) {
        merged.aggregators = {
          // Normal aggs
          added: { type: 'doubleSum', name: 'added', fieldName: 'added' },
          count: { type: 'longSum', name: 'count', fieldName: 'count' },

          // This can happen if the JS agg was used at ingestion time
          delta: {
            type: 'javascript',
            name: 'delta',
            fieldNames: ['delta'],
            fnAggregate: 'function(partialA,partialB) {return partialA + partialB; }',
            fnReset: 'function() {return 0; }',
            fnCombine: 'function(partialA,partialB) {return partialA + partialB; }',
          },

          // A histogram
          delta_hist: {
            type: 'approxHistogramFold',
            name: 'delta_hist',
            fieldName: 'delta_hist',
            resolution: 50,
            numBuckets: 7,
            lowerLimit: '-Infinity',
            upperLimit: 'Infinity',
          },
          user_unique: { type: 'hyperUnique', name: 'user_unique', fieldName: 'user_unique' },
        };
      }

      return Promise.resolve(merged);
    } else if (query.queryType === 'introspect') {
      return Promise.resolve({
        dimensions: ['anonymous', 'language', 'namespace', 'newPage', 'page', 'time'],
        metrics: ['added', 'count', 'delta_hist', 'user_unique'],
      });
    } else if (query.queryType === 'timeBoundary') {
      return Promise.resolve({
        minTime: '2013-05-09T18:24:00.000Z',
        maxTime: '2013-05-09T18:37:00.000Z',
      });
    } else {
      throw new Error(`unsupported query ${query.queryType}`);
    }
  });

  it('errors on bad introspectionStrategy', () => {
    expect(() => {
      External.fromJS(
        {
          engine: 'druid',
          source: 'wikipedia',
          introspectionStrategy: 'crowd-source',
        },
        requesterFail,
      );
    }).to.throw("invalid introspectionStrategy 'crowd-source'");
  });

  it('does an introspect with general failure', () => {
    let wikiExternal = External.fromJS(
      {
        engine: 'druid',
        source: 'wikipedia',
      },
      requesterFail,
    );

    return wikiExternal
      .introspect()
      .then(() => {
        throw new Error('DID_NOT_ERROR');
      })
      .catch(err => {
        expect(err.message).to.equal('Bad status code');
      });
  });

  it('does an introspect with segmentMetadata (with aggregators)', () => {
    let wikiExternal = External.fromJS(
      {
        engine: 'druid',
        source: 'wikipedia',
        timeAttribute: 'time',
      },
      requesterDruid_0_10_0,
    );

    return wikiExternal.introspect().then(introspectedExternal => {
      expect(introspectedExternal.version).to.equal('0.10.0');
      expect(introspectedExternal.toJS().attributes).to.deep.equal([
        {
          name: 'time',
          nativeType: '__time',
          type: 'TIME',
          range: {
            bounds: '[]',
            end: new Date('2013-05-09T18:37:00.000Z'),
            start: new Date('2013-05-09T18:24:00.000Z'),
          },
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
          nativeType: 'FLOAT',
          type: 'NUMBER',
          unsplitable: true,
        },
        {
          name: 'anonymous',
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
          maker: {
            expression: {
              name: 'delta',
              op: 'ref',
            },
            op: 'sum',
          },
          name: 'delta',
          nativeType: 'FLOAT',
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
          name: 'language',
          nativeType: 'STRING',
          type: 'STRING',
        },
        {
          name: 'namespace',
          nativeType: 'STRING',
          type: 'SET/STRING',
        },
        {
          name: 'page',
          nativeType: 'STRING',
          type: 'STRING',
        },
        {
          name: 'user_unique',
          nativeType: 'hyperUnique',
          type: 'NULL',
          unsplitable: true,
        },
      ]);
    });
  });

  it('respects the introspectionStrategy flag', () => {
    let wikiExternal = External.fromJS(
      {
        engine: 'druid',
        source: 'wikipedia',
        introspectionStrategy: 'datasource-get',
        timeAttribute: 'time',
      },
      requesterDruid_0_10_0,
    );

    return wikiExternal.introspect().then(introspectedExternal => {
      expect(introspectedExternal.version).to.equal('0.10.0');
      expect(introspectedExternal.toJS().attributes).to.deep.equal([
        {
          name: 'time',
          nativeType: '__time',
          type: 'TIME',
        },
        {
          name: 'anonymous',
          nativeType: 'STRING',
          type: 'STRING',
        },
        {
          name: 'language',
          nativeType: 'STRING',
          type: 'STRING',
        },
        {
          name: 'namespace',
          nativeType: 'STRING',
          type: 'STRING',
        },
        {
          name: 'newPage',
          nativeType: 'STRING',
          type: 'STRING',
        },
        {
          name: 'page',
          nativeType: 'STRING',
          type: 'STRING',
        },
        {
          name: 'added',
          nativeType: 'FLOAT',
          type: 'NUMBER',
          unsplitable: true,
        },
        {
          name: 'count',
          nativeType: 'FLOAT',
          type: 'NUMBER',
          unsplitable: true,
        },
        {
          name: 'delta_hist',
          nativeType: 'FLOAT',
          type: 'NUMBER',
          unsplitable: true,
        },
        {
          name: 'user_unique',
          nativeType: 'FLOAT',
          type: 'NUMBER',
          unsplitable: true,
        },
      ]);
    });
  });
});
