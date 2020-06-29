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

describe('DruidExternal Introspection Large', () => {
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
        },
      };

      for (let i = 0; i < 40000; i++) {
        merged.columns['dim_' + i] = {
          type: 'STRING',
          hasMultipleValues: false,
          size: 0,
          cardinality: 0,
          errorMessage: null,
        };
      }

      return Promise.resolve(merged);
    } else if (query.queryType === 'timeBoundary') {
      return Promise.resolve({
        minTime: 'sdsd',
        maxTime: 'sdsd',
      });
    } else {
      throw new Error(`unsupported query ${query.queryType}`);
    }
  });

  it('does an introspect with segmentMetadata', () => {
    let wikiExternal = External.fromJS(
      {
        engine: 'druid',
        source: 'wikipedia',
        timeAttribute: 'time',
        attributes: [
          { name: 'color', type: 'STRING' },
          { name: 'cut', type: 'STRING' },
          { name: 'carat', type: 'STRING' },
          { name: 'price', type: 'NUMBER', unsplitable: true },
        ],
      },
      requesterDruid_0_10_0,
    );

    return wikiExternal.introspect().then(introspectedExternal => {
      expect(introspectedExternal.version).to.equal('0.10.0');
      expect(introspectedExternal.toJS().attributes.length).to.equal(40007);
    });
  });
});
