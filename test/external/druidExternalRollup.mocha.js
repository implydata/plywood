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

const plywood = require('../plywood');

const { Expression, External, TimeRange, $, ply, r } = plywood;

describe('DruidExternal Rollup', () => {
  const context = {
    wiki: External.fromJS({
      engine: 'druid',
      version: '0.20.0',
      source: 'diamonds',
      rollup: true,
      timeAttribute: 'time',
      attributes: [
        {
          name: 'time',
          type: 'TIME',
        },
        {
          maker: {
            action: 'sum',
            expression: {
              name: 'added',
              op: 'ref',
            },
          },
          name: 'added',
          type: 'NUMBER',
          unsplitable: true,
        },
        {
          maker: {
            action: 'sum',
            expression: {
              name: 'deleted',
              op: 'ref',
            },
          },
          name: 'deleted',
          type: 'NUMBER',
          unsplitable: true,
        },
        {
          name: 'anonymous',
          type: 'STRING',
        },
        {
          maker: {
            action: 'count',
          },
          name: 'count',
          type: 'NUMBER',
          unsplitable: true,
        },
        {
          name: 'delta_hist',
          nativeType: 'approximateHistogram',
          type: 'NUMBER',
        },
        {
          name: 'channel',
          type: 'STRING',
        },
        {
          name: 'namespace',
          type: 'SET/STRING',
        },
        {
          name: 'page',
          type: 'STRING',
        },
        {
          name: 'user_unique',
          nativeType: 'hyperUnique',
          type: 'STRING',
        },
      ],
      allowSelectQueries: true,
      filter: $('time').overlap({
        start: new Date('2015-03-12T00:00:00Z'),
        end: new Date('2015-03-19T00:00:00Z'),
      }),
    }),
  };

  it('works in basic case', () => {
    const ex = ply().apply('Count', '$wiki.count()').apply('AvgAdded', '$wiki.average($added)');

    const queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0]).to.deep.equal([
      {
        aggregations: [
          {
            fieldName: 'count',
            name: 'Count',
            type: 'doubleSum',
          },
          {
            fieldName: 'added',
            name: '!T_0',
            type: 'doubleSum',
          },
        ],
        dataSource: 'diamonds',
        granularity: 'all',
        intervals: '2015-03-12T00Z/2015-03-19T00Z',
        postAggregations: [
          {
            expression: 'if("Count"!=0,(cast("!T_0",\'DOUBLE\')/"Count"),null)',
            name: 'AvgAdded',
            type: 'expression',
          },
        ],
        queryType: 'timeseries',
      },
    ]);
  });

  it('works in filtered average case', () => {
    const ex = ply()
      .apply('AvgEnAdded', '$wiki.filter($channel == "en").average($added)')
      .apply('AvgHeDeleted', '$wiki.filter($channel == "he").average($deleted)');

    const queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan.length).to.equal(1);
    expect(queryPlan[0]).to.deep.equal([
      {
        aggregations: [
          {
            aggregator: {
              fieldName: 'added',
              name: '!T_0',
              type: 'doubleSum',
            },
            filter: {
              dimension: 'channel',
              type: 'selector',
              value: 'en',
            },
            name: '!T_0',
            type: 'filtered',
          },
          {
            aggregator: {
              fieldName: 'count',
              name: '!T_1',
              type: 'doubleSum',
            },
            filter: {
              dimension: 'channel',
              type: 'selector',
              value: 'en',
            },
            name: '!T_1',
            type: 'filtered',
          },
          {
            aggregator: {
              fieldName: 'deleted',
              name: '!T_2',
              type: 'doubleSum',
            },
            filter: {
              dimension: 'channel',
              type: 'selector',
              value: 'he',
            },
            name: '!T_2',
            type: 'filtered',
          },
          {
            aggregator: {
              fieldName: 'count',
              name: '!T_3',
              type: 'doubleSum',
            },
            filter: {
              dimension: 'channel',
              type: 'selector',
              value: 'he',
            },
            name: '!T_3',
            type: 'filtered',
          },
        ],
        dataSource: 'diamonds',
        granularity: 'all',
        intervals: '2015-03-12T00Z/2015-03-19T00Z',
        postAggregations: [
          {
            expression: 'if("!T_1"!=0,(cast("!T_0",\'DOUBLE\')/"!T_1"),null)',
            name: 'AvgEnAdded',
            type: 'expression',
          },
          {
            expression: 'if("!T_3"!=0,(cast("!T_2",\'DOUBLE\')/"!T_3"),null)',
            name: 'AvgHeDeleted',
            type: 'expression',
          },
        ],
        queryType: 'timeseries',
      },
    ]);
  });
});
