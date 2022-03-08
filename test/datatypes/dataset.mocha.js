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
const { Timezone } = require('chronoshift');
const { testImmutableClass } = require('immutable-class-tester');

const { sane } = require('../utils');
const plywood = require('../plywood');
const { Dataset, AttributeInfo, $, Set, r, TimeRange } = plywood;

describe('Dataset', () => {
  it('is immutable class', () => {
    testImmutableClass(Dataset, [
      {
        attributes: [
          { name: 'x', type: 'NUMBER' },
          { name: 'y', type: 'NUMBER' },
        ],
        data: [
          { x: 1, y: 2 },
          { x: 2, y: 3 },
        ],
      },

      {
        keys: ['BestCity'],
        attributes: [
          { name: 'Void', type: 'NULL' },
          { name: 'SoTrue', type: 'BOOLEAN' },
          { name: 'NotSoTrue', type: 'BOOLEAN' },
          { name: 'Zero', type: 'NUMBER' },
          { name: 'Count', type: 'NUMBER' },
          { name: 'HowAwesome', type: 'NUMBER' },
          { name: 'HowLame', type: 'NUMBER' },
          { name: 'HowMuch', type: 'NUMBER_RANGE' },
          { name: 'ToInfinityAndBeyond', type: 'NUMBER_RANGE' },
          { name: 'SomeDate', type: 'TIME' },
          { name: 'SomeTimeRange', type: 'TIME_RANGE' },
          { name: 'BestCity', type: 'STRING' },
          { name: 'Vegetables', type: 'SET/STRING' },
          { name: 'FunTimes', type: 'SET/TIME_RANGE' },
          { name: 'SubData', type: 'DATASET' },
          { name: 'hasOwnProperty', type: 'STRING' },
        ],
        data: [
          {
            Void: null,
            SoTrue: true,
            NotSoTrue: false,
            Zero: 0,
            Count: 2353,
            HowAwesome: 'Infinity',
            HowLame: '-Infinity',
            HowMuch: {
              start: 0,
              end: 7,
            },
            ToInfinityAndBeyond: {
              start: null,
              end: null,
              bounds: '()',
            },
            SomeDate: new Date('2015-01-26T04:54:10Z'),
            SomeTimeRange: {
              start: new Date('2015-01-26T04:54:10Z'),
              end: new Date('2015-01-26T05:00:00Z'),
            },
            BestCity: 'San Francisco',
            Vegetables: {
              setType: 'STRING',
              elements: ['Broccoli', 'Brussels sprout', 'Potato'],
            },
            FunTimes: {
              setType: 'TIME_RANGE',
              elements: [
                { start: new Date('2015-01-26T04:54:10Z'), end: new Date('2015-01-26T05:00:00Z') },
                { start: new Date('2015-02-20T04:54:10Z'), end: new Date('2015-02-20T05:00:00Z') },
              ],
            },
            SubData: {
              attributes: [
                { name: 'x', type: 'NUMBER' },
                { name: 'y', type: 'NUMBER' },
              ],
              data: [
                { x: 1, y: 2 },
                { x: 2, y: 3 },
              ],
            },
            hasOwnProperty: 'troll',
          },
        ],
      },

      {
        attributes: [
          { name: 'Carat', type: 'NUMBER_RANGE' },
          { name: 'Count', type: 'NUMBER' },
          { name: 'Other', type: 'NULL' },
        ],
        data: [
          {
            Carat: {
              end: 0.5,
              start: 0.25,
            },
            Count: 1360,
            Other: 'hello',
          },
          {
            Carat: {
              end: 0.75,
              start: 0.5,
            },
            Count: 919,
            Other: 'hello',
          },
          {
            Carat: {
              end: 1.25,
              start: 1,
            },
            Count: 298,
            Other: 'hello',
          },
        ],
      },
    ]);
  });

  describe('fromJS', () => {
    it('works in basic form', () => {
      expect(Dataset.fromJS([{ nan: NaN }]).data).to.deep.equal([{ nan: null }]);
    });
  });

  describe('getFullType', () => {
    it('works in empty case', () => {
      expect(Dataset.fromJS([]).getFullType()).to.deep.equal({
        type: 'DATASET',
        datasetType: {},
      });
    });

    it('works in singleton case', () => {
      expect(Dataset.fromJS([{}]).getFullType()).to.deep.equal({
        type: 'DATASET',
        datasetType: {},
      });
    });

    it('works in basic case', () => {
      expect(
        Dataset.fromJS([
          { x: 1, y: 'hello', z: new Date(1000) },
          { x: 2, y: 'world', z: new Date(1001) },
        ]).getFullType(),
      ).to.deep.equal({
        type: 'DATASET',
        datasetType: {
          x: { type: 'NUMBER' },
          y: { type: 'STRING' },
          z: { type: 'TIME' },
        },
      });
    });

    it('works in set case', () => {
      expect(
        Dataset.fromJS([
          { x: 1, y: ['hello', 'moon'] },
          { x: 2, y: ['world', 'moon'] },
        ]).getFullType(),
      ).to.deep.equal({
        type: 'DATASET',
        datasetType: {
          x: { type: 'NUMBER' },
          y: { type: 'SET/STRING' },
        },
      });
    });

    it('works in nested case', () => {
      expect(
        Dataset.fromJS([
          {
            x: 1,
            y: 'hello',
            z: new Date(1000),
            subData: [
              { a: 50.5, b: 'woop' },
              { a: 50.6, b: 'w00p' },
            ],
          },
          {
            x: 2,
            y: 'woops',
            z: new Date(1001),
            subData: [
              { a: 51.5, b: 'Woop' },
              { a: 51.6, b: 'W00p' },
            ],
          },
        ]).getFullType(),
      ).to.deep.equal({
        type: 'DATASET',
        datasetType: {
          subData: {
            type: 'DATASET',
            datasetType: {
              a: { type: 'NUMBER' },
              b: { type: 'STRING' },
            },
          },
          x: { type: 'NUMBER' },
          y: { type: 'STRING' },
          z: { type: 'TIME' },
        },
      });
    });

    it('works in double nested case', () => {
      expect(
        Dataset.fromJS([
          {
            a: 'hello',
            subData: [
              {
                b: 'world',
                subSubData: [
                  {
                    c: 'piece',
                    subData: [
                      { d: 50.5, e: 'woop' },
                      { d: 50.6, e: 'w00p' },
                    ],
                  },
                ],
              },
            ],
          },
        ]).getFullType(),
      ).to.deep.equal({
        type: 'DATASET',
        datasetType: {
          a: {
            type: 'STRING',
          },
          subData: {
            datasetType: {
              b: {
                type: 'STRING',
              },
              subSubData: {
                datasetType: {
                  c: {
                    type: 'STRING',
                  },
                  subData: {
                    datasetType: {
                      d: {
                        type: 'NUMBER',
                      },
                      e: {
                        type: 'STRING',
                      },
                    },
                    type: 'DATASET',
                  },
                },
                type: 'DATASET',
              },
            },
            type: 'DATASET',
          },
        },
      });
    });
  });

  describe('introspects', () => {
    it('works in nested case', () => {
      let ds = Dataset.fromJS([
        {
          x: 1,
          y: 'hello',
          z: new Date(1000),
          subData: [
            { a: 50.5, b: 'woop' },
            { a: 50.6, b: 'w00p' },
          ],
        },
        {
          x: 2,
          y: 'woops',
          z: new Date(1001),
          subData: [
            { a: 51.5, b: 'Woop' },
            { a: 51.6, b: 'W00p' },
          ],
        },
      ]);

      expect(AttributeInfo.toJSs(ds.attributes)).to.deep.equal([
        {
          name: 'z',
          type: 'TIME',
        },
        {
          name: 'y',
          type: 'STRING',
        },
        {
          name: 'x',
          type: 'NUMBER',
        },
        {
          name: 'subData',
          type: 'DATASET',
        },
      ]);
    });

    it('in real case', () => {
      let ds = Dataset.fromJS([
        {
          time: new Date('2015-09-12T00:46:58.771Z'),
          channel: '#en.wikipedia',
          cityName: 'SF',
          comment: 'added project',
          countryIsoCode: 'US',
          countryName: 'United States',
          isAnonymous: false,
          isMinor: false,
          isNew: false,
          isRobot: false,
          isUnpatrolled: false,
          metroCode: null,
          namespace: 'Talk',
          page: 'Talk:Oswald Tilghman',
          regionIsoCode: null,
          regionName: null,
          user: 'GELongstreet',
          delta: 36,
          added: 36,
          deleted: 0,
        },
        {
          time: new Date('2015-09-12T00:48:20.157Z'),
          channel: '#en.wikipedia',
          cityName: 'Campbell',
          comment: "Rectifying someone's mischief",
          countryIsoCode: 'US',
          countryName: 'United States',
          isAnonymous: true,
          isMinor: false,
          isNew: false,
          isRobot: false,
          isUnpatrolled: false,
          metroCode: 807,
          namespace: 'Main',
          page: 'President of India',
          regionIsoCode: null,
          regionName: null,
          user: '73.162.114.225',
          delta: -26,
          added: 0,
          deleted: 26,
        },
      ]);

      expect(AttributeInfo.toJSs(ds.attributes)).to.deep.equal([
        { name: 'time', type: 'TIME' },
        { name: 'channel', type: 'STRING' },
        { name: 'cityName', type: 'STRING' },
        { name: 'comment', type: 'STRING' },
        { name: 'countryIsoCode', type: 'STRING' },
        { name: 'countryName', type: 'STRING' },
        { name: 'namespace', type: 'STRING' },
        { name: 'page', type: 'STRING' },
        { name: 'regionIsoCode', type: 'STRING' },
        { name: 'regionName', type: 'STRING' },
        { name: 'user', type: 'STRING' },
        { name: 'isAnonymous', type: 'BOOLEAN' },
        { name: 'isMinor', type: 'BOOLEAN' },
        { name: 'isNew', type: 'BOOLEAN' },
        { name: 'isRobot', type: 'BOOLEAN' },
        { name: 'isUnpatrolled', type: 'BOOLEAN' },
        { name: 'added', type: 'NUMBER' },
        { name: 'deleted', type: 'NUMBER' },
        { name: 'delta', type: 'NUMBER' },
        { name: 'metroCode', type: 'NUMBER' },
      ]);
    });
  });

  describe('sorts', () => {
    let someDataset = Dataset.fromJS([
      { time: new Date('2015-01-04T12:32:43Z'), resource: 'A', value: 7, nice: false },
      { time: null, resource: 'B', value: 2, nice: true },
      { time: new Date('2015-01-03T12:32:43Z'), resource: null, value: null, nice: null },
    ]);

    it('STRING, ascending', () => {
      expect(
        someDataset
          .sort($('resource'), 'ascending')
          .toJS()
          .data.map(d => {
            return d.resource;
          }),
      ).to.deep.equal([null, 'A', 'B']);
    });

    it('STRING, descending', () => {
      expect(
        someDataset
          .sort($('resource'), 'descending')
          .toJS()
          .data.map(d => {
            return d.resource;
          }),
      ).to.deep.equal(['B', 'A', null]);
    });

    it('NUMBER, ascending', () => {
      expect(
        someDataset
          .sort($('value'), 'ascending')
          .toJS()
          .data.map(d => {
            return d.value;
          }),
      ).to.deep.equal([null, 2, 7]);
    });

    it('NUMBER, descending', () => {
      expect(
        someDataset
          .sort($('value'), 'descending')
          .toJS()
          .data.map(d => {
            return d.value;
          }),
      ).to.deep.equal([7, 2, null]);
    });

    it('BOOLEAN, ascending', () => {
      expect(
        someDataset
          .sort($('nice'), 'ascending')
          .toJS()
          .data.map(d => {
            return d.nice;
          }),
      ).to.deep.equal([null, false, true]);
    });

    it('BOOLEAN, descending', () => {
      expect(
        someDataset
          .sort($('nice'), 'descending')
          .toJS()
          .data.map(d => {
            return d.nice;
          }),
      ).to.deep.equal([true, false, null]);
    });
  });

  describe('methods', () => {
    let emptyDataset = Dataset.fromJS([]);

    let emptyNestedDataset = Dataset.fromJS([
      {
        count: 0,
        split: [],
      },
    ]);

    let totalsDataset = Dataset.fromJS([{ count: 0 }]);

    let totalsDatasetWithSplit = Dataset.fromJS({
      keys: [],
      attributes: [
        { name: 'count', type: 'NUMBER' },
        { name: 'split', type: 'DATASET' },
      ],
      data: [
        {
          count: 90,
          split: {
            keys: ['model'],
            attributes: [
              { name: 'model', type: 'STRING' },
              { name: 'count', type: 'NUMBER' },
            ],
            data: [
              {
                model: 'Civic',
                count: 20,
              },
              {
                model: 'Prius',
                count: 10,
              },
            ],
          },
        },
      ],
    });

    let carDataset = Dataset.fromJS([
      {
        time: new Date('2015-01-04T12:32:43Z'),
        make: 'Honda',
        model: 'Civic',
        price: 10000,
      },
      {
        time: new Date('2015-01-04T14:00:40Z'),
        make: 'Toyota',
        model: 'Prius',
        price: 20000,
      },
    ]).select(['time', 'make', 'model', 'price']);

    let carAndPartsDataset = Dataset.fromJS([
      {
        time: new Date('2015-01-04T12:32:43Z'),
        make: 'Honda',
        model: 'Civic',
        price: 10000,
        parts: [
          { part: 'Engine', weight: 500 },
          { part: 'Door', weight: 20 },
        ],
      },
      {
        time: new Date('2015-01-04T14:00:40Z'),
        make: 'Toyota',
        model: 'Prius',
        price: 20000,
        parts: [
          { part: 'Engine', weight: 400 },
          { part: 'Door', weight: 25 },
        ],
      },
    ]).select(['time', 'make', 'model', 'price', 'parts']);

    let carTotalAndSubSplitDataset = Dataset.fromJS([
      {
        price: 10000,
        weight: 1000,
        ByMake: [
          {
            make: 'Honda',
            price: 12000,
            weight: 1200,
            ByModel: [
              {
                model: 'Civic',
                price: 11000,
                weight: 1100,
              },
              {
                model: 'Accord',
                price: 13000,
                weight: 1300,
              },
            ],
          },
          {
            make: 'Toyota',
            price: 12000,
            weight: 1200,
            ByModel: [
              {
                model: 'Prius',
                price: 11000,
                weight: 1100,
              },
              {
                model: 'Corolla',
                price: 13000,
                weight: 1300,
              },
            ],
          },
        ],
      },
    ]);

    let timeSeriesResult = Dataset.fromJS([
      {
        count: 31427,
        added: 6686857,
        Split: [
          {
            Segment: {
              start: '2013-02-26T16:00:00.000Z',
              end: '2013-02-26T17:00:00.000Z',
              type: 'TIME_RANGE',
            },
            count: 2012,
            added: 373390,
          },
          {
            Segment: {
              start: '2013-02-26T01:00:00.000Z',
              end: '2013-02-26T02:00:00.000Z',
              type: 'TIME_RANGE',
            },
            count: 1702,
            added: 181266,
          },
          {
            Segment: {
              start: '2013-02-26T15:00:00.000Z',
              end: '2013-02-26T16:00:00.000Z',
              type: 'TIME_RANGE',
            },
            count: 1625,
            added: 284339,
          },
        ],
      },
    ]);

    describe('#rows', () => {
      it('works', () => {
        expect(carTotalAndSubSplitDataset.rows()).to.equal(7);
      });
    });

    describe('#depthFirstTrimTo', () => {
      it('works', () => {
        expect(carTotalAndSubSplitDataset.depthFirstTrimTo(3).toJS().data).to.deep.equal([
          {
            ByMake: {
              attributes: [
                {
                  name: 'make',
                  type: 'STRING',
                },
                {
                  name: 'price',
                  type: 'NUMBER',
                },
                {
                  name: 'weight',
                  type: 'NUMBER',
                },
                {
                  name: 'ByModel',
                  type: 'DATASET',
                },
              ],
              data: [
                {
                  ByModel: {
                    attributes: [
                      {
                        name: 'model',
                        type: 'STRING',
                      },
                      {
                        name: 'price',
                        type: 'NUMBER',
                      },
                      {
                        name: 'weight',
                        type: 'NUMBER',
                      },
                    ],
                    data: [
                      {
                        model: 'Civic',
                        price: 11000,
                        weight: 1100,
                      },
                    ],
                  },
                  make: 'Honda',
                  price: 12000,
                  weight: 1200,
                },
              ],
            },
            price: 10000,
            weight: 1000,
          },
        ]);
      });
    });

    describe('#findDatumByAttribute', () => {
      it('works with basic dataset', () => {
        expect(carDataset.findDatumByAttribute('make', 'Kaka')).to.deep.equal(undefined);

        expect(carDataset.findDatumByAttribute('make', 'Honda')).to.deep.equal({
          time: new Date('2015-01-04T12:32:43Z'),
          make: 'Honda',
          model: 'Civic',
          price: 10000,
        });

        expect(
          carDataset.findDatumByAttribute('time', new Date('2015-01-04T12:32:43Z')),
        ).to.deep.equal({
          time: new Date('2015-01-04T12:32:43Z'),
          make: 'Honda',
          model: 'Civic',
          price: 10000,
        });
      });
    });

    describe('#getColumns', () => {
      it('works with empty dataset', () => {
        expect(emptyDataset.getColumns()).to.deep.equal([]);
      });

      it('works with basic dataset', () => {
        expect(
          carDataset
            .getColumns()
            .map(c => c.name)
            .sort()
            .join(','),
        ).to.equal('make,model,price,time');
      });

      it('works with sub-dataset without prefix', () => {
        expect(
          carAndPartsDataset
            .getColumns()
            .map(c => c.name)
            .sort()
            .join(','),
        ).to.equal('make,model,part,price,time,weight');
      });

      it('works with sub-dataset with prefix', () => {
        expect(
          carAndPartsDataset
            .getColumns({ prefixColumns: true })
            .map(c => c.name)
            .sort()
            .join(','),
        ).to.equal('make,model,parts.part,parts.weight,price,time');
      });

      it('works with total and sub-split', () => {
        expect(
          carTotalAndSubSplitDataset
            .getColumns()
            .map(c => c.name)
            .sort()
            .join(','),
        ).to.deep.equal('make,model,price,weight');
      });
    });

    describe('#flatten', () => {
      it('works with empty dataset', () => {
        expect(emptyDataset.flatten().data).to.deep.equal([]);
      });

      it('works with empty nested dataset', () => {
        expect(emptyNestedDataset.flatten().data).to.deep.equal([]);
      });

      it('works with totals dataset', () => {
        expect(totalsDataset.flatten().data).to.deep.equal([{ count: 0 }]);
      });

      it('works with totals dataset with split', () => {
        expect(totalsDatasetWithSplit.flatten().toJS()).to.deep.equal({
          attributes: [
            {
              name: 'count',
              type: 'NUMBER',
            },
            {
              name: 'model',
              type: 'STRING',
            },
          ],
          data: [
            {
              count: 20,
              model: 'Civic',
            },
            {
              count: 10,
              model: 'Prius',
            },
          ],
        });
      });

      it("works with totals dataset with split (columnOrdering: 'keys-first')", () => {
        expect(
          totalsDatasetWithSplit.flatten({ columnOrdering: 'keys-first' }).toJS(),
        ).to.deep.equal({
          attributes: [
            {
              name: 'model',
              type: 'STRING',
            },
            {
              name: 'count',
              type: 'NUMBER',
            },
          ],
          data: [
            {
              count: 20,
              model: 'Civic',
            },
            {
              count: 10,
              model: 'Prius',
            },
          ],
        });
      });

      it('works with basic dataset', () => {
        expect(carDataset.flatten().data).to.deep.equal([
          {
            make: 'Honda',
            model: 'Civic',
            price: 10000,
            time: new Date('2015-01-04T12:32:43.000Z'),
          },
          {
            make: 'Toyota',
            model: 'Prius',
            price: 20000,
            time: new Date('2015-01-04T14:00:40.000Z'),
          },
        ]);
      });

      it('works with sub-dataset with prefix', () => {
        expect(carAndPartsDataset.flatten({ prefixColumns: true }).toJS()).to.deep.equal({
          attributes: [
            {
              name: 'time',
              type: 'TIME',
            },
            {
              name: 'make',
              type: 'STRING',
            },
            {
              name: 'model',
              type: 'STRING',
            },
            {
              name: 'price',
              type: 'NUMBER',
            },
            {
              name: 'parts.part',
              type: 'STRING',
            },
            {
              name: 'parts.weight',
              type: 'NUMBER',
            },
          ],
          data: [
            {
              'make': 'Honda',
              'model': 'Civic',
              'parts.part': 'Engine',
              'parts.weight': 500,
              'price': 10000,
              'time': new Date('2015-01-04T12:32:43.000Z'),
            },
            {
              'make': 'Honda',
              'model': 'Civic',
              'parts.part': 'Door',
              'parts.weight': 20,
              'price': 10000,
              'time': new Date('2015-01-04T12:32:43.000Z'),
            },
            {
              'make': 'Toyota',
              'model': 'Prius',
              'parts.part': 'Engine',
              'parts.weight': 400,
              'price': 20000,
              'time': new Date('2015-01-04T14:00:40.000Z'),
            },
            {
              'make': 'Toyota',
              'model': 'Prius',
              'parts.part': 'Door',
              'parts.weight': 25,
              'price': 20000,
              'time': new Date('2015-01-04T14:00:40.000Z'),
            },
          ],
        });
      });

      it('works with two sub-datasets with prefix', () => {
        const carAndPartsDatasetX2 = carAndPartsDataset.apply('smarts', $('parts', 'DATASET'));
        expect(carAndPartsDatasetX2.flatten({ prefixColumns: true }).toJS()).to.deep.equal({
          attributes: [
            {
              name: 'time',
              type: 'TIME',
            },
            {
              name: 'make',
              type: 'STRING',
            },
            {
              name: 'model',
              type: 'STRING',
            },
            {
              name: 'price',
              type: 'NUMBER',
            },
            {
              name: 'parts.part',
              type: 'STRING',
            },
            {
              name: 'parts.weight',
              type: 'NUMBER',
            },
            {
              name: 'smarts.part',
              type: 'STRING',
            },
            {
              name: 'smarts.weight',
              type: 'NUMBER',
            },
          ],
          data: [
            {
              'make': 'Honda',
              'model': 'Civic',
              'parts.part': 'Engine',
              'parts.weight': 500,
              'price': 10000,
              'time': new Date('2015-01-04T12:32:43.000Z'),
            },
            {
              'make': 'Honda',
              'model': 'Civic',
              'parts.part': 'Door',
              'parts.weight': 20,
              'price': 10000,
              'time': new Date('2015-01-04T12:32:43.000Z'),
            },
            {
              'make': 'Honda',
              'model': 'Civic',
              'price': 10000,
              'smarts.part': 'Engine',
              'smarts.weight': 500,
              'time': new Date('2015-01-04T12:32:43.000Z'),
            },
            {
              'make': 'Honda',
              'model': 'Civic',
              'price': 10000,
              'smarts.part': 'Door',
              'smarts.weight': 20,
              'time': new Date('2015-01-04T12:32:43.000Z'),
            },
            {
              'make': 'Toyota',
              'model': 'Prius',
              'parts.part': 'Engine',
              'parts.weight': 400,
              'price': 20000,
              'time': new Date('2015-01-04T14:00:40.000Z'),
            },
            {
              'make': 'Toyota',
              'model': 'Prius',
              'parts.part': 'Door',
              'parts.weight': 25,
              'price': 20000,
              'time': new Date('2015-01-04T14:00:40.000Z'),
            },
            {
              'make': 'Toyota',
              'model': 'Prius',
              'price': 20000,
              'smarts.part': 'Engine',
              'smarts.weight': 400,
              'time': new Date('2015-01-04T14:00:40.000Z'),
            },
            {
              'make': 'Toyota',
              'model': 'Prius',
              'price': 20000,
              'smarts.part': 'Door',
              'smarts.weight': 25,
              'time': new Date('2015-01-04T14:00:40.000Z'),
            },
          ],
        });
      });

      it('works with total and sub-split', () => {
        expect(carTotalAndSubSplitDataset.flatten().data).to.deep.equal([
          {
            make: 'Honda',
            model: 'Civic',
            price: 11000,
            weight: 1100,
          },
          {
            make: 'Honda',
            model: 'Accord',
            price: 13000,
            weight: 1300,
          },
          {
            make: 'Toyota',
            model: 'Prius',
            price: 11000,
            weight: 1100,
          },
          {
            make: 'Toyota',
            model: 'Corolla',
            price: 13000,
            weight: 1300,
          },
        ]);
      });

      it('works with total and sub-split with postorder', () => {
        expect(carTotalAndSubSplitDataset.flatten({ order: 'postorder' }).data).to.deep.equal([
          {
            make: 'Honda',
            model: 'Civic',
            price: 11000,
            weight: 1100,
          },
          {
            make: 'Honda',
            model: 'Accord',
            price: 13000,
            weight: 1300,
          },
          {
            make: 'Honda',
            price: 12000,
            weight: 1200,
          },
          {
            make: 'Toyota',
            model: 'Prius',
            price: 11000,
            weight: 1100,
          },
          {
            make: 'Toyota',
            model: 'Corolla',
            price: 13000,
            weight: 1300,
          },
          {
            make: 'Toyota',
            price: 12000,
            weight: 1200,
          },
          {
            price: 10000,
            weight: 1000,
          },
        ]);
      });

      it('works with total and sub-split with preorder and nesting indicator', () => {
        expect(
          carTotalAndSubSplitDataset.flatten({ order: 'preorder', nestingName: 'nest' }).data,
        ).to.deep.equal([
          {
            nest: 0,
            price: 10000,
            weight: 1000,
          },
          {
            make: 'Honda',
            nest: 1,
            price: 12000,
            weight: 1200,
          },
          {
            make: 'Honda',
            model: 'Civic',
            nest: 2,
            price: 11000,
            weight: 1100,
          },
          {
            make: 'Honda',
            model: 'Accord',
            nest: 2,
            price: 13000,
            weight: 1300,
          },
          {
            make: 'Toyota',
            nest: 1,
            price: 12000,
            weight: 1200,
          },
          {
            make: 'Toyota',
            model: 'Prius',
            nest: 2,
            price: 11000,
            weight: 1100,
          },
          {
            make: 'Toyota',
            model: 'Corolla',
            nest: 2,
            price: 13000,
            weight: 1300,
          },
        ]);
      });

      it('works with timeseries with preorder and nesting indicator', () => {
        expect(
          timeSeriesResult.flatten({ order: 'preorder', nestingName: 'nest' }).data[0],
        ).to.deep.equal({
          added: 6686857,
          count: 31427,
          nest: 0,
        });
      });
    });

    describe('#toTabular', () => {
      it('does not auto remove line breaks', () => {
        let dsLineBreak = Dataset.fromJS([{ letter: `dear john\nhow are you doing\nfish` }]);
        expect(dsLineBreak.toTabular({ lineBreak: '\n', finalLineBreak: 'suppress' })).to
          .equal(sane`
          letter
          dear john
          how are you doing
          fish
        `);
      });

      it('allows for custom finalization', () => {
        let ds = Dataset.fromJS([{ number: 2, isEmpty: true }]);

        let finalizer = v => {
          return `[${v}]`;
        };

        expect(ds.toTabular({ finalizer: finalizer, lineBreak: '\n', finalLineBreak: 'suppress' }))
          .to.equal(sane`
          [isEmpty],[number]
          [true],[2]
        `);
      });
    });

    describe('#toCSV', () => {
      it('works with basic dataset', () => {
        expect(carDataset.toCSV({ lineBreak: '\n', finalLineBreak: 'suppress' })).to.equal(sane`
          time,make,model,price
          2015-01-04T12:32:43Z,Honda,Civic,10000
          2015-01-04T14:00:40Z,Toyota,Prius,20000
        `);
      });

      it('works with basic dataset, NULL types', () => {
        const nullCarDataset = new Dataset({
          attributes: carDataset.attributes.map(a => a.changeType('NULL')),
          data: carDataset.data,
        });

        expect(nullCarDataset.toCSV({ lineBreak: '\n', finalLineBreak: 'suppress' })).to.equal(sane`
          time,make,model,price
          2015-01-04T12:32:43.000Z,Honda,Civic,10000
          2015-01-04T14:00:40.000Z,Toyota,Prius,20000
        `);
      });

      it('works with sub-dataset', () => {
        expect(carAndPartsDataset.toCSV({ lineBreak: '\n', finalLineBreak: 'suppress' })).to
          .equal(sane`
          time,make,model,price,part,weight
          2015-01-04T12:32:43Z,Honda,Civic,10000,Engine,500
          2015-01-04T12:32:43Z,Honda,Civic,10000,Door,20
          2015-01-04T14:00:40Z,Toyota,Prius,20000,Engine,400
          2015-01-04T14:00:40Z,Toyota,Prius,20000,Door,25
        `);
      });

      it('escapes commas by enclosing whole field in quotes', () => {
        let dsComma = Dataset.fromJS([{ letter: 'dear john, how are you doing' }]);

        expect(dsComma.toCSV({ lineBreak: '\n', finalLineBreak: 'suppress' })).to.equal(sane`
        letter
        "dear john, how are you doing"
        `);
      });

      it('escapes quotes by escaping quoted text but not if already quoted due to comma escape', () => {
        let dsComma = Dataset.fromJS([{ letter: 'dear john, how are you "doing"' }]);
        expect(dsComma.toCSV({ lineBreak: '\n', finalLineBreak: 'suppress' })).to.equal(sane`
        letter
        "dear john, how are you ""doing"""
        `);
      });

      it('escapes sets properly', () => {
        let ds = Dataset.fromJS([
          {
            w: [1, 2],
            x: 1,
            y: ['hel,lo', 'mo\non'],
            z: [
              'Thu Feb 19 2015 16:00:00 GMT-0800 (PST)',
              'Fri Feb 20 2015 16:00:00 GMT-0800 (PST)',
            ],
          },
          { w: ['null'], x: 2, y: ['wo\r\nrld', 'mo\ron'], z: ['stars'] },
        ]).select(['w', 'x', 'y', 'z']);

        expect(ds.toCSV({ lineBreak: '\n', finalLineBreak: 'suppress' })).to.equal(sane`
          w,x,y,z
          "1, 2",1,"hel,lo, mo on","Thu Feb 19 2015 16:00:00 GMT-0800 (PST), Fri Feb 20 2015 16:00:00 GMT-0800 (PST)"
          null,2,"wo rld, mo on",stars
        `);
      });

      it('removes line breaks with csv', () => {
        let dsLineBreak = Dataset.fromJS([
          { letter: `dear john\nhow are you doing?\r\nI'm good.\r-mildred` },
        ]);
        expect(dsLineBreak.toCSV({ lineBreak: '\n', finalLineBreak: 'suppress' })).to.equal(sane`
        letter
        dear john how are you doing? I'm good. -mildred
        `);
      });

      it('is ok with null', () => {
        let ds = Dataset.fromJS([{ letter: null }]);

        expect(ds.toCSV({ lineBreak: '\n', finalLineBreak: 'suppress' })).to.equal(sane`
        letter
        null
        `);
      });

      it('works with timezones', () => {
        let ds = Dataset.fromJS([
          {
            time: new Date('2015-01-04T12:32:43Z'),
            make: 'Honda',
            model: 'Civic',
            price: 10000,
          },
          {
            time: new Date('2015-01-04T14:00:40Z'),
            make: 'Toyota',
            model: 'Prius',
            price: 20000,
          },
        ]);

        expect(
          ds.toCSV({
            lineBreak: '\n',
            finalLineBreak: 'suppress',
            timezone: Timezone.fromJS('Asia/Kathmandu'),
          }),
        ).to.equal(sane`
          time,make,model,price
          2015-01-04T18:17:43+05:45,Honda,Civic,10000
          2015-01-04T19:45:40+05:45,Toyota,Prius,20000
        `);
      });

      it('respects ordered columns', () => {
        let carDataset = Dataset.fromJS([
          {
            time: new Date('2015-01-04T12:32:43Z'),
            make: 'Honda',
            model: 'Civic',
            price: 10000,
          },
          {
            time: new Date('2015-01-04T14:00:40Z'),
            make: 'Toyota',
            model: 'Prius',
            price: 20000,
          },
        ]);

        expect(carDataset.select(['model', 'make', 'price', 'time']).toCSV({ lineBreak: '\n' })).to
          .deep.equal(sane`
          model,make,price,time
          Civic,Honda,10000,2015-01-04T12:32:43Z
          Prius,Toyota,20000,2015-01-04T14:00:40Z
        `);
      });
    });

    describe('#toTSV', () => {
      it('does not escape commas in text by enclosing whole field in quotes', () => {
        let dsComma = Dataset.fromJS([{ letter: 'dear john, how are you doing' }]);

        expect(dsComma.toTSV({ lineBreak: '\n', finalLineBreak: 'suppress' })).to.equal(sane`
        letter
        dear john, how are you doing
        `);
      });

      it('escapes tabs in text field', () => {
        let dsComma = Dataset.fromJS([{ letter: 'dear john, \thow are you doing' }]);

        expect(dsComma.toTSV({ lineBreak: '\n', finalLineBreak: 'suppress' })).to.equal(sane`
        letter
        dear john, how are you doing
        `);
      });

      it('escapes set/string properly', () => {
        let ds = Dataset.fromJS([
          { x: 1, y: ['hel,lo', 'mo\non'] },
          { x: 2, y: ['wo\r\nrld', 'mo\ron'] },
        ]).select(['x', 'y']);

        expect(ds.toTSV({ lineBreak: '\n', finalLineBreak: 'suppress' })).to.equal(sane`
          x	y
          1	hel,lo, mo on
          2	wo rld, mo on
        `);
      });

      it('works with timezones', () => {
        let ds = Dataset.fromJS({
          attributes: [
            { name: 'time', type: 'TIME' },
            { name: 'favoriteTimes', type: 'SET/TIME' },
            { name: 'favoriteTimeRanges', type: 'SET/TIME_RANGE' },
            { name: 'favoriteTimeRange', type: 'TIME_RANGE' },
          ],
          data: [
            {
              time: new Date('2015-01-04T14:00:40Z'),
              favoriteTimeRanges: {
                type: 'SET',
                setType: 'TIME_RANGE',
                elements: [
                  {
                    start: new Date('2015-02-20T00:00:00Z'),
                    end: new Date('2015-02-21T00:00:00Z'),
                  },
                  {
                    start: new Date('2015-02-22T00:00:00Z'),
                    end: new Date('2015-02-24T00:00:00Z'),
                  },
                ],
              },
              favoriteTimeRange: {
                type: 'TIME_RANGE',
                start: new Date('2015-02-20T00:00:00Z'),
                end: new Date('2015-02-21T00:00:00Z'),
              },
              favoriteTimes: {
                type: 'SET',
                setType: 'TIME',
                elements: [new Date('2015-02-20T00:00:00Z'), new Date('2015-02-24T00:00:00Z')],
              },
            },
          ],
        });

        expect(
          ds.toTSV({
            lineBreak: '\n',
            finalLineBreak: 'suppress',
            timezone: Timezone.fromJS('Asia/Kathmandu'),
          }),
        ).to.equal(sane`
          time	favoriteTimes	favoriteTimeRanges	favoriteTimeRange
          2015-01-04T19:45:40+05:45	2015-02-20T05:45:00+05:45, 2015-02-24T05:45:00+05:45	[2015-02-20T05:45:00+05:45,2015-02-21T05:45:00+05:45], [2015-02-22T05:45:00+05:45,2015-02-24T05:45:00+05:45]	[2015-02-20T05:45:00+05:45,2015-02-21T05:45:00+05:45]`);
      });
    });

    describe('#select', () => {
      it('respects order', () => {
        let carDataset = Dataset.fromJS([
          {
            time: new Date('2015-01-04T12:32:43Z'),
            make: 'Honda',
            model: 'Civic',
            price: 10000,
          },
          {
            time: new Date('2015-01-04T14:00:40Z'),
            make: 'Toyota',
            model: 'Prius',
            price: 20000,
          },
        ]);

        expect(
          carDataset
            .select(['time', 'model', 'make', 'price'])
            .getColumns()
            .map(c => c.name),
        ).to.deep.equal(['time', 'model', 'make', 'price']);
      });
    });

    describe('#join', () => {
      it('works on simple key values', () => {
        let carDataset1 = Dataset.fromJS({
          keys: ['make'],
          data: [
            {
              time: new Date('2015-01-04T12:32:43Z'),
              make: 'Honda',
              model: 'Civic',
              price: 10000,
            },
            {
              time: new Date('2015-01-04T14:00:40Z'),
              make: 'Toyota',
              model: 'Prius',
              price: 20000,
            },
          ],
        });

        let carDataset2 = Dataset.fromJS({
          keys: ['make'],
          data: [
            {
              make: 'Toyota',
              model: 'Prius',
              cmo: 23,
            },
            {
              make: 'Ford',
              model: 'Focus',
              cmo: 13,
            },
          ],
        });

        expect(carDataset1.join(carDataset2).toJS()).to.deep.equal({
          attributes: [
            {
              name: 'time',
              type: 'TIME',
            },
            {
              name: 'make',
              type: 'STRING',
            },
            {
              name: 'model',
              type: 'STRING',
            },
            {
              name: 'price',
              type: 'NUMBER',
            },
            {
              name: 'cmo',
              type: 'NUMBER',
            },
          ],
          data: [
            {
              make: 'Honda',
              model: 'Civic',
              price: 10000,
              time: new Date('2015-01-04T12:32:43.000Z'),
            },
            {
              cmo: 23,
              make: 'Toyota',
              model: 'Prius',
              price: 20000,
              time: new Date('2015-01-04T14:00:40.000Z'),
            },
          ],
          keys: ['make'],
        });
      });

      it('works on complex key values', () => {
        let carDataset1 = Dataset.fromJS({
          keys: ['make'],
          data: [
            {
              time: new Date('2015-01-04T12:32:43Z'),
              make: 'Honda',
              model: 'Civic',
              price: 10000,
            },
            {
              time: new Date('2015-01-04T14:00:40Z'),
              make: 'Toyota',
              model: 'Prius',
              price: 20000,
            },
          ],
        });

        let carDataset2 = Dataset.fromJS({
          keys: ['make'],
          data: [
            {
              make: 'Toyota',
              model: 'Prius',
              cmo: 23,
            },
            {
              make: 'Ford',
              model: 'Focus',
              cmo: 13,
            },
          ],
        });

        expect(carDataset1.join(carDataset2).toJS()).to.deep.equal({
          attributes: [
            {
              name: 'time',
              type: 'TIME',
            },
            {
              name: 'make',
              type: 'STRING',
            },
            {
              name: 'model',
              type: 'STRING',
            },
            {
              name: 'price',
              type: 'NUMBER',
            },
            {
              name: 'cmo',
              type: 'NUMBER',
            },
          ],
          data: [
            {
              make: 'Honda',
              model: 'Civic',
              price: 10000,
              time: new Date('2015-01-04T12:32:43.000Z'),
            },
            {
              cmo: 23,
              make: 'Toyota',
              model: 'Prius',
              price: 20000,
              time: new Date('2015-01-04T14:00:40.000Z'),
            },
          ],
          keys: ['make'],
        });
      });
    });
  });

  describe('#fullJoin', () => {
    it('works on simple key values', () => {
      function mkHour(s) {
        const start = new Date(s);
        const end = new Date(start.valueOf() + 60 * 60 * 1000);
        return { type: 'TIME_RANGE', start, end };
      }

      let ds1 = Dataset.fromJS({
        keys: ['__time'],
        data: [
          {
            count: 1,
            __time: mkHour('2018-08-28T12:00:00.000Z'),
          },
          {
            count: 2,
            __time: mkHour('2018-08-28T13:00:00.000Z'),
          },
          {
            count: 1,
            __time: mkHour('2018-08-28T14:00:00.000Z'),
          },
          {
            count: 3,
            __time: mkHour('2018-08-28T15:00:00.000Z'),
          },
          {
            count: 12,
            __time: mkHour('2018-08-28T17:00:00.000Z'),
          },
          {
            count: 1,
            __time: mkHour('2018-08-28T18:00:00.000Z'),
          },
          {
            count: 10,
            __time: mkHour('2018-08-28T20:00:00.000Z'),
          },
          {
            count: 1,
            __time: mkHour('2018-08-28T22:00:00.000Z'),
          },
          {
            count: 4,
            __time: mkHour('2018-08-28T23:00:00.000Z'),
          },
          {
            count: 11,
            __time: mkHour('2018-08-29T00:00:00.000Z'),
          },
          {
            count: 1,
            __time: mkHour('2018-08-29T01:00:00.000Z'),
          },
          {
            count: 3,
            __time: mkHour('2018-08-29T02:00:00.000Z'),
          },
          {
            count: 5,
            __time: mkHour('2018-08-29T03:00:00.000Z'),
          },
        ],
      });

      let ds2 = Dataset.fromJS({
        keys: ['__time'],
        data: [
          {
            _cmp_count: 2,
            __time: mkHour('2018-08-28T15:00:00.000Z'),
          },
          {
            _cmp_count: 4,
            __time: mkHour('2018-08-28T17:00:00.000Z'),
          },
          {
            _cmp_count: 2,
            __time: mkHour('2018-08-28T20:00:00.000Z'),
          },
          {
            _cmp_count: 4,
            __time: mkHour('2018-08-28T22:00:00.000Z'),
          },
          {
            _cmp_count: 6,
            __time: mkHour('2018-08-28T23:00:00.000Z'),
          },
          {
            _cmp_count: 3,
            __time: mkHour('2018-08-29T00:00:00.000Z'),
          },
          {
            _cmp_count: 1,
            __time: mkHour('2018-08-29T01:00:00.000Z'),
          },
          {
            _cmp_count: 1,
            __time: mkHour('2018-08-29T02:00:00.000Z'),
          },
        ],
      });

      expect(
        ds1.fullJoin(ds2, (a, b) => a.start.valueOf() - b.start.valueOf()).toJS().data,
      ).to.deep.equal([
        {
          __time: {
            end: new Date('2018-08-28T13:00:00.000Z'),
            start: new Date('2018-08-28T12:00:00.000Z'),
          },
          count: 1,
        },
        {
          __time: {
            end: new Date('2018-08-28T14:00:00.000Z'),
            start: new Date('2018-08-28T13:00:00.000Z'),
          },
          count: 2,
        },
        {
          __time: {
            end: new Date('2018-08-28T15:00:00.000Z'),
            start: new Date('2018-08-28T14:00:00.000Z'),
          },
          count: 1,
        },
        {
          __time: {
            end: new Date('2018-08-28T16:00:00.000Z'),
            start: new Date('2018-08-28T15:00:00.000Z'),
          },
          _cmp_count: 2,
          count: 3,
        },
        {
          __time: {
            end: new Date('2018-08-28T18:00:00.000Z'),
            start: new Date('2018-08-28T17:00:00.000Z'),
          },
          _cmp_count: 4,
          count: 12,
        },
        {
          __time: {
            end: new Date('2018-08-28T19:00:00.000Z'),
            start: new Date('2018-08-28T18:00:00.000Z'),
          },
          count: 1,
        },
        {
          __time: {
            end: new Date('2018-08-28T21:00:00.000Z'),
            start: new Date('2018-08-28T20:00:00.000Z'),
          },
          _cmp_count: 2,
          count: 10,
        },
        {
          __time: {
            end: new Date('2018-08-28T23:00:00.000Z'),
            start: new Date('2018-08-28T22:00:00.000Z'),
          },
          _cmp_count: 4,
          count: 1,
        },
        {
          __time: {
            end: new Date('2018-08-29T00:00:00.000Z'),
            start: new Date('2018-08-28T23:00:00.000Z'),
          },
          _cmp_count: 6,
          count: 4,
        },
        {
          __time: {
            end: new Date('2018-08-29T01:00:00.000Z'),
            start: new Date('2018-08-29T00:00:00.000Z'),
          },
          _cmp_count: 3,
          count: 11,
        },
        {
          __time: {
            end: new Date('2018-08-29T02:00:00.000Z'),
            start: new Date('2018-08-29T01:00:00.000Z'),
          },
          _cmp_count: 1,
          count: 1,
        },
        {
          __time: {
            end: new Date('2018-08-29T03:00:00.000Z'),
            start: new Date('2018-08-29T02:00:00.000Z'),
          },
          _cmp_count: 1,
          count: 3,
        },
        {
          __time: {
            end: new Date('2018-08-29T04:00:00.000Z'),
            start: new Date('2018-08-29T03:00:00.000Z'),
          },
          count: 5,
        },
      ]);
    });
  });
});
