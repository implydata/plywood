/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
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

var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { Dataset, $, i$, ply, r, AttributeInfo, External } = plywood;

// used to trigger routes with external
var dummyExternal = External.fromJS({
  engine: 'druid',
  source: 'diamonds',
  attributes: [
    { name: 'time', type: 'TIME' },
    { name: 'color', type: 'STRING' }
  ]
});

describe("compute native", () => {
  var data = [
    { cut: 'Good',  price: 400,  time: new Date('2015-10-01T09:20:30Z'), tags: ['super', 'cool'] },
    { cut: 'Good',  price: 300,  time: new Date('2015-10-02T08:20:30Z'), tags: ['super'] },
    { cut: 'Great', price: 124,  time: null,                             tags: ['cool'] },
    { cut: 'Wow',   price: 160,  time: new Date('2015-10-04T06:20:30Z'), tags: ['sweet'] },
    { cut: 'Wow',   price: 100,  time: new Date('2015-10-05T05:20:30Z'), tags: null },
    { cut: null,    price: null, time: new Date('2015-10-06T04:20:30Z'), tags: ['super', 'sweet', 'cool'] }
  ];

  it("works in uber-basic case", () => {
    var ex = ply()
      .apply('five', 5)
      .apply('nine', 9);

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            five: 5,
            nine: 9
          }
        ]);
      });
  });

  it("works in nested case", () => {
    var ex = ply()
      .apply('TotalPrice', '$data.sum($price)')
      .apply('ByCut',
        $('data').split('$cut', 'Cut')
          .apply('SumPrice', '$data.sum($price)')
          .apply('PriceOfTotal', '$SumPrice / $TotalPrice')
          .sort('$SumPrice', 'descending')
          .apply('ByTags',
            $('data').split('$tags', 'Tag')
              .apply('SumPrice', '$data.sum($price)')
              .sort('$Tag', 'ascending')
          )
      );

    return ex.compute({ data: Dataset.fromJS(data) })
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "ByCut": [
              {
                "ByTags": [
                  {
                    "SumPrice": 400,
                    "Tag": "cool"
                  },
                  {
                    "SumPrice": 700,
                    "Tag": "super"
                  }
                ],
                "Cut": "Good",
                "PriceOfTotal": 0.6457564575645757,
                "SumPrice": 700
              },
              {
                "ByTags": [
                  {
                    "SumPrice": 100,
                    "Tag": null
                  },
                  {
                    "SumPrice": 160,
                    "Tag": "sweet"
                  }
                ],
                "Cut": "Wow",
                "PriceOfTotal": 0.23985239852398524,
                "SumPrice": 260
              },
              {
                "ByTags": [
                  {
                    "SumPrice": 124,
                    "Tag": "cool"
                  }
                ],
                "Cut": "Great",
                "PriceOfTotal": 0.11439114391143912,
                "SumPrice": 124
              },
              {
                "ByTags": [
                  {
                    "SumPrice": 0,
                    "Tag": "cool"
                  },
                  {
                    "SumPrice": 0,
                    "Tag": "super"
                  },
                  {
                    "SumPrice": 0,
                    "Tag": "sweet"
                  }
                ],
                "Cut": null,
                "PriceOfTotal": 0,
                "SumPrice": 0
              }
            ],
            "TotalPrice": 1084
          }
        ]);
      });
  });

  it("gets length of string", () => {
    var ex = ply()
      .apply('length', r('hey').length());

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            length: 3
          }
        ]);
      });
  });

  it("gets location of substring", () => {
    var ex = ply()
      .apply('location', r('hey').indexOf('e'));

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            location: 1
          }
        ]);
      });
  });

  it("transforms case of a string", () => {
    var ex = ply()
      .apply('upper', r('hey').transformCase('upperCase'))
      .apply('lower', r('HEY').transformCase('lowerCase'));

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            upper: 'HEY',
            lower: 'hey'
          }
        ]);
      });
  });

  it("works with power and abs", () => {
    var ex = ply()
      .apply('number', 256)
      .apply('four', $('number').power(0.5).power(0.5))
      .apply('one', $('four').power(0))
      .apply('reciprocal', $('four').power(-1))
      .apply('negative', -4)
      .apply('positive', 4)
      .apply('absNeg', $('negative').absolute())
      .apply('absPos', $('positive').absolute());

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            'number': 256,
            'four': 4,
            'one': 1,
            "reciprocal": 0.25,
            'negative': -4,
            'positive': 4,
            'absNeg': 4,
            'absPos': 4
          }
        ]);
      });
  });

  it("casts from number to time", () => {
    // 1442016000000 -> 09/12/2015 00:00:00
    // 1442059199000 -> 09/12/2015 11:59:59

    var ex = ply()
      .apply('time', new Date('2015-09-12T09:20:30Z'))
      .apply('between', $('time').greaterThan(r(1442016000000).cast('TIME')).and($('time').lessThan(r(1442059199000).cast('TIME'))))

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "between": true,
            "time": {
              "type": "TIME",
              "value": new Date('2015-09-12T09:20:30.000Z')
            }
          }
        ]);
      });
  });

  it("casts from time to number", () => {
    // 1442049630000 -> 09/12/2015 02:20:30
    var ex = ply()
      .apply('unixTimestamp', r(1442049630000))
      .apply('between', $('unixTimestamp').greaterThan(r(new Date('2015-09-12T00:00:00.000Z')).cast('NUMBER')).and($('unixTimestamp').lessThan(r(new Date('2015-09-12T11:59:30.000Z')).cast('NUMBER'))));

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "between": true,
            "unixTimestamp": 1442049630000
          }
        ]);
      });
  });

  it("casts from number to string", () => {
    var ex = ply()
      .apply('stringifiedNumber', r(22345243).cast('STRING'));

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "stringifiedNumber": "22345243"
          }
        ]);
      });
  });

  it("casts from string to number", () => {
    var ex = ply()
      .apply('numberfiedString', r("22345243").cast('NUMBER'));

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "numberfiedString": 22345243
          }
        ]);
      });
  });

  it("doesn't fallback if not null", () => {
    var ex = $('x').fallback(5);
    return ex.compute({ x: 2 })
      .then((v) => {
        expect(v).to.deep.equal(2);
      });
  });

  it("fallback works with datasets", () => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Two', 2)
      .apply('EmptyData', ply(ds).filter('false'))
      .apply('SumPrice', '$EmptyData.sum($price)')
      .apply('AvgPrice1', $('EmptyData').average($('price')).fallback(2))
      .apply('AvgPrice2', '$EmptyData.sum($price) / $EmptyData.count()');

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "AvgPrice1": 2,
            "AvgPrice2": null,
            "SumPrice": 0,
            "Two": 2
          }
        ]);
      });
  });

  it("gets cardinality of set", () => {
    var data = [
      { id: 1, prices: [400, 200, 3], times: {type: 'SET', elements: [new Date('2015-10-01T09:20:30Z')]}, tags: ['super', 'cool'] },
      { id: 2, prices: [300, 2, 3], times: {type: 'SET', elements: [new Date('2015-10-01T09:20:30Z')]}, tags: ['super'] },
      { id: 3, prices: [124], times: null, tags: ['cool'] },
      { id: 4, prices: [22, 28], times: {type: 'SET', elements: [new Date('2015-10-01T09:20:30Z')]}, tags: ['sweet'] },
      { id: 5, prices: [100, 105], times: {type: 'SET', elements: [new Date('2015-10-01T09:20:30Z')]}, tags: null },
      { id: null, prices: null, times: {type: 'SET', elements: [new Date('2015-10-01T09:20:30Z')]}, tags: ['super', 'sweet', 'cool'] }
    ];

    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply(
        'SetSize',
        $('Data').split({
          'Prices': "$prices.cardinality()",
          'Tags': '$tags.cardinality()',
          'Times': '$times.cardinality()'
        })
      );

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "SetSize": [
              {
                "Prices": 3,
                "Tags": 2,
                "Times": 1
              },
              {
                "Prices": 3,
                "Tags": 1,
                "Times": 1
              },
              {
                "Prices": 1,
                "Tags": 1,
                "Times": null
              },
              {
                "Prices": 2,
                "Tags": 1,
                "Times": 1
              },
              {
                "Prices": 2,
                "Tags": null,
                "Times": 1
              },
              {
                "Prices": null,
                "Tags": 3,
                "Times": 1
              }
            ]
          }
        ]);
      });
  });

  it("works in existing dataset case", () => {
    var ds = Dataset.fromJS([
      { cut: 'Good', price: 400 },
      { cut: 'Great', price: 124 },
      { cut: 'Wow', price: 160 }
    ]);

    var ex = ply(ds)
      .apply('priceX2', $('price').multiply(2));

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          { cut: 'Good', price: 400, priceX2: 800 },
          { cut: 'Great', price: 124, priceX2: 248 },
          { cut: 'Wow', price: 160, priceX2: 320 }
        ]);
      });
  });


  it("will upgrade time for time data", () => {
    var ds = Dataset.fromJS([
      { cut: 'Good', time: new Date('2015-01-03T00:00:00Z') },
      { cut: 'Great', time: new Date('2014-01-04T00:00:00Z') },
      { cut: 'Wow', time: new Date('2015-01-05T00:00:00Z') }
    ]);

    var ex = ply(ds)
      .apply('laterThanJan01', $('time').greaterThan(`'2015-01-01T00:00:00.000'`))
      .apply('laterThanOrEqualJan01', $('time').greaterThanOrEqual(`'2015-01-01T00:00:00.000'`))
      .apply('earlierThanJan04', $('time').lessThan(`'2015-01-04T00:00:00.000'`))
      .apply('earlierThanOrEqualJan04', $('time').lessThan(`'2015-01-04T00:00:00.000'`));

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "cut": "Good",
            "earlierThanJan04": true,
            "earlierThanOrEqualJan04": true,
            "laterThanJan01": true,
            "laterThanOrEqualJan01": true,
            "time": {
              "type": "TIME",
              "value": new Date('2015-01-03T00:00:00.000Z')
            }
          },
          {
            "cut": "Great",
            "earlierThanJan04": true,
            "earlierThanOrEqualJan04": true,
            "laterThanJan01": false,
            "laterThanOrEqualJan01": false,
            "time": {
              "type": "TIME",
              "value": new Date('2014-01-04T00:00:00.000Z')
            }
          },
          {
            "cut": "Wow",
            "earlierThanJan04": false,
            "earlierThanOrEqualJan04": false,
            "laterThanJan01": true,
            "laterThanOrEqualJan01": true,
            "time": {
              "type": "TIME",
              "value": new Date('2015-01-05T00:00:00.000Z')
            }
          }
        ]);
      });
  });

  it("will not upgrade string for string data that can be parsed into time", () => {
    var ds = Dataset.fromJS([
      { cut: 'Good', time: '2015-01-03T00:00:00Z' },
      { cut: 'Great', time: '2014-01-04T00:00:00Z' },
      { cut: 'Wow', time: '2015-01-05T00:00:00Z' }
    ]);

    var ex = ply(ds)
      .apply('laterThanJan01', $('time').greaterThan(`'2015-01-03T00:00:00Z'`))
      .apply('laterThanOrEqualJan01', $('time').greaterThanOrEqual(`'2015-01-03T00:00:00Z'`))
      .apply('earlierThanJan04', $('time').lessThan(`'2015-01-03T00:00:00Z'`))
      .apply('earlierThanOrEqualJan04', $('time').lessThan(`'2015-01-03T00:00:00Z'`));

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "cut": "Good",
            "earlierThanJan04": false,
            "earlierThanOrEqualJan04": false,
            "laterThanJan01": false,
            "laterThanOrEqualJan01": true,
            "time": "2015-01-03T00:00:00Z"
          },
          {
            "cut": "Great",
            "earlierThanJan04": true,
            "earlierThanOrEqualJan04": true,
            "laterThanJan01": false,
            "laterThanOrEqualJan01": false,
            "time": "2014-01-04T00:00:00Z"
          },
          {
            "cut": "Wow",
            "earlierThanJan04": false,
            "earlierThanOrEqualJan04": false,
            "laterThanJan01": true,
            "laterThanOrEqualJan01": true,
            "time": "2015-01-05T00:00:00Z"
          }
        ]);
      });
  });

  it("left side", () => {
    var ds = Dataset.fromJS([
      { cut: 'Good', time: new Date('2015-01-03T00:00:00Z') },
      { cut: 'Great', time: new Date('2014-01-04T00:00:00Z') },
      { cut: 'Wow', time: new Date('2015-01-05T00:00:00Z') }
    ]);

    var ex = ply(ds)
      .apply('Added_NullCities',  `'2015-01-01T00:00:00.000' <= $time`);

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Added_NullCities": true,
            "cut": "Good",
            "time": {
              "type": "TIME",
              "value": new Date('2015-01-03T00:00:00.000Z')
            }
          },
          {
            "Added_NullCities": false,
            "cut": "Great",
            "time": {
              "type": "TIME",
              "value": new Date('2014-01-04T00:00:00.000Z')
            }
          },
          {
            "Added_NullCities": true,
            "cut": "Wow",
            "time": {
              "type": "TIME",
              "value": new Date('2015-01-05T00:00:00.000Z')
            }
          }
        ]);
      });
  });

  it("case insensitivity is respected", () => {
    var ds = Dataset.fromJS([
      { cut: 'Good', time: new Date('2015-01-03T00:00:00Z') },
      { cut: 'Great', time: new Date('2014-01-04T00:00:00Z') },
      { cut: 'Wow', time: new Date('2015-01-05T00:00:00Z') }
    ]);

    var ex = ply(ds)
      .apply('LessThanM',  `'M' <= i$cUt`);

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "LessThanM": false,
            "cut": "Good",
            "time": {
              "type": "TIME",
              "value": new Date('2015-01-03T00:00:00.000Z')
            }
          },
          {
            "LessThanM": false,
            "cut": "Great",
            "time": {
              "type": "TIME",
              "value": new Date('2014-01-04T00:00:00.000Z')
            }
          },
          {
            "LessThanM": true,
            "cut": "Wow",
            "time": {
              "type": "TIME",
              "value": new Date('2015-01-05T00:00:00.000Z')
            }
          }
        ]);
      });
  });

  it("computes quarters", () => {
    var ds = Dataset.fromJS([
      { cut: 'Good', time: new Date('2015-03-31T19:00:00Z') },
      { cut: 'Great', time: new Date('2015-06-30T19:00:00Z') },
      { cut: 'Wow', time: new Date('2015-09-05T00:00:00Z') },
      { cut: 'Wow', time: new Date('2015-12-05T00:00:00Z') }
    ]);

    var ex = ply(ds)
      .apply('Quarter', '$time.timePart("QUARTER")')
      .apply('QuarterAsia', '$time.timePart("QUARTER", "Asia/Kathmandu")');

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Quarter": 1,
            "QuarterAsia": 2,
            "cut": "Good",
            "time": {
              "type": "TIME",
              "value": new Date('2015-03-31T19:00:00.000Z')
            }
          },
          {
            "Quarter": 2,
            "QuarterAsia": 3,
            "cut": "Great",
            "time": {
              "type": "TIME",
              "value": new Date('2015-06-30T19:00:00.000Z')
            }
          },
          {
            "Quarter": 3,
            "QuarterAsia": 3,
            "cut": "Wow",
            "time": {
              "type": "TIME",
              "value": new Date('2015-09-05T00:00:00.000Z')
            }
          },
          {
            "Quarter": 4,
            "QuarterAsia": 4,
            "cut": "Wow",
            "time": {
              "type": "TIME",
              "value": new Date('2015-12-05T00:00:00.000Z')
            }
          }
        ]);
      });
  });

  it("computes fancy quarters", () => {
    var ds = Dataset.fromJS([
      { cut: 'Good', time: new Date('2015-03-31T19:00:00Z') },
      { cut: 'Great', time: new Date('2015-06-30T19:00:00Z') },
      { cut: 'Wow', time: new Date('2015-09-05T00:00:00Z') },
      { cut: 'Wow', time: new Date('2015-12-05T00:00:00Z') }
    ]);

    var ex = ply(ds)
      .split(i$('time').timeFloor('P3M').timePart('SECOND_OF_YEAR'), 'soy', 'data')
      .select("soy");

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "soy": 0
          },
          {
            "soy": 129600
          },
          {
            "soy": 260640
          },
          {
            "soy": 393120
          }
        ]);
      });
  });

  it("works with filter, select", () => {
    var ds = Dataset.fromJS(data);

    var ex = $('ds').filter('$price > 200').select('cut');

    return ex.compute({ ds, dummyExternal })
      .then((v) => {
        expect(v.getColumns()).to.deep.equal([
          {
            "name": "cut",
            "type": "STRING"
          }
        ]);

        expect(v.toJS()).to.deep.equal([
          {
            "cut": "Good"
          },
          {
            "cut": "Good"
          }
        ]);
      });
  });

  it("works with select, limit", () => {
    var ds = Dataset.fromJS(data);

    var ex = $('ds').select('cut').limit(3);

    return ex.compute({ ds })
      .then((v) => {
        expect(v.getColumns()).to.deep.equal([
          {
            "name": "cut",
            "type": "STRING"
          }
        ]);

        expect(v.toJS()).to.deep.equal([
          {
            "cut": "Good"
          },
          {
            "cut": "Good"
          },
          {
            "cut": "Great"
          }
        ]);
      });
  });

  it("works with pure filter", () => {
    var ds = Dataset.fromJS(data);

    var ex = ply(ds).filter('$cut == Great');

    return ex.compute()
      .then((v) => {
        expect(AttributeInfo.toJSs(v.attributes)).to.deep.equal([
          { "name": "time", "type": "TIME" },
          { "name": "cut", "type": "STRING" },
          { "name": "tags", "type": "SET/STRING" },
          { "name": "price", "type": "NUMBER" }
        ]);

        expect(v.toJS()).to.deep.equal([
          {
            cut: 'Great',
            price: 124,
            tags: { type: "SET", setType: "STRING", elements: ['cool'] },
            time: null
          }
        ]);

      });
  });

  it("works with pure empty filter", () => {
    var ds = Dataset.fromJS(data);

    var ex = ply(ds).filter('$cut == Best');

    return ex.compute()
      .then((v) => {
        expect(AttributeInfo.toJSs(v.attributes)).to.deep.equal([
          { "name": "time", "type": "TIME" },
          { "name": "cut", "type": "STRING" },
          { "name": "tags", "type": "SET/STRING" },
          { "name": "price", "type": "NUMBER" }
        ]);

        expect(v.toJS()).to.deep.equal([]);

      });
  });

  it("works with various applies", () => {
    var ds = Dataset.fromJS(data);

    var ex = ply(ds)
      .apply('cutConcat', '"[" ++ $cut ++ "]"')
      .apply('cutMatch', $('cut').match('^G.+'))
      .apply('cutInGoodGreat', $('cut').in(['Good', 'Great']))
      .apply('cutOverlapGoodGreat', $('cut').overlap(['Good', 'Great']))
      .apply('cutOverlapNull', $('cut').overlap([null]))
      .apply('cutIsGoodOverlapFalse', $('cut').is('Good').overlap([false]))
      .apply('timeFloorDay', $('time').timeFloor('P1D'))
      .apply('timeShiftDay2', $('time').timeShift('P1D', 2))
      .apply('timeRangeHours', $('time').timeRange('PT2H', -1))
      .apply('overlapSuperCool', $('tags').overlap(['super', 'cool']));

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            cut: 'Good',
            cutConcat: '[Good]',
            cutMatch: true,
            cutInGoodGreat: true,
            cutOverlapGoodGreat: true,
            cutOverlapNull: false,
            cutIsGoodOverlapFalse: false,
            price: 400,
            tags: { type: "SET", setType: "STRING", elements: ['super', 'cool'] },
            overlapSuperCool: true,
            time: { type: "TIME", value: new Date('2015-10-01T09:20:30Z') },
            timeFloorDay: { type: "TIME", value: new Date('2015-10-01T00:00:00Z') },
            timeShiftDay2: { type: "TIME", value: new Date('2015-10-03T09:20:30Z') },
            timeRangeHours: {
              type: "TIME_RANGE",
              start: new Date('2015-10-01T07:20:30Z'),
              end: new Date('2015-10-01T09:20:30Z')
            }
          },
          {
            cut: 'Good',
            cutConcat: '[Good]',
            cutMatch: true,
            cutInGoodGreat: true,
            cutOverlapGoodGreat: true,
            cutOverlapNull: false,
            cutIsGoodOverlapFalse: false,
            price: 300,
            tags: { type: "SET", setType: "STRING", elements: ['super'] },
            overlapSuperCool: true,
            time: { type: "TIME", value: new Date('2015-10-02T08:20:30Z') },
            timeFloorDay: { type: "TIME", value: new Date('2015-10-02T00:00:00Z') },
            timeShiftDay2: { type: "TIME", value: new Date('2015-10-04T08:20:30Z') },
            timeRangeHours: {
              type: "TIME_RANGE",
              start: new Date('2015-10-02T06:20:30Z'),
              end: new Date('2015-10-02T08:20:30Z')
            }
          },
          {
            cut: 'Great',
            cutConcat: '[Great]',
            cutMatch: true,
            cutInGoodGreat: true,
            cutOverlapGoodGreat: true,
            cutOverlapNull: false,
            cutIsGoodOverlapFalse: true,
            price: 124,
            tags: { type: "SET", setType: "STRING", elements: ['cool'] },
            overlapSuperCool: true,
            time: null,
            timeFloorDay: null,
            timeShiftDay2: null,
            timeRangeHours: null
          },
          {
            cut: 'Wow',
            cutConcat: '[Wow]',
            cutMatch: false,
            cutInGoodGreat: false,
            cutOverlapGoodGreat: false,
            cutOverlapNull: false,
            cutIsGoodOverlapFalse: true,
            price: 160,
            tags: { type: "SET", setType: "STRING", elements: ['sweet'] },
            overlapSuperCool: false,
            time: { type: "TIME", value: new Date('2015-10-04T06:20:30Z') },
            timeFloorDay: { type: "TIME", value: new Date('2015-10-04T00:00:00Z') },
            timeShiftDay2: { type: "TIME", value: new Date('2015-10-06T06:20:30Z') },
            timeRangeHours: {
              type: "TIME_RANGE",
              start: new Date('2015-10-04T04:20:30Z'),
              end: new Date('2015-10-04T06:20:30Z')
            }
          },
          {
            cut: 'Wow',
            cutConcat: '[Wow]',
            cutMatch: false,
            cutInGoodGreat: false,
            cutOverlapGoodGreat: false,
            cutOverlapNull: false,
            cutIsGoodOverlapFalse: true,
            price: 100,
            tags: null,
            overlapSuperCool: false,
            time: { type: "TIME", value: new Date('2015-10-05T05:20:30Z') },
            timeFloorDay: { type: "TIME", value: new Date('2015-10-05T00:00:00Z') },
            timeShiftDay2: { type: "TIME", value: new Date('2015-10-07T05:20:30Z') },
            timeRangeHours: {
              type: "TIME_RANGE",
              start: new Date('2015-10-05T03:20:30Z'),
              end: new Date('2015-10-05T05:20:30Z')
            }
          },
          {
            cut: null,
            cutConcat: null,
            cutMatch: null,
            cutInGoodGreat: false, // ToDo: this is inconsistent, figure put how to fix it.
            cutOverlapGoodGreat: false,
            cutOverlapNull: true,
            cutIsGoodOverlapFalse: true,
            price: null,
            tags: { type: "SET", setType: "STRING", elements: ['super', 'sweet', 'cool'] },
            overlapSuperCool: true,
            time: { type: "TIME", value: new Date('2015-10-06T04:20:30Z') },
            timeFloorDay: { type: "TIME", value: new Date('2015-10-06T00:00:00Z') },
            timeShiftDay2: { type: "TIME", value: new Date('2015-10-08T04:20:30Z') },
            timeRangeHours: {
              type: "TIME_RANGE",
              start: new Date('2015-10-06T02:20:30Z'),
              end: new Date('2015-10-06T04:20:30Z')
            }
          }
        ]);
      });
  });

  it("works with collect applies", () => {
    var ds = Dataset.fromJS([
      { cut: "Good", color: "A" },
      { cut: "Good", color: "A" },
      { cut: "Good", color: "B" },
      { cut: "Great", color: "B" },
      { cut: "Great", color: "C" },
      { cut: "Great", color: "D" },
      { cut: "Amaze", color: "D" },
    ]);

    var ex = ply(ds)
      .split('$cut', 'Cut', 'data')
      .apply('colors', '$data.collect($color)');

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Cut": "Good",
            "colors": {
              "elements": [
                "A",
                "B"
              ],
              "setType": "STRING",
              "type": "SET"
            }
          },
          {
            "Cut": "Great",
            "colors": {
              "elements": [
                "B",
                "C",
                "D"
              ],
              "setType": "STRING",
              "type": "SET"
            }
          },
          {
            "Cut": "Amaze",
            "colors": {
              "elements": [
                "D"
              ],
              "setType": "STRING",
              "type": "SET"
            }
          }
        ]);
      });
  });

  it("works with quantiles", () => {
    // Test data comes from: https://en.wikipedia.org/wiki/Quantile (order changed to not be sorted)
    var quantileData = [
      { vOdd: 20, vEven: 20 },
      { vOdd:  6, vEven:  6 },
      { vOdd:  7, vEven:  7 },
      { vOdd:  8, vEven:  8 },
      { vOdd:  8, vEven:  8 },
      { vOdd:  9, vEven: null },
      { vOdd: 10, vEven: 10 },
      { vOdd: 13, vEven: 13 },
      { vOdd: 15, vEven: 15 },
      { vOdd: 16, vEven: 16 },
      { vOdd:  3, vEven:  3 }
    ];

    var ex = ply()
      .apply('d', Dataset.fromJS(quantileData).hide())
      .apply('quantileEven0.00', '$d.quantile($vEven, 0.00)')
      .apply('quantileEven0.25', '$d.quantile($vEven, 0.25)')
      .apply('quantileEven0.50', '$d.quantile($vEven, 0.50)')
      .apply('quantileEven0.75', '$d.quantile($vEven, 0.75)')
      .apply('quantileEven1.00', '$d.quantile($vEven, 1.00)')
      .apply('quantileOdd0.00', '$d.quantile($vOdd, 0.00)')
      .apply('quantileOdd0.25', '$d.quantile($vOdd, 0.25)')
      .apply('quantileOdd0.50', '$d.quantile($vOdd, 0.50)')
      .apply('quantileOdd0.75', '$d.quantile($vOdd, 0.75)')
      .apply('quantileOdd1.00', '$d.quantile($vOdd, 1.00)');

    return ex.compute().then((v) => {
      expect(v.toJS()).to.deep.equal([
        {
          "quantileEven0.00": 3,
          "quantileEven0.25": 7,
          "quantileEven0.50": 9,
          "quantileEven0.75": 15,
          "quantileEven1.00": 20,
          "quantileOdd0.00": 3,
          "quantileOdd0.25": 7,
          "quantileOdd0.50": 9,
          "quantileOdd0.75": 15,
          "quantileOdd1.00": 20
        }
      ]);
    });
  });

  it("works with a basic select", () => {
    var ds = Dataset.fromJS(data);

    var ex = ply(ds).select('price', 'cut');

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          { cut: 'Good',  price: 400  },
          { cut: 'Good',  price: 300  },
          { cut: 'Great', price: 124  },
          { cut: 'Wow',   price: 160  },
          { cut: 'Wow',   price: 100  },
          { cut: null,    price: null }
        ]);
      });
  });

  it("works with a transformed select", () => {
    var ds = Dataset.fromJS(data);

    var ex = ply(ds)
      .apply('[cut]', '"[" ++ $cut ++ "]"')
      .apply('price+1', '$price + 1')
      .select('[cut]', 'price+1');

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "[cut]": "[Good]",
            "price+1": 401
          },
          {
            "[cut]": "[Good]",
            "price+1": 301
          },
          {
            "[cut]": "[Great]",
            "price+1": 125
          },
          {
            "[cut]": "[Wow]",
            "price+1": 161
          },
          {
            "[cut]": "[Wow]",
            "price+1": 101
          },
          {
            "[cut]": null,
            "price+1": 1
          }
        ]);
      });
  });

  it("works with simple split", () => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply(
        'Cuts',
        $('Data').split('$cut', 'Cut')
      );

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Cuts": [
              { "Cut": "Good" },
              { "Cut": "Great" },
              { "Cut": "Wow" },
              { "Cut": null }
            ]
          }
        ]);
      });
  });

  it("works with set split", () => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply(
        'Tags',
        $('Data').split('$tags', 'Tag')
          .apply('Count', '$Data.count()')
          .sort('$Count', 'descending')
      );

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Tags": [
              {
                "Count": 3,
                "Tag": "super"
              },
              {
                "Count": 3,
                "Tag": "cool"
              },
              {
                "Count": 2,
                "Tag": "sweet"
              },
              {
                "Count": 1,
                "Tag": null
              }
            ]
          }
        ]);
      });
  });

  it("works with singleton dataset", () => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Two', 2)
      .apply('EmptyData', ply(ds).filter('false'))
      .apply('SumPrice', '$EmptyData.sum($price)')
      .apply('AvgPrice1', '$EmptyData.average($price)')
      .apply('AvgPrice2', '$EmptyData.sum($price) / $EmptyData.count()');

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "AvgPrice1": null,
            "AvgPrice2": null,
            "SumPrice": 0,
            "Two": 2
          }
        ]);
      });
  });

  it("works with simple split followed by some simple applies", () => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Two', 2)
      .apply('Data', ply(ds))
      .apply(
        'Cuts',
        $('Data').split('$cut', 'Cut')
          .apply('Six', 6)
          .apply('Seven', $('Six').add(1))
          .apply('EightByZero', r(8).divide(0))
          .apply('ZeroByZero', r(0).divide(0))
      );

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Two": 2,
            "Cuts": [
              {
                "Cut": "Good",
                "Six": 6,
                "Seven": 7,
                "EightByZero": { "type": "NUMBER", "value": "Infinity" },
                "ZeroByZero": null
              },
              {
                "Cut": "Great",
                "Six": 6,
                "Seven": 7,
                "EightByZero": { "type": "NUMBER", "value": "Infinity" },
                "ZeroByZero": null
              },
              {
                "Cut": "Wow",
                "Six": 6,
                "Seven": 7,
                "EightByZero": { "type": "NUMBER", "value": "Infinity" },
                "ZeroByZero": null
              },
              {
                "Cut": null,
                "Six": 6,
                "Seven": 7,
                "EightByZero": { "type": "NUMBER", "value": "Infinity" },
                "ZeroByZero": null
              }
            ]
          }
        ]);
      });
  });

  it("works with timePart split (non-UTC timezone)", () => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply('Count', '$Data.count()')
      .apply(
        'TimeParts',
        $('Data').split("$time.timePart('HOUR_OF_DAY', 'Etc/UTC')", 'Part')
          .apply('Count', '$Data.count()')
      );

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Count": 6,
            "TimeParts": [
              { "Count": 1, "Part": 9 },
              { "Count": 1, "Part": 8 },
              { "Count": 1, "Part": null },
              { "Count": 1, "Part": 6 },
              { "Count": 1, "Part": 5 },
              { "Count": 1, "Part": 4 }
            ]
          }
        ]);
      });
  });

  it("works with timePart split (other timezone)", () => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply('Count', '$Data.count()')
      .apply(
        'TimeParts',
        $('Data').split("$time.timePart('HOUR_OF_DAY', 'America/New_York')", 'Part')
          .apply('Count', '$Data.count()')
      );

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Count": 6,
            "TimeParts": [
              { "Count": 1, "Part": 5 },
              { "Count": 1, "Part": 4 },
              { "Count": 1, "Part": null },
              { "Count": 1, "Part": 2 },
              { "Count": 1, "Part": 1 },
              { "Count": 1, "Part": 0 }
            ]
          }
        ]);
      });
  });

  it("works with context", () => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply('CountPlusX', '$Data.count() + $x');

    return ex.compute({ x: 13 })
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "CountPlusX": 19
          }
        ]);
      });
  });

  it("works with context and split", () => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply(
        'Cuts',
        $('Data').split('$cut', 'Cut')
          .apply('CountPlusX', '$Data.count() + $x')
          .apply('SumPrice', '$Data.sum($price)')
          .apply('MinPrice', '$Data.min($price)')
          .apply('MaxPrice', '$Data.max($price)')
          .apply('MinTime', '$Data.min($time)')
          .apply('MaxTime', '$Data.max($time)')
      );

    return ex.compute({ x: 13 })
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Cuts": [
              {
                "CountPlusX": 15,
                "Cut": "Good",
                "MaxPrice": 400,
                "MinPrice": 300,
                "SumPrice": 700,
                "MaxTime": {
                  "type": "TIME",
                  "value": new Date('2015-10-02T08:20:30Z')
                },
                "MinTime": {
                  "type": "TIME",
                  "value": new Date('2015-10-01T09:20:30Z')
                }
              },
              {
                "CountPlusX": 14,
                "Cut": "Great",
                "MaxPrice": 124,
                "MinPrice": 124,
                "SumPrice": 124,
                "MaxTime": null,
                "MinTime": null
              },
              {
                "CountPlusX": 15,
                "Cut": "Wow",
                "MaxPrice": 160,
                "MinPrice": 100,
                "SumPrice": 260,
                "MaxTime": {
                  "type": "TIME",
                  "value": new Date('2015-10-05T05:20:30Z')
                },
                "MinTime": {
                  "type": "TIME",
                  "value": new Date('2015-10-04T06:20:30Z')
                }
              },
              {
                "CountPlusX": 14,
                "Cut": null,
                "MaxPrice": null,
                "MinPrice": null,
                "SumPrice": 0,
                "MaxTime": {
                  "type": "TIME",
                  "value": new Date('2015-10-06T04:20:30Z')
                },
                "MinTime": {
                  "type": "TIME",
                  "value": new Date('2015-10-06T04:20:30Z')
                }
              }
            ]
          }
        ]);
      });
  });

  it("works with simple split and sub apply", () => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply(
        'Cuts',
        $('Data').split('$cut', 'Cut')
          .apply('Count', $('Data').count())
          .apply('AvgPrice', $('Data').average('$price'))
      );

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Cuts": [
              {
                "Count": 2,
                "Cut": "Good",
                "AvgPrice": 350
              },
              {
                "Count": 1,
                "Cut": "Great",
                "AvgPrice": 124
              },
              {
                "Count": 2,
                "Cut": "Wow",
                "AvgPrice": 130
              },
              {
                "Count": 1,
                "Cut": null,
                "AvgPrice": 0
              }
            ]
          }
        ]);
      });
  });

  it("works with simple split and sub apply + sort + limit", () => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply(
        'Cuts',
        $('Data').split('$cut', 'Cut')
          .apply('Count', $('Data').count())
          .sort('$Cut', 'descending')
          .limit(2)
      );

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Cuts": [
              {
                "Count": 2,
                "Cut": "Wow"
              },
              {
                "Count": 1,
                "Cut": "Great"
              }
            ]
          }
        ]);
      });
  });

  it("works with simple filter", () => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds).filter($('price').in(105, 305)))
      .apply('Count', '$Data.count()');

    return ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Count": 3
          }
        ]);
      });
  });


  describe("sort test", () => {
    var data = [
      { n: 1 },
      { n: 2 },
      { n: 10 },
      { n: 20 }
    ];

    it("sorts on numbers", () => {
      var ds = Dataset.fromJS(data);

      var ex = ply(ds).sort('$n');

      return ex.compute()
        .then((v) => {
          expect(v.toJS()).to.deep.equal([
            { n: 1 },
            { n: 2 },
            { n: 10 },
            { n: 20 }
          ]);
        });
    });

    it("sorts on number ranges", () => {
      var ds = Dataset.fromJS(data);

      var ex = ply(ds).apply('nr', '$n.numberBucket(1)').select('nr').sort('$nr');

      return ex.compute()
        .then((v) => {
          expect(v.toJS()).to.deep.equal([
            {
              "nr": {
                "end": 2,
                "start": 1,
                "type": "NUMBER_RANGE"
              }
            },
            {
              "nr": {
                "end": 3,
                "start": 2,
                "type": "NUMBER_RANGE"
              }
            },
            {
              "nr": {
                "end": 11,
                "start": 10,
                "type": "NUMBER_RANGE"
              }
            },
            {
              "nr": {
                "end": 21,
                "start": 20,
                "type": "NUMBER_RANGE"
              }
            }
          ]);
        });
    });

  });


  describe("it works with re-selects", () => {
    var ds = Dataset.fromJS(data).hide();
    var midData = null;

    it("works with simple group/label and subData filter with applies", () => {
      var ex = ply()
        .apply('Data', ply(ds))
        .apply('Count', '$Data.count()')
        .apply('Price', '$Data.sum($price)')
        .apply(
          'Cuts',
          $('Data').split('$cut', 'Cut')
            .apply('Count', '$Data.count()')
            .apply('Price', '$Data.sum($price)')
        );

      return ex.compute()
        .then((v) => {
          midData = v;
          expect(midData.toJS()).to.deep.equal([
            {
              "Count": 6,
              "Price": 1084,
              "Cuts": [
                {
                  "Cut": "Good",
                  "Count": 2,
                  "Price": 700
                },
                {
                  "Cut": "Great",
                  "Count": 1,
                  "Price": 124
                },
                {
                  "Cut": "Wow",
                  "Count": 2,
                  "Price": 260
                },
                {
                  "Cut": null,
                  "Count": 1,
                  "Price": 0
                }
              ]
            }
          ]);
        })
        .done();
    });

    it("re-selects", () => {
      var ex = ply(midData)
        .apply('CountOver2', '$Count / 2')
        .apply(
          'Cuts',
          $('Cuts')
            .apply('AvgPrice', '$Data.sum($price) / $Data.count()')
        );

      return ex.compute()
        .then((v) => {
          expect(v.toJS()).to.deep.equal([
            {
              "Count": 6,
              "CountOver2": 3,
              "Cuts": [
                {
                  "AvgPrice": 350,
                  "Count": 2,
                  "Cut": "Good",
                  "Price": 700
                },
                {
                  "AvgPrice": 124,
                  "Count": 1,
                  "Cut": "Great",
                  "Price": 124
                },
                {
                  "AvgPrice": 130,
                  "Count": 2,
                  "Cut": "Wow",
                  "Price": 260
                },
                {
                  "AvgPrice": 0,
                  "Count": 1,
                  "Cut": null,
                  "Price": 0
                }
              ],
              "Price": 1084
            }
          ]);
        })
        .done();
    });
  });


  describe("joins", () => {
    it("does a join on split", () => {
      var ds = Dataset.fromJS(data).hide();

      var ex = ply()
        .apply('Data1', ply(ds).filter($('price').in(105, 305)))
        .apply('Data2', ply(ds).filter($('price').in(105, 305).not()))
        .apply('Count1', '$Data1.count()')
        .apply('Count2', '$Data2.count()')
        .apply(
          'Cuts',
          $('Data1').split('$cut', 'Cut').join($('Data2').split('$cut', 'Cut', 'K2'))
            .apply('Counts', '100 * $Data1.count() + $K2.count()')
        );

      return ex.compute()
        .then((v) => {
          expect(v.toJS()).to.deep.equal([
            {
              "Count1": 3,
              "Count2": 3,
              "Cuts": [
                {
                  "Counts": 101,
                  "Cut": "Good"
                },
                {
                  "Counts": 100,
                  "Cut": "Great"
                },
                {
                  "Counts": 101,
                  "Cut": "Wow"
                },
                {
                  "Counts": 1,
                  "Cut": null
                }
              ]
            }
          ]);
        })
        .done();
    });
  });
});
