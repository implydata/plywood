var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { Dataset, $, ply, r, AttributeInfo, External } = plywood;

// used to trigger routes with external
var dummyExternal = External.fromJS({
  engine: 'druid',
  dataSource: 'diamonds',
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

  it("works in uber-basic case", (testComplete) => {
    var ex = ply()
      .apply('five', 5)
      .apply('nine', 9);

    ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            five: 5,
            nine: 9
          }
        ]);
        testComplete();
      })
      .done();
  });

  it("works with power and abs", (testComplete) => {
    var ex = ply()
      .apply('number', 256)
      .apply('four', $('number').power(0.5).power(0.5))
      .apply('one', $('four').power(0))
      .apply('reciprocal', $('four').power(-1))
      .apply('negative', -4)
      .apply('positive', 4)
      .apply('absNeg', $('negative').absolute())
      .apply('absPos', $('positive').absolute());

    ex.compute()
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
        testComplete();
      })
      .done();
  });

  it("doesn't fallback if not null", (testComplete) => {
    var ex = $('x').fallback(5);
    ex.compute({ x: 2 })
      .then((v) => {
        expect(v).to.deep.equal(2);
        testComplete();
      })
      .done();
  });

  it("fallback works with datasets", (testComplete) => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Two', 2)
      .apply('EmptyData', ply(ds).filter('false'))
      .apply('SumPrice', '$EmptyData.sum($price)')
      .apply('AvgPrice1', $('EmptyData').average($('price')).fallback(2))
      .apply('AvgPrice2', '$EmptyData.sum($price) / $EmptyData.count()');

    ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "AvgPrice1": 2,
            "AvgPrice2": null,
            "SumPrice": 0,
            "Two": 2
          }
        ]);
        testComplete();
      })
      .done();
  });


  it("works in existing dataset case", (testComplete) => {
    var ds = Dataset.fromJS([
      { cut: 'Good', price: 400 },
      { cut: 'Great', price: 124 },
      { cut: 'Wow', price: 160 }
    ]);

    var ex = ply(ds)
      .apply('priceX2', $('price').multiply(2));

    ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          { cut: 'Good', price: 400, priceX2: 800 },
          { cut: 'Great', price: 124, priceX2: 248 },
          { cut: 'Wow', price: 160, priceX2: 320 }
        ]);
        testComplete();
      })
      .done();
  });

  it("works with filter, select", (testComplete) => {
    var ds = Dataset.fromJS(data);

    var ex = $('ds').filter('$price > 200').select('cut');

    ex.compute({ ds, dummyExternal })
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
        testComplete();
      })
      .done();
  });

  it("works with select, limit", (testComplete) => {
    var ds = Dataset.fromJS(data);

    var ex = $('ds').select('cut').limit(3);

    ex.compute({ ds })
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
        testComplete();
      })
      .done();
  });

  it("works with pure filter", (testComplete) => {
    var ds = Dataset.fromJS(data);

    var ex = ply(ds).filter('$cut == Great');

    ex.compute()
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

        testComplete();
      })
      .done();
  });

  it("works with pure empty filter", (testComplete) => {
    var ds = Dataset.fromJS(data);

    var ex = ply(ds).filter('$cut == Best');

    ex.compute()
      .then((v) => {
        expect(AttributeInfo.toJSs(v.attributes)).to.deep.equal([
          { "name": "time", "type": "TIME" },
          { "name": "cut", "type": "STRING" },
          { "name": "tags", "type": "SET/STRING" },
          { "name": "price", "type": "NUMBER" }
        ]);

        expect(v.toJS()).to.deep.equal([]);

        testComplete();
      })
      .done();
  });

  it("works with various applies", (testComplete) => {
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

    ex.compute()
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
        testComplete();
      })
      .done();
  });

  it("works with quantiles", (testComplete) => {
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

    var p = ex.compute();
    p.then((v) => {
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
      testComplete();
    })
      .done();
  });

  it("works with a basic select", (testComplete) => {
    var ds = Dataset.fromJS(data);

    var ex = ply(ds).select('price', 'cut');

    ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          { cut: 'Good',  price: 400  },
          { cut: 'Good',  price: 300  },
          { cut: 'Great', price: 124  },
          { cut: 'Wow',   price: 160  },
          { cut: 'Wow',   price: 100  },
          { cut: null,    price: null }
        ]);
        testComplete();
      })
      .done();
  });

  it("works with a transformed select", (testComplete) => {
    var ds = Dataset.fromJS(data);

    var ex = ply(ds)
      .apply('[cut]', '"[" ++ $cut ++ "]"')
      .apply('price+1', '$price + 1')
      .select('[cut]', 'price+1');

    ex.compute()
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
        testComplete();
      })
      .done();
  });

  it("works with simple split", (testComplete) => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply(
        'Cuts',
        $('Data').split('$cut', 'Cut')
      );

    ex.compute()
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
        testComplete();
      })
      .done();
  });

  it("works with set split", (testComplete) => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply(
        'Tags',
        $('Data').split('$tags', 'Tag')
          .apply('Count', '$Data.count()')
          .sort('$Count', 'descending')
      );

    ex.compute()
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
        testComplete();
      })
      .done();
  });

  it("works with singleton dataset", (testComplete) => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Two', 2)
      .apply('EmptyData', ply(ds).filter('false'))
      .apply('SumPrice', '$EmptyData.sum($price)')
      .apply('AvgPrice1', '$EmptyData.average($price)')
      .apply('AvgPrice2', '$EmptyData.sum($price) / $EmptyData.count()');

    ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "AvgPrice1": null,
            "AvgPrice2": null,
            "SumPrice": 0,
            "Two": 2
          }
        ]);
        testComplete();
      })
      .done();
  });

  it("works with simple split followed by some simple applies", (testComplete) => {
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

    ex.compute()
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
        testComplete();
      })
      .done();
  });

  it("works with timePart split (non-UTC timezone)", (testComplete) => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply('Count', '$Data.count()')
      .apply(
        'TimeParts',
        $('Data').split("$time.timePart('HOUR_OF_DAY', 'Etc/UTC')", 'Part')
          .apply('Count', '$Data.count()')
      );

    ex.compute()
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
        testComplete();
      })
      .done();
  });

  it("works with timePart split (other timezone)", (testComplete) => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply('Count', '$Data.count()')
      .apply(
        'TimeParts',
        $('Data').split("$time.timePart('HOUR_OF_DAY', 'America/New_York')", 'Part')
          .apply('Count', '$Data.count()')
      );

    ex.compute()
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
        testComplete();
      })
      .done();
  });

  it("works with context", (testComplete) => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply('CountPlusX', '$Data.count() + $x');

    ex.compute({ x: 13 })
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "CountPlusX": 19
          }
        ]);
        testComplete();
      })
      .done();
  });

  it("works with context and split", (testComplete) => {
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

    ex.compute({ x: 13 })
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
        testComplete();
      })
      .done();
  });

  it("works with simple split and sub apply", (testComplete) => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds))
      .apply(
        'Cuts',
        $('Data').split('$cut', 'Cut')
          .apply('Count', $('Data').count())
          .apply('AvgPrice', $('Data').average('$price'))
      );

    ex.compute()
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
        testComplete();
      })
      .done();
  });

  it("works with simple split and sub apply + sort + limit", (testComplete) => {
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

    ex.compute()
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
        testComplete();
      })
      .done();
  });

  it("works with simple filter", (testComplete) => {
    var ds = Dataset.fromJS(data).hide();

    var ex = ply()
      .apply('Data', ply(ds).filter($('price').in(105, 305)))
      .apply('Count', '$Data.count()');

    ex.compute()
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Count": 3
          }
        ]);
        testComplete();
      })
      .done();
  });


  describe("sort test", () => {
    var data = [
      { n: 1 },
      { n: 2 },
      { n: 10 },
      { n: 20 }
    ];

    it("sorts on numbers", (testComplete) => {
      var ds = Dataset.fromJS(data);

      var ex = ply(ds).sort('$n');

      ex.compute()
        .then((v) => {
          expect(v.toJS()).to.deep.equal([
            { n: 1 },
            { n: 2 },
            { n: 10 },
            { n: 20 }
          ]);
          testComplete();
        })
        .done();
    });

    it("sorts on number ranges", (testComplete) => {
      var ds = Dataset.fromJS(data);

      var ex = ply(ds).apply('nr', '$n.numberBucket(1)').select('nr').sort('$nr');

      ex.compute()
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
          testComplete();
        })
        .done();
    });

  });


  describe("it works with re-selects", () => {
    var ds = Dataset.fromJS(data).hide();
    var midData = null;

    it("works with simple group/label and subData filter with applies", (testComplete) => {
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

      ex.compute()
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
          testComplete();
        })
        .done();
    });

    it("re-selects", (testComplete) => {
      var ex = ply(midData)
        .apply('CountOver2', '$Count / 2')
        .apply(
          'Cuts',
          $('Cuts')
            .apply('AvgPrice', '$Data.sum($price) / $Data.count()')
        );

      ex.compute()
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
          testComplete();
        })
        .done();
    });
  });


  describe("joins", () => {
    it("does a join on split", (testComplete) => {
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

      ex.compute()
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
          testComplete();
        })
        .done();
    });
  });
});
