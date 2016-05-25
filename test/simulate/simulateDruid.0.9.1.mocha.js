var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { Expression, External, Dataset, TimeRange, $, ply, r } = plywood;

var attributes = [
  { name: 'time', type: 'TIME' },
  { name: 'color', type: 'STRING' },
  { name: 'cut', type: 'STRING' },
  { name: 'isNice', type: 'BOOLEAN' },
  { name: 'tags', type: 'SET/STRING' },
  { name: 'carat', type: 'NUMBER' },
  { name: 'height_bucket', type: 'NUMBER' },
  { name: 'price', type: 'NUMBER', unsplitable: true },
  { name: 'tax', type: 'NUMBER', unsplitable: true },
  { name: 'vendor_id', special: 'unique', unsplitable: true },
];

var context = {
  'diamonds': External.fromJS({
    engine: 'druid',
    version: '0.9.1',
    source: 'diamonds',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true,
    filter: $("time").in({
      start: new Date('2015-03-12T00:00:00'),
      end: new Date('2015-03-19T00:00:00')
    })
  })
};

describe("simulate Druid 0.9.1", () => {

  it("makes a filter on timePart", () => {
    var ex = $("diamonds").filter(
      $("time").timePart('HOUR_OF_DAY', 'Etc/UTC').in([3, 4, 10]).and($("time").in([
        TimeRange.fromJS({ start: new Date('2015-03-12T00:00:00'), end: new Date('2015-03-15T00:00:00') }),
        TimeRange.fromJS({ start: new Date('2015-03-16T00:00:00'), end: new Date('2015-03-18T00:00:00') })
      ]))
    ).split("$color", 'Color')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending')
      .limit(10);

    expect(() => {
      ex.simulateQueryPlan(context)
    }).to.throw('can not do secondary filtering on primary time dimension (https://github.com/druid-io/druid/issues/2816)');
  });

  it("splits on timePart with sub split", () => {
    var ex = $("diamonds").split($("time").timePart('HOUR_OF_DAY', 'Etc/UTC'), 'hourOfDay')
      .apply('Count', '$diamonds.count()')
      .sort('$Count', 'descending')
      .limit(3)
      .apply(
        'Colors',
        $("diamonds").split("$color", 'Color')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(10)
      );

    expect(() => {
      ex.simulateQueryPlan(context)
    }).to.throw('can not do secondary filtering on primary time dimension (https://github.com/druid-io/druid/issues/2816)');
  });

});
