var { expect } = require("chai");

var { testImmutableClass } = require("immutable-class/build/tester");

var plywood = require('../../build/plywood');
var { TimeRange, $, ply, r } = plywood;

describe("TimeRange", function() {
  it("is immutable class", function() {
    return testImmutableClass(TimeRange, [
      {
        start: new Date('2015-01-26T04:54:10Z'),
        end:   new Date('2015-01-26T05:54:10Z')
      },
      {
        start: new Date('2015-01-26T04:54:10Z'),
        end:   new Date('2015-01-26T05:00:00Z')
      }
    ]);
  });

  describe("does not die with hasOwnProperty", function() {
    return it("survives", function() {
      return expect(TimeRange.fromJS({
        start: new Date('2015-01-26T04:54:10Z'),
        end:   new Date('2015-01-26T05:54:10Z'),
        hasOwnProperty: 'troll'
      }).toJS()).to.deep.equal({
        start: new Date('2015-01-26T04:54:10Z'),
        end:   new Date('2015-01-26T05:54:10Z')
      });
    });
  });

  describe("upgrades", function() {
    return it("upgrades from a string", function() {
      var timeRange = TimeRange.fromJS({
        start: '2015-01-26T04:54:10Z',
        end:   '2015-01-26T05:00:00Z'
      });
      expect(timeRange.start.valueOf()).to.equal(Date.parse('2015-01-26T04:54:10Z'));
      return expect(timeRange.end.valueOf()  ).to.equal(Date.parse('2015-01-26T05:00:00Z'));
    });
  });


  describe("#union()", function() {
    it('works correctly with a non-disjoint range', function() {
      return expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-26T02:00:00' }).union(TimeRange.fromJS({ start: '2015-01-26T01:00:00', end: '2015-01-26T03:00:00' })).toJS()
      ).to.deep.equal({ start: new Date('2015-01-26T00:00:00'), end: new Date('2015-01-26T03:00:00') });
    });

    it('works correctly with a disjoint range', function() {
      return expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-26T01:00:00' }).union(TimeRange.fromJS({ start: '2015-01-26T02:00:00', end: '2015-01-26T03:00:00' }))
      ).to.deep.equal(null);
    });

    return it('works correctly with a adjacent range', function() {
      return expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-26T01:00:00' }).union(TimeRange.fromJS({ start: '2015-01-26T01:00:00', end: '2015-01-26T02:00:00' })).toJS()
      ).to.deep.equal({ start: new Date('2015-01-26T00:00:00'), end: new Date('2015-01-26T02:00:00') });
    });
  });


  describe("#intersect()", function() {
    it('works correctly with a non-disjoint range', function() {
      return expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-26T02:00:00' }).intersect(TimeRange.fromJS({ start: '2015-01-26T01:00:00', end: '2015-01-26T03:00:00' })).toJS()
      ).to.deep.equal({ start: new Date('2015-01-26T01:00:00'), end: new Date('2015-01-26T02:00:00') });
    });

    it('works correctly with a disjoint range', function() {
      return expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-26T01:00:00' }).intersect(TimeRange.fromJS({ start: '2015-01-26T02:00:00', end: '2015-01-26T03:00:00' }))
      ).to.deep.equal(null);
    });

    return it('works correctly with a adjacent range', function() {
      return expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-26T01:00:00' }).intersect(TimeRange.fromJS({ start: '2015-01-26T01:00:00', end: '2015-01-26T02:00:00' })).toJS()
      ).to.deep.equal({ start: new Date(0), end: new Date(0) });
    });
  });


  return describe("#toInterval", function() {
    it("works in general", function() {
      var timeRange = TimeRange.fromJS({
        start: '2015-01-26T04:54:10Z',
        end:   '2015-01-26T05:00:00Z'
      });
      return expect(timeRange.toInterval()).to.equal('2015-01-26T04:54:10/2015-01-26T05');
    });

    it('works on a round interval', function() {
      return expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-27T00:00:00' }).toInterval()
      ).to.deep.equal('2015-01-26/2015-01-27');
    });

    it('works on a non round interval', function() {
      return expect(
        TimeRange.fromJS({ start: '2015-01-26T12:34:56', end: '2015-01-27T11:22:33' }).toInterval()
      ).to.deep.equal('2015-01-26T12:34:56/2015-01-27T11:22:33');
    });

    return it('works on an interval with different bounds', function() {
      return expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-27T00:00:00', bounds: '(]' }).toInterval()
      ).to.deep.equal('2015-01-26T00:00:00.001/2015-01-27T00:00:00.001');
    });
  });
});
