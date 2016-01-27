{ expect } = require("chai")

{ testImmutableClass } = require("immutable-class/build/tester")

plywood = require('../../build/plywood')
{ TimeRange, $, ply, r } = plywood

describe "TimeRange", ->
  it "is immutable class", ->
    testImmutableClass(TimeRange, [
      {
        start: new Date('2015-01-26T04:54:10Z')
        end:   new Date('2015-01-26T05:54:10Z')
      }
      {
        start: new Date('2015-01-26T04:54:10Z')
        end:   new Date('2015-01-26T05:00:00Z')
      }
    ])

  describe "does not die with hasOwnProperty", ->
    it "survives", ->
      expect(TimeRange.fromJS({
        start: new Date('2015-01-26T04:54:10Z')
        end:   new Date('2015-01-26T05:54:10Z')
        hasOwnProperty: 'troll'
      }).toJS()).to.deep.equal({
        start: new Date('2015-01-26T04:54:10Z')
        end:   new Date('2015-01-26T05:54:10Z')
      })

  describe "upgrades", ->
    it "upgrades from a string", ->
      timeRange = TimeRange.fromJS({
        start: '2015-01-26T04:54:10Z'
        end:   '2015-01-26T05:00:00Z'
      })
      expect(timeRange.start.valueOf()).to.equal(Date.parse('2015-01-26T04:54:10Z'))
      expect(timeRange.end.valueOf()  ).to.equal(Date.parse('2015-01-26T05:00:00Z'))


  describe "#union()", ->
    it 'works correctly with a non-disjoint range', ->
      expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-26T02:00:00' }).union(TimeRange.fromJS({ start: '2015-01-26T01:00:00', end: '2015-01-26T03:00:00' })).toJS()
      ).to.deep.equal({ start: new Date('2015-01-26T00:00:00'), end: new Date('2015-01-26T03:00:00') })

    it 'works correctly with a disjoint range', ->
      expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-26T01:00:00' }).union(TimeRange.fromJS({ start: '2015-01-26T02:00:00', end: '2015-01-26T03:00:00' }))
      ).to.deep.equal(null)

    it 'works correctly with a adjacent range', ->
      expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-26T01:00:00' }).union(TimeRange.fromJS({ start: '2015-01-26T01:00:00', end: '2015-01-26T02:00:00' })).toJS()
      ).to.deep.equal({ start: new Date('2015-01-26T00:00:00'), end: new Date('2015-01-26T02:00:00') })


  describe "#intersect()", ->
    it 'works correctly with a non-disjoint range', ->
      expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-26T02:00:00' }).intersect(TimeRange.fromJS({ start: '2015-01-26T01:00:00', end: '2015-01-26T03:00:00' })).toJS()
      ).to.deep.equal({ start: new Date('2015-01-26T01:00:00'), end: new Date('2015-01-26T02:00:00') })

    it 'works correctly with a disjoint range', ->
      expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-26T01:00:00' }).intersect(TimeRange.fromJS({ start: '2015-01-26T02:00:00', end: '2015-01-26T03:00:00' }))
      ).to.deep.equal(null)

    it 'works correctly with a adjacent range', ->
      expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-26T01:00:00' }).intersect(TimeRange.fromJS({ start: '2015-01-26T01:00:00', end: '2015-01-26T02:00:00' })).toJS()
      ).to.deep.equal({ start: new Date(0), end: new Date(0) })


  describe "#toInterval", ->
    it "works in general", ->
      timeRange = TimeRange.fromJS({
        start: '2015-01-26T04:54:10Z'
        end:   '2015-01-26T05:00:00Z'
      })
      expect(timeRange.toInterval()).to.equal('2015-01-26T04:54:10/2015-01-26T05')

    it 'works on a round interval', ->
      expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-27T00:00:00' }).toInterval()
      ).to.deep.equal('2015-01-26/2015-01-27')

    it 'works on a non round interval', ->
      expect(
        TimeRange.fromJS({ start: '2015-01-26T12:34:56', end: '2015-01-27T11:22:33' }).toInterval()
      ).to.deep.equal('2015-01-26T12:34:56/2015-01-27T11:22:33')

    it 'works on an interval with different bounds', ->
      expect(
        TimeRange.fromJS({ start: '2015-01-26T00:00:00', end: '2015-01-27T00:00:00', bounds: '(]' }).toInterval()
      ).to.deep.equal('2015-01-26T00:00:00.001/2015-01-27T00:00:00.001')
