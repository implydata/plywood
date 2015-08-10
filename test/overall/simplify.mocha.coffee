{ expect } = require("chai")

{ testHigherObjects } = require("higher-object/build/tester")

{ WallTime } = require('chronology')
if not WallTime.rules
  tzData = require("chronology/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

plywood = require('../../build/plywood')
{ Expression, $, literal, TimeRange, NumberRange } = plywood

describe "Simplify", ->
  it "simplifies to number", ->
    ex = $(5).add(1).subtract(4)
    expect(ex.simplify().toJS()).to.deep.equal({
      op: 'literal'
      value: 2
    })

  it "simplifies literal prefix", ->
    ex = $(5).add(1).subtract(4).multiply('$x')
    ex2 = $(2).multiply('$x')
    expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())


  describe 'add', ->
    it "removes 0 in simple case", ->
      ex = $('x').add(0)
      ex2 = $('x')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "removes 0 complex case", ->
      ex = $('x').add(0, '$y', 0, '$z')
      ex2 = $('x').add('$y', '$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "removes leading 0", ->
      ex = $(0).add('$y', '$z')
      ex2 = $('y').add('$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "works in nested expression case", ->
      ex = $('x').add('0 + $y + 0 + $z')
      ex2 = $('x').add('$y', '$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "works with nested add", ->
      ex = $('x').add('2 * $y + $z')
      ex2 = $('x').add('2 * $y', '$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())


  describe 'multiply', ->
    it "collapses 0 in simple case", ->
      ex = $('x').multiply(0)
      ex2 = $(0)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "collapses 0 in complex case", ->
      ex = $('x').multiply(6, '$y', 0, '$z')
      ex2 = $(0)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "collapses leading 0", ->
      ex = $(0).multiply(6, '$y', '$z')
      ex2 = $(0)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "removes 1 in simple case", ->
      ex = $('x').multiply(1)
      ex2 = $('x')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "removes 1 complex case", ->
      ex = $('x').multiply(1, '$y', 1, '$z')
      ex2 = $('x').multiply('$y', '$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "removes leading 1", ->
      ex = $(1).multiply('$y', '$z')
      ex2 = $('y').multiply('$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "works in nested expression case", ->
      ex = $('x').multiply('1 * $y * 1 * $z')
      ex2 = $('x').multiply('$y', '$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "works with nested add", ->
      ex = $('x').multiply('(1 + $y) * $z')
      ex2 = $('x').multiply('1 + $y', '$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())


  describe 'and', ->
    it "collapses false in simple case", ->
      ex = $('x').and(false)
      ex2 = $(false)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "collapses false in complex case", ->
      ex = $('x').and('$y', false, '$z')
      ex2 = $(false)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "collapses leading false", ->
      ex = $(false).and('$y', '$z')
      ex2 = $(false)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "removes true in simple case", ->
      ex = $('x').and(true)
      ex2 = $('x')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "removes true complex case", ->
      ex = $('x').and(true, '$y', true, '$z')
      ex2 = $('x').and('$y', '$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "removes leading true", ->
      ex = $(true).and('$y', '$z')
      ex2 = $('y').and('$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "works in nested expression case", ->
      ex = $('x').and('true and $y and true and $z')
      ex2 = $('x').and('$y', '$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "works with nested add", ->
      ex = $('x').and('($a or $b) and $z')
      ex2 = $('x').and('$a or $b', '$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "works with different filters", ->
      ex = $('flight').is(5).and($('flight').is(7))
      ex2 = $(false)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "works with same filters", ->
      ex = $('flight').is(5).and($('flight').is(5))
      ex2 = $('flight').is(7)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "works with IS an IN", ->
      ex = $('flight').is(5).and($('flight').in(new NumberRange({start: 5, end: 7})))
      ex2 = $('flight').is(5)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())


  describe 'or', ->
    it "collapses true in simple case", ->
      ex = $('x').or(true)
      ex2 = $(true)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "collapses true in complex case", ->
      ex = $('x').or('$y', true, '$z')
      ex2 = $(true)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "collapses leading true", ->
      ex = $(true).or('$y', '$z')
      ex2 = $(true)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "removes false in simple case", ->
      ex = $('x').or(false)
      ex2 = $('x')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "removes false complex case", ->
      ex = $('x').or(false, '$y', false, '$z')
      ex2 = $('x').or('$y', '$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "removes leading false", ->
      ex = $(false).or('$y', '$z')
      ex2 = $('y').or('$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "works in nested expression case", ->
      ex = $('x').or('false or $y or false or $z')
      ex2 = $('x').or('$y', '$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "works with nested add", ->
      ex = $('x').or('($a and $b) or $z')
      ex2 = $('x').or('$a and $b', '$z')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "works with different filters", ->
      ex = $('flight').is(5).or($('flight').is(7))
      ex2 = $('flight').in([5, 7])
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "works with same filters", ->
      ex = $('flight').is(5).or($('flight').is(5))
      ex2 = $('flight').is(5)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it "works with IS an IN", ->
      ex = $('flight').is(5).or($('flight').in(new NumberRange({start: 5, end: 7})))
      ex2 = $('flight').is($('flight').in(new NumberRange({start: 5, end: 7})))
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())


  describe.only 'is', ->
    it "simplifies to false", ->
      ex = $(5).is(8)
      expect(ex.simplify().toJS()).to.deep.equal({
        op: 'literal'
        value: false
      })

    it "simplifies to true", ->
      ex = $(5).is(5)
      expect(ex.simplify().toJS()).to.deep.equal({
        op: 'literal'
        value: true
      })

    it "simplifies to true", ->
      ex = $('x').is('$x')
      expect(ex.simplify().toJS()).to.deep.equal({
        op: 'literal'
        value: true
      })

    it 'swaps literal', ->
      ex = literal("Honda").is('$x')
      ex2 = $('x').is('Honda')
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())


  describe 'match', ->
    it 'with false value', ->
      ex = literal("Honda").match('^\\d+')
      ex2 = $(false)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it 'with true value', ->
      ex = literal("123").match('^\\d+')
      ex2 = $(true)
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

    it 'with reference value', ->
      ex = $('test').match('^\\d+')
      expect(ex.simplify().toJS()).to.deep.equal(ex.toJS())


  describe 'timeOffset', ->
    it 'with simple expression', ->
      ex = literal(new Date('2015-02-20T15:41:12')).timeOffset('P1D', 'Etc/UTC')
      ex2 = $(new Date('2015-02-21T15:41:12'))
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())


  describe 'timeBucket', ->
    it 'with simple expression', ->
      ex = literal(new Date("2015-02-19T05:59:02.822Z")).timeBucket('P1D', 'Etc/UTC')
      ex2 = literal(TimeRange.fromJS({
        start: new Date("2015-02-19T00:00:00.000Z")
        end: new Date("2015-02-20T00:00:00.000Z")
      }))
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())


  describe 'numberBucket', ->
    it 'with simple expression', ->
      ex = literal(1.03).numberBucket(0.05, 0.02)
      ex2 = literal(NumberRange.fromJS({
        start: 1.02
        end: 1.07
      }))
      expect(ex.simplify().toJS()).to.deep.equal(ex2.toJS())

