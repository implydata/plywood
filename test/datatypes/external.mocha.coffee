{ expect } = require("chai")

{ testHigherObjects } = require("higher-object/build/tester")

{ WallTime } = require('chronology')
if not WallTime.rules
  tzData = require("chronology/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

plywood = require('../../build/plywood')
{ Expression, Dataset, External, TimeRange, $ } = plywood

wikiDataset = External.fromJS({
  engine: 'druid',
  dataSource: 'wikipedia_editstream',
  timeAttribute: 'time',
  context: null
  attributes: {
    time: { type: 'TIME' }
    language: { type: 'STRING' }
    user: { type: 'STRING' }
    page: { type: 'STRING' }
    added: { type: 'NUMBER' }
  }
})

context = {
  wiki: wikiDataset.addFilter($('time').in(TimeRange.fromJS({
    start: new Date("2013-02-26T00:00:00Z")
    end: new Date("2013-02-27T00:00:00Z")
  })))
  wikiCmp: wikiDataset.addFilter($('time').in(TimeRange.fromJS({
    start: new Date("2013-02-25T00:00:00Z")
    end: new Date("2013-02-26T00:00:00Z")
  })))
}

describe "External", ->
  it "passes higher object tests", ->
    testHigherObjects(External, [
      {
        engine: 'druid',
        dataSource: 'moon_child',
        timeAttribute: 'time',
        context: null
        attributes: {
          color: { type: 'STRING' }
          cut: { type: 'STRING' }
          carat: { type: 'STRING' }
          price: { type: 'NUMBER', filterable: false, splitable: false }
        }
      }

      {
        engine: 'druid',
        dataSource: 'wiki',
        timeAttribute: 'time',
        allowEternity: true,
        allowSelectQueries: true,
        exactResultsOnly: true,
        context: null
      }

      {
        engine: 'druid',
        dataSource: 'moon_child2', # ToDo: remove the 2 and fix the equality test
        timeAttribute: 'time',
        context: null
        attributeOverrides: {
          color: { type: 'STRING' }
          cut: { type: 'STRING' }
          unique: { type: "STRING", special: 'unique' }
        }
      }
    ], {
      newThrows: true
    })

  describe "does not die with hasOwnProperty", ->
    it "survives", ->
      expect(External.fromJS({
        engine: 'druid',
        dataSource: 'wiki',
        timeAttribute: 'time',
        context: null,
        hasOwnProperty: 'troll'
      }).toJS()).to.deep.equal({
        engine: 'druid',
        dataSource: 'wiki',
        timeAttribute: 'time',
        context: null
      })


  describe.only "simplifies / digests", ->
    it "a total", ->
      ex = $()
        .apply("wiki",
          $('^wiki')
            .apply('addedTwice', '$added * 2')
            .filter($("language").is('en'))
        )
        .apply('Count', '$wiki.count()')
        .apply('TotalAdded', '$wiki.sum($added)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('literal')
      remoteDataset = ex.value
      expect(remoteDataset.derivedAttributes).to.have.all.keys(['addedTwice']);
      expect(remoteDataset.defs).to.have.length(0)
      expect(remoteDataset.applies).to.have.length(2)
      expect(remoteDataset.toJS().attributes).to.deep.equal({
        Count: { "type": "NUMBER" },
        TotalAdded: { "type": "NUMBER" }
      })

      expect(remoteDataset.simulate().toJS()).to.deep.equal([
        {
          "Count": 4
          "TotalAdded": 4
        }
      ])

    return
    it "a split on string", ->
      ex = $('wiki').split("$page", 'Page')
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5)

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('literal')
      remoteDataset = ex.value
      expect(remoteDataset.defs).to.have.length(0)
      expect(remoteDataset.applies).to.have.length(2)
      expect(remoteDataset.limit.limit).to.equal(5)
      expect(remoteDataset.toJS().attributes).to.deep.equal({
        Page: { "type": "STRING" },
        Count: { "type": "NUMBER" },
        Added: { "type": "NUMBER" }
      })

      expect(remoteDataset.simulate().toJS()).to.deep.equal([
        "Added": 4
        "Count": 4
        "Page": "some_page"
      ])

    it "a split on string with multiple limits in ascending order", ->
      ex = $('wiki').split("$page", 'Page')
        .apply('Count', '$wiki.count()')
        .sort('$Count', 'descending')
        .limit(5)
        .apply('Added', '$wiki.sum($added)')
        .limit(9)

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('literal')
      remoteDataset = ex.value
      expect(remoteDataset.defs).to.have.length(0)
      expect(remoteDataset.applies).to.have.length(2)
      expect(remoteDataset.limit.limit).to.equal(5)
      expect(remoteDataset.toJS().attributes).to.deep.equal({
        Page: { "type": "STRING" },
        Count: { "type": "NUMBER" },
        Added: { "type": "NUMBER" }
      })

    it "a split on string with multiple limits in descending order", ->
      ex = $('wiki').split("$page", 'Page')
        .apply('Count', '$wiki.count()')
        .sort('$Count', 'descending')
        .limit(9)
        .apply('Added', '$wiki.sum($added)')
        .limit(5)

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('literal')
      remoteDataset = ex.value
      expect(remoteDataset.defs).to.have.length(0)
      expect(remoteDataset.applies).to.have.length(2)
      expect(remoteDataset.limit.limit).to.equal(5)
      expect(remoteDataset.toJS().attributes).to.deep.equal({
        Page: { "type": "STRING" },
        Count: { "type": "NUMBER" },
        Added: { "type": "NUMBER" }
      })

    it "a split on time", ->
      ex = $('wiki').split($("time").timeBucket('P1D', 'America/Los_Angeles'), 'Timestamp')
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5)

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('actions')
      expect(ex.actions).to.have.length(2)
      remoteDataset = ex.operand.value
      expect(remoteDataset.defs).to.have.length(0)
      expect(remoteDataset.applies).to.have.length(2)
      expect(remoteDataset.toJS().attributes).to.deep.equal({
        Timestamp: { "type": "TIME_RANGE" },
        Count: { "type": "NUMBER" },
        Added: { "type": "NUMBER" }
      })

      expect(remoteDataset.simulate().toJS()).to.deep.equal([
        {
          "Added": 4
          "Count": 4
          "Timestamp": {
            "start": new Date('2015-03-13T07:00:00.000Z')
            "end": new Date('2015-03-14T07:00:00.000Z')
            "type": "TIME_RANGE"
          }
        }
      ])

    it "a filtered split on string", ->
      ex = $('wiki').filter('$language = "en"').group("$page").label('Page')
        .apply('wiki', '$wiki.filter($language = "en").filter($page = $^Page)')
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5)

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('literal')
      remoteDataset = ex.value
      expect(remoteDataset.filter.toString()).to.equal('($language:STRING = "en" and $time in [2013-02-26T00:00:00.000Z,2013-02-27T00:00:00.000Z))')
      expect(remoteDataset.defs).to.have.length(0)
      expect(remoteDataset.applies).to.have.length(2)
      expect(remoteDataset.toJS().attributes).to.deep.equal({
        Page: { "type": "STRING" },
        Count: { "type": "NUMBER" },
        Added: { "type": "NUMBER" }
      })

      expect(remoteDataset.simulate().toJS()).to.deep.equal([
        "Added": 4
        "Count": 4
        "Page": "some_page"
      ])

    it "a total and a split", ->
      ex = $()
        .apply("wiki",
          $('^wiki')
            .apply('addedTwice', '$added * 2')
            .filter($("language").is('en'))
        )
        .apply('Count', '$wiki.count()')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply('Pages',
          $('wiki').split("$page", 'Page')
            .apply('Count', '$wiki.count()')
            .apply('Added', '$wiki.sum($added)')
            .sort('$Count', 'descending')
            .limit(5)
        )

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('actions')
      expect(ex.actions).to.have.length(2)

      remoteDataset = ex.operand.value
      expect(remoteDataset.defs).to.have.length(0)
      expect(remoteDataset.applies).to.have.length(2)
      expect(remoteDataset.toJS().attributes).to.deep.equal({
        Count: { "type": "NUMBER" },
        TotalAdded: { "type": "NUMBER" }
      })

    it "a total and a split in a strange order", ->
      ex = $()
        .apply("wiki",
          $('^wiki')
            .apply('addedTwice', '$added * 2')
            .filter($("language").is('en'))
        )
        .apply('Count', '$wiki.count()')
        .apply('Pages',
          $('wiki').split("$page", 'Page')
            .apply('Count', '$wiki.count()')
            .apply('Added', '$wiki.sum($added)')
            .sort('$Count', 'descending')
            .limit(5)
        )
        .apply('TotalAdded', '$wiki.sum($added)')

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('actions')
      expect(ex.actions).to.have.length(2)

      remoteDataset = ex.operand.value
      expect(remoteDataset.defs).to.have.length(0)
      expect(remoteDataset.applies).to.have.length(2)
      expect(remoteDataset.toJS().attributes).to.deep.equal({
        Count: { "type": "NUMBER" },
        TotalAdded: { "type": "NUMBER" }
      })

    it "a split and another split in a strange order", ->
      ex = $('wiki').split("$page", 'Page')
        .apply('Count', '$wiki.count()')
        .sort('$Count', 'descending')
        .apply('Users'
          $('wiki').split("$user", 'User')
            .apply('Count', '$wiki.count()')
            .sort('$Count', 'descending')
            .limit(3)
        )
        .apply('Added', '$wiki.sum($added)')
        .limit(5)

      ex = ex.referenceCheck(context).resolve(context).simplify()

      expect(ex.op).to.equal('actions')
      expect(ex.actions).to.have.length(2)

      remoteDataset = ex.operand.value
      expect(remoteDataset.defs).to.have.length(0)
      expect(remoteDataset.applies).to.have.length(2)
      expect(remoteDataset.limit.limit).to.equal(5)
      expect(remoteDataset.toJS().attributes).to.deep.equal({
        Page: { "type": "STRING" },
        Count: { "type": "NUMBER" },
        Added: { "type": "NUMBER" }
      })

    it "a union of two groups", ->
      ex = $('wiki').group('$page').union($('wikiCmp').group('$page')).label('Page')
        .apply('wiki', '$wiki.filter($page = $^Page)')
        .apply('wikiCmp', '$wikiCmp.filter($page = $^Page)')
        .apply('Count', '$wiki.count()')
        .apply('CountDiff', '$wiki.count() - $wikiCmp.count()')
        .sort('$CountDiff', 'descending')
        .limit(5)

      ex = ex.referenceCheck(context).resolve(context).simplify()

      # console.log("ex.toString()", ex.toString());

      expect(ex.op).to.equal('actions')
      expect(ex.operand.op).to.equal('join')

      remoteDatasetMain = ex.operand.lhs.value
      expect(remoteDatasetMain.defs).to.have.length(0)
      expect(remoteDatasetMain.applies).to.have.length(2)
      expect(remoteDatasetMain.toJS().attributes).to.deep.equal({
        Count: { "type": "NUMBER" }
        Page: { "type": "STRING" }
        _br_0: { "type": "NUMBER" }
      })

      remoteDatasetCmp = ex.operand.rhs.value
      expect(remoteDatasetCmp.defs).to.have.length(1)
      expect(remoteDatasetCmp.applies).to.have.length(1)
      expect(remoteDatasetCmp.toJS().attributes).to.deep.equal({
        Page: { "type": "STRING" }
        _br_1: { "type": "NUMBER" }
      })

      expect(ex.actions[0].toString()).to.equal('.apply(CountDiff, ($_br_0 + $_br_1))')
