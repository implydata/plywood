{ expect } = require("chai")

{ testHigherObjects } = require("higher-object/build/tester")

plywood = require('../../build/plywood')
{ External, $ } = plywood

describe "External", ->
  it "passes higher object tests", ->
    testHigherObjects(External, [
      {
        source: 'druid',
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
        source: 'druid',
        dataSource: 'wiki',
        timeAttribute: 'time',
        allowEternity: true,
        allowSelectQueries: true,
        exactResultsOnly: true,
        context: null
      }

      {
        source: 'druid',
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
      expect(Dataset.fromJS({
        source: 'druid',
        dataSource: 'wiki',
        timeAttribute: 'time',
        context: null,
        hasOwnProperty: 'troll'
      }).toJS()).to.deep.equal({
        source: 'druid',
        dataSource: 'wiki',
        timeAttribute: 'time',
        context: null
      })
