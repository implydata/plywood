{ expect } = require("chai")

{ testHigherObjects } = require("higher-object/build/tester")

plywood = require('../../build/plywood')
{ Dataset, $ } = plywood

describe "Dataset", ->
  it "passes higher object tests", ->
    testHigherObjects(Dataset, [
      [
        { x: 1, y: 2 }
        { x: 2, y: 3 }
      ]

      [
        {
          Void: null
          SoTrue: true
          NotSoTrue: false
          Zero: 0
          Count: 2353
          HowAwesome: { type: 'NUMBER', value: 'Infinity' }
          HowLame: { type: 'NUMBER', value: '-Infinity' }
          HowMuch: {
            type: 'NUMBER_RANGE'
            start: 0
            end: 7
          }
          ToInfinityAndBeyond: {
            type: 'NUMBER_RANGE'
            start: null
            end: null
            bounds: "()"
          }
          SomeDate: {
            type: 'TIME'
            value: new Date('2015-01-26T04:54:10Z')
          }
          SomeTimeRange: {
            type: 'TIME_RANGE'
            start: new Date('2015-01-26T04:54:10Z')
            end:   new Date('2015-01-26T05:00:00Z')
          }
          BestCity: 'San Francisco'
          Vegetables: {
            type: 'SET'
            setType: 'STRING'
            elements: ['Broccoli', 'Brussels sprout', 'Potato']
          }
          FunTimes: {
            type: 'SET'
            setType: 'TIME_RANGE'
            elements: [
              { start: new Date('2015-01-26T04:54:10Z'), end: new Date('2015-01-26T05:00:00Z') }
              { start: new Date('2015-02-20T04:54:10Z'), end: new Date('2015-02-20T05:00:00Z') }
            ]
          }
          SubData: [
            { x: 1, y: 2 }
            { x: 2, y: 3 }
          ]
          hasOwnProperty: 'troll'
        }
      ]

      [
        {
          "Carat": {
            "end": 0.5
            "start": 0.25
            "type": "NUMBER_RANGE"
          }
          "Count": 1360
        }
        {
          "Carat": {
            "end": 0.75
            "start": 0.5
            "type": "NUMBER_RANGE"
          }
          "Count": 919
        }
        {
          "Carat": {
            "end": 1.25
            "start": 1
            "type": "NUMBER_RANGE"
          }
          "Count": 298
        }
      ]

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

  describe "getFullType (NativeDataset)", ->
    it "works in empty case", ->
      expect(Dataset.fromJS([]).getFullType()).to.deep.equal({
        type: "DATASET",
        datasetType: {}
      })

    it "works in singleton case", ->
      expect(Dataset.fromJS([{}]).getFullType()).to.deep.equal({
        type: "DATASET",
        datasetType: {}
      })

    it "works in basic case", ->
      expect(Dataset.fromJS([
        { x: 1, y: "hello", z: new Date(1000) }
        { x: 2, y: "woops", z: new Date(1001) }
      ]).getFullType()).to.deep.equal({
        "type": "DATASET"
        "datasetType": {
          "x": { type: "NUMBER" }
          "y": { type: "STRING" }
          "z": { type: "TIME" }
        }
      })

    it "works in nested case", ->
      expect(Dataset.fromJS([
        {
          x: 1
          y: "hello"
          z: new Date(1000)
          subData: [
            { a: 50.5, b: 'woop' }
            { a: 50.6, b: 'w00p' }
          ]
        }
        {
          x: 2
          y: "woops"
          z: new Date(1001)
          subData: [
            { a: 51.5, b: 'Woop' }
            { a: 51.6, b: 'W00p' }
          ]
        }
      ]).getFullType()).to.deep.equal({
        type: "DATASET"
        datasetType: {
          "subData": {
            type: "DATASET"
            datasetType: {
              "a": {type: "NUMBER"}
              "b": {type: "STRING"}
            }
          }
          "x": {type: "NUMBER"}
          "y": {type: "STRING"}
          "z": {type: "TIME"}
        }
      })
