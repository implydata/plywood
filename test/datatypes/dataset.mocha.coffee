{ expect } = require("chai")

{ testImmutableClass } = require("immutable-class/build/tester")

plywood = require('../../build/plywood')
{ Dataset, $, ply } = plywood

describe "Dataset", ->
  it "passes higher object tests", ->
    testImmutableClass(Dataset, [
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
    ])


  describe "getFullType", ->
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
        { x: 2, y: "world", z: new Date(1001) }
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


  describe "methods", ->
    carDataset = Dataset.fromJS([
      {
        time: new Date('2015-01-04T12:32:43')
        make: 'Honda'
        model: 'Civic'
        price: 10000
      }
      {
        time: new Date('2015-01-04T14:00:40')
        make: 'Toyota'
        model: 'Prius'
        price: 20000
      }
    ])

    carAndPartsDataset = Dataset.fromJS([
      {
        time: new Date('2015-01-04T12:32:43')
        make: 'Honda'
        model: 'Civic'
        price: 10000
        parts: [
          { part: 'Engine', weight: 500 }
          { part: 'Door', weight: 20 }
        ]
      }
      {
        time: new Date('2015-01-04T14:00:40')
        make: 'Toyota'
        model: 'Prius'
        price: 20000
        parts: [
          { part: 'Engine', weight: 400 }
          { part: 'Door', weight: 25 }
        ]
      }
    ])

    carTotalAndSubSplitDataset = Dataset.fromJS([
      {
        price: 10000
        weight: 1000
        ByMake: [
          {
            make: 'Honda'
            price: 12000
            weight: 1200
            ByModel: [
              {
                model: 'Civic'
                price: 11000
                weight: 1100
              }
              {
                model: 'Accord'
                price: 13000
                weight: 1300
              }
            ]
          }
          {
            make: 'Toyota'
            price: 12000
            weight: 1200
            ByModel: [
              {
                model: 'Prius'
                price: 11000
                weight: 1100
              }
              {
                model: 'Corolla'
                price: 13000
                weight: 1300
              }
            ]
          }
        ]
      }
    ])

    timeSeriesResult = Dataset.fromJS([
      {
        "count": 31427,
        "added": 6686857,
        "Split": [
          {
            "Segment": {
              "start": "2013-02-26T16:00:00.000Z",
              "end": "2013-02-26T17:00:00.000Z",
              "type": "TIME_RANGE"
            },
            "count": 2012,
            "added": 373390,
          },
          {
            "Segment": {
              "start": "2013-02-26T01:00:00.000Z",
              "end": "2013-02-26T02:00:00.000Z",
              "type": "TIME_RANGE"
            },
            "count": 1702,
            "added": 181266,
          },
          {
            "Segment": {
              "start": "2013-02-26T15:00:00.000Z",
              "end": "2013-02-26T16:00:00.000Z",
              "type": "TIME_RANGE"
            },
            "count": 1625,
            "added": 284339,
          },
        ]
      }
    ])

    describe "#getColumns", ->
      it "works with basic dataset", ->
        expect(carDataset.getColumns()).to.deep.equal([
          {
            "name": "time"
            "type": "TIME"
          }
          {
            "name": "make"
            "type": "STRING"
          }
          {
            "name": "model"
            "type": "STRING"
          }
          {
            "name": "price"
            "type": "NUMBER"
          }
        ])

      it "works with sub-dataset without prefix", ->
        expect(carAndPartsDataset.getColumns()).to.deep.equal([
          {
            "name": "time"
            "type": "TIME"
          }
          {
            "name": "make"
            "type": "STRING"
          }
          {
            "name": "model"
            "type": "STRING"
          }
          {
            "name": "part"
            "type": "STRING"
          }
          {
            "name": "price"
            "type": "NUMBER"
          }
          {
            "name": "weight"
            "type": "NUMBER"
          }
        ])

      it "works with sub-dataset with prefix", ->
        expect(carAndPartsDataset.getColumns({ prefixColumns: true })).to.deep.equal([
          {
            "name": "time"
            "type": "TIME"
          }
          {
            "name": "make"
            "type": "STRING"
          }
          {
            "name": "model"
            "type": "STRING"
          }
          {
            "name": "parts.part"
            "type": "STRING"
          }
          {
            "name": "parts.weight"
            "type": "NUMBER"
          }
          {
            "name": "price"
            "type": "NUMBER"
          }
        ])

      it "works with total and sub-split", ->
        expect(carTotalAndSubSplitDataset.getColumns()).to.deep.equal([
          {
            "name": "make"
            "type": "STRING"
          }
          {
            "name": "model"
            "type": "STRING"
          }
          {
            "name": "price"
            "type": "NUMBER"
          }
          {
            "name": "weight"
            "type": "NUMBER"
          }
        ])


    describe "#flatten", ->
      it "works with basic dataset", ->
        expect(carDataset.flatten()).to.deep.equal([
          {
            "make": "Honda"
            "model": "Civic"
            "price": 10000
            "time": new Date("2015-01-04T12:32:43.000Z")
          }
          {
            "make": "Toyota"
            "model": "Prius"
            "price": 20000
            "time": new Date("2015-01-04T14:00:40.000Z")
          }
        ])

      it "works with sub-dataset with prefix", ->
        expect(carAndPartsDataset.flatten({ prefixColumns: true })).to.deep.equal([
          {
            "make": "Honda"
            "model": "Civic"
            "parts.part": "Engine"
            "parts.weight": 500
            "price": 10000
            "time": new Date("2015-01-04T12:32:43.000Z")
          }
          {
            "make": "Honda"
            "model": "Civic"
            "parts.part": "Door"
            "parts.weight": 20
            "price": 10000
            "time": new Date("2015-01-04T12:32:43.000Z")
          }
          {
            "make": "Toyota"
            "model": "Prius"
            "parts.part": "Engine"
            "parts.weight": 400
            "price": 20000
            "time": new Date("2015-01-04T14:00:40.000Z")
          }
          {
            "make": "Toyota"
            "model": "Prius"
            "parts.part": "Door"
            "parts.weight": 25
            "price": 20000
            "time": new Date("2015-01-04T14:00:40.000Z")
          }
        ])

      it "works with total and sub-split", ->
        expect(carTotalAndSubSplitDataset.flatten()).to.deep.equal([
          {
            "make": "Honda"
            "model": "Civic"
            "price": 11000
            "weight": 1100
          }
          {
            "make": "Honda"
            "model": "Accord"
            "price": 13000
            "weight": 1300
          }
          {
            "make": "Toyota"
            "model": "Prius"
            "price": 11000
            "weight": 1100
          }
          {
            "make": "Toyota"
            "model": "Corolla"
            "price": 13000
            "weight": 1300
          }
        ])

      it "works with total and sub-split with postorder", ->
        expect(carTotalAndSubSplitDataset.flatten({ order: 'postorder' })).to.deep.equal([
          {
            "make": "Honda"
            "model": "Civic"
            "price": 11000
            "weight": 1100
          }
          {
            "make": "Honda"
            "model": "Accord"
            "price": 13000
            "weight": 1300
          }
          {
            "make": "Honda"
            "price": 12000
            "weight": 1200
          }
          {
            "make": "Toyota"
            "model": "Prius"
            "price": 11000
            "weight": 1100
          }
          {
            "make": "Toyota"
            "model": "Corolla"
            "price": 13000
            "weight": 1300
          }
          {
            "make": "Toyota"
            "price": 12000
            "weight": 1200
          }
          {
            "price": 10000
            "weight": 1000
          }
        ])

      it "works with total and sub-split with preorder and nesting indicator", ->
        expect(carTotalAndSubSplitDataset.flatten({ order: 'preorder', nestingName: 'nest' })).to.deep.equal([
          {
            "nest": 0
            "price": 10000
            "weight": 1000
          }
          {
            "make": "Honda"
            "nest": 1
            "price": 12000
            "weight": 1200
          }
          {
            "make": "Honda"
            "model": "Civic"
            "nest": 2
            "price": 11000
            "weight": 1100
          }
          {
            "make": "Honda"
            "model": "Accord"
            "nest": 2
            "price": 13000
            "weight": 1300
          }
          {
            "make": "Toyota"
            "nest": 1
            "price": 12000
            "weight": 1200
          }
          {
            "make": "Toyota"
            "model": "Prius"
            "nest": 2
            "price": 11000
            "weight": 1100
          }
          {
            "make": "Toyota"
            "model": "Corolla"
            "nest": 2
            "price": 13000
            "weight": 1300
          }
        ])

      it "works with timeseries with preorder and nesting indicator", ->
        expect(timeSeriesResult.flatten({ order: 'preorder', nestingName: 'nest' })[0]).to.deep.equal(
          {
            "added": 6686857
            "count": 31427
            "nest": 0
          }
        )


    describe "#toCSV", ->
      it "works with basic dataset", ->
        expect(carDataset.toTabular({})).to.equal("""
        time,make,model,price
        2015-01-04T12:32:43.000Z,Honda,Civic,10000
        2015-01-04T14:00:40.000Z,Toyota,Prius,20000
        """)

      it "works with sub-dataset", ->
        expect(carAndPartsDataset.toTabular({})).to.equal("""
        time,make,model,part,price,weight
        2015-01-04T12:32:43.000Z,Honda,Civic,Engine,10000,500
        2015-01-04T12:32:43.000Z,Honda,Civic,Door,10000,20
        2015-01-04T14:00:40.000Z,Toyota,Prius,Engine,20000,400
        2015-01-04T14:00:40.000Z,Toyota,Prius,Door,20000,25
        """)
