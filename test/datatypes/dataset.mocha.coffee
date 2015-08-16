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

    describe "#getOrderedColumns", ->
      it "works with basic dataset", ->
        expect(carDataset.getOrderedColumns()).to.deep.equal([
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

      it "works with sub-dataset", ->
        expect(carAndPartsDataset.getOrderedColumns()).to.deep.equal([
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
          {
            "columns": [
              {
                "name": "part"
                "type": "STRING"
              }
              {
                "name": "weight"
                "type": "NUMBER"
              }
            ]
            "name": "parts"
            "type": "DATASET"
          }
        ])

    describe "#flatten", ->
      it "works with basic dataset", ->
        expect(carDataset.flatten().data).to.deep.equal([
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

      it "works with sub-dataset", ->
        expect(carAndPartsDataset.flatten().data).to.deep.equal([
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
        expect(carTotalAndSubSplitDataset.flatten()).to.deep.equal({

        })


    describe "#toCSV", ->
      it "works with basic dataset", ->
        expect(carDataset.toTabular({})).to.equal("""
        time,make,model,price
        2015-01-04T12:32:43.000Z,Honda,Civic,10000
        2015-01-04T14:00:40.000Z,Toyota,Prius,20000
        """)

      it "works with sub-dataset", ->
        expect(carAndPartsDataset.toTabular({})).to.equal("""
        time,make,model,price,parts.part,parts.weight
        2015-01-04T12:32:43.000Z,Honda,Civic,10000,Engine,500
        2015-01-04T12:32:43.000Z,Honda,Civic,10000,Door,20
        2015-01-04T14:00:40.000Z,Toyota,Prius,20000,Engine,400
        2015-01-04T14:00:40.000Z,Toyota,Prius,20000,Door,25
        """)
