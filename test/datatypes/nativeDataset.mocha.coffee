{ expect } = require("chai")

plywood = require('../../build/plywood')
{ NativeDataset, $ } = plywood

describe "NativeDataset", ->
  carDataset = NativeDataset.fromJS({
    data: [
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
    ]
  })

  carAndPartsDataset = NativeDataset.fromJS({
    data: [
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
    ]
  })

  describe "#getFlattenedColumns", ->
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
