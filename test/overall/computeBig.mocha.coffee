{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, Dataset, $, ply, r } = plywood

wikiDayData = require('../../data/wikipedia')

describe "compute native nontrivial data", ->
  ds = Dataset.fromJS(wikiDayData)

  it "works in simple agg case", (testComplete) ->
    ex = ply()
      .apply('Count', '$data.count()')
      .apply('SumAdded', '$data.sum($added)')

    p = ex.compute({ data: ds })
    p.then((v) ->
      expect(v.toJS()).to.deep.equal([
        {
          "Count": 28673
          "SumAdded": 6686857
        }
      ])
      testComplete()
    ).done()

  it "works in simple split case (small dimension)", (testComplete) ->
    ex = $('data').split('$language', 'Language')
      .apply('Count', '$data.count()')
      .apply('SumAdded', '$data.sum($added)')
      .sort('$SumAdded', 'descending')
      .limit(5)

    p = ex.compute({ data: ds })
    p.then((v) ->
      expect(v.toJS()).to.deep.equal([
        {
          "Count": 15316
          "Language": "en"
          "SumAdded": 2086462
        }
        {
          "Count": 397
          "Language": "pl"
          "SumAdded": 784245
        }
        {
          "Count": 1512
          "Language": "sv"
          "SumAdded": 573478
        }
        {
          "Count": 1201
          "Language": "fr"
          "SumAdded": 458503
        }
        {
          "Count": 687
          "Language": "ru"
          "SumAdded": 412581
        }
      ])
      testComplete()
    ).done()

  it "works in simple split case (large dimension)", (testComplete) ->
    ex = $('data').split('$page', 'Page')
      .apply('Count', '$data.count()')
      .apply('SumAdded', '$data.sum($added)')
      .sort('$SumAdded', 'descending')
      .limit(5)

    p = ex.compute({ data: ds })
    p.then((v) ->
      expect(v.toJS()).to.deep.equal([
        {
          "Count": 3
          "Page": "Wikipedysta:Malarz_pl/szablony/Miasto_zagranica_infobox"
          "SumAdded": 617145
        }
        {
          "Count": 1
          "Page": "User:Tim.landscheidt/Sandbox/Unusually_long_IP_blocks"
          "SumAdded": 164225
        }
        {
          "Count": 1
          "Page": "Demographics_of_the_United_States"
          "SumAdded": 119910
        }
        {
          "Count": 1
          "Page": "Usuario:Adolfobrigido/Pruebas"
          "SumAdded": 77338
        }
        {
          "Count": 1
          "Page": "Pedro_Álvares_Cabral"
          "SumAdded": 68390
        }
      ])
      testComplete()
    ).done()

  it "works in with funny aggregates", (testComplete) ->
    ex = $('data').split('$language', 'Language')
      .apply('Language', '"[" ++ $Language ++ "]"')
      .apply('Count', '$data.count()')
      .apply('CountLT1000', '$Count < 1000')
      .apply('CountGT1000', '$Count > 1000')
      .apply('CountLTE397', '$Count <= 397')
      .apply('CountGTE397', '$Count >= 397')
      .apply('SumAdded', '$data.sum($added)')
      .apply('NegSumAdded', '-$SumAdded')
      .sort('$SumAdded', 'descending')
      .limit(5)

    p = ex.compute({ data: ds })
    p.then((v) ->
      expect(v.toJS()).to.deep.equal([
        {
          "Count": 15316
          "CountGT1000": true
          "CountGTE397": true
          "CountLT1000": false
          "CountLTE397": false
          "Language": "[en]"
          "NegSumAdded": -2086462
          "SumAdded": 2086462
        }
        {
          "Count": 397
          "CountGT1000": false
          "CountGTE397": true
          "CountLT1000": true
          "CountLTE397": true
          "Language": "[pl]"
          "NegSumAdded": -784245
          "SumAdded": 784245
        }
        {
          "Count": 1512
          "CountGT1000": true
          "CountGTE397": true
          "CountLT1000": false
          "CountLTE397": false
          "Language": "[sv]"
          "NegSumAdded": -573478
          "SumAdded": 573478
        }
        {
          "Count": 1201
          "CountGT1000": true
          "CountGTE397": true
          "CountLT1000": false
          "CountLTE397": false
          "Language": "[fr]"
          "NegSumAdded": -458503
          "SumAdded": 458503
        }
        {
          "Count": 687
          "CountGT1000": false
          "CountGTE397": true
          "CountLT1000": true
          "CountLTE397": false
          "Language": "[ru]"
          "NegSumAdded": -412581
          "SumAdded": 412581
        }
      ])
      testComplete()
    ).done()
