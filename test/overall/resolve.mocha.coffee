{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, Dataset, External, $ } = plywood

describe "resolve", ->
  describe "errors if", ->
    it "went too deep", ->
      ex = $()
        .apply('num', '$^foo + 1')
        .apply('subData',
          $()
            .apply('x', '$^num * 3')
            .apply('y', '$^^^foo * 10')
        )

      expect(->
        ex.resolve({ foo: 7 })
      ).to.throw('went too deep during resolve on: $^^^foo')

    it "could not find something in context", ->
      ex = $()
        .apply('num', '$^foo + 1')
        .apply('subData',
          $()
            .apply('x', '$^num * 3')
            .apply('y', '$^^foobar * 10')
        )

      expect(->
        ex.resolve({ foo: 7 })
      ).to.throw('could not resolve $^^foobar because is was not in the context')

    it "ended up with bad types", ->
      ex = $()
        .apply('num', '$^foo + 1')
        .apply('subData',
          $()
            .apply('x', '$^num * 3')
            .apply('y', '$^^foo * 10')
        )

      expect(->
        ex.resolve({ foo: 'bar' })
      ).to.throw('add must have input of type NUMBER (is STRING)')


  describe "resolves", ->
    it "works in a basic case", ->
      ex = $('foo').add('$bar')

      context = {
        foo: 7
      }

      ex = ex.resolve(context, true)
      expect(ex.toJS()).to.deep.equal(
        $(7).add('$bar').toJS()
      )

    it "works in a basic case (and simplifies)", ->
      ex = $('foo').add(3)

      context = {
        foo: 7
      }

      ex = ex.resolve(context, true).simplify()
      expect(ex.toJS()).to.deep.equal(
        $(10).toJS()
      )

    it "works in a nested case", ->
      ex = $()
        .apply('num', '$^foo + 1')
        .apply('subData',
          $()
            .apply('x', '$^num * 3')
            .apply('y', '$^^foo * 10')
        )

      context = {
        foo: 7
      }

      ex = ex.resolve(context)
      expect(ex.toJS()).to.deep.equal(
        $()
          .apply('num', '7 + 1')
          .apply('subData',
            $()
              .apply('x', '$^num * 3')
              .apply('y', '7 * 10')
          )
          .toJS()
      )

      ex = ex.simplify()
      expect(ex.toJS()).to.deep.equal(
        $(Dataset.fromJS([{ num: 8 }]))
          .apply('subData',
            $()
              .apply('x', '$^num * 3')
              .apply('y', 70)
          )
          .toJS()
      )

    it "works with dataset", ->
      data = [
        { cut: 'Good',  price: 400 }
        { cut: 'Good',  price: 300 }
        { cut: 'Great', price: 124 }
        { cut: 'Wow',   price: 160 }
        { cut: 'Wow',   price: 100 }
      ]

      ex = $()
        .apply('Data', $(Dataset.fromJS(data)))
        .apply('FooPlusCount', '$^foo + $Data.count()')
        .apply('CountPlusBar', '$Data.count() + $^bar')

      context = {
        foo: 7
        bar: 8
      }

      ex = ex.resolve(context)
      expect(ex.toJS()).to.deep.equal(
        $()
          .apply('Data', $(Dataset.fromJS(data)))
          .apply('FooPlusCount', '7 + $Data.count()')
          .apply('CountPlusBar', '$Data.count() + 8')
          .toJS()
      )

    it "works with sub-expressions", ->
      datum = {
        Count: 5
        diamonds: External.fromJS({
          engine: 'druid',
          dataSource: 'diamonds',
          timeAttribute: 'time',
          context: null
          attributes: {
            time: {type: 'TIME'}
            color: {type: 'STRING'}
            cut: {type: 'STRING'}
            carat: {type: 'NUMBER'}
          }
        })
      }

      ex = $("diamonds").split("$cut", 'Cut')
        .apply('Count', $('diamonds').count())
        .apply('PercentOfTotal', '$^Count / $Count')

      ex = ex.resolve(datum)
      console.log('ex.toString(2)', ex.toString(2));


  describe.skip "resolves remotes", ->
    context = {
      diamonds: External.fromJS({
        engine: 'druid',
        dataSource: 'diamonds',
        timeAttribute: 'time',
        context: null
        attributes: {
          time: { type: 'TIME' }
          color: { type: 'STRING' }
          cut: { type: 'STRING' }
          carat: { type: 'NUMBER' }
        }
      })
      diamonds2: External.fromJS({
        engine: 'druid',
        dataSource: 'diamonds2',
        timeAttribute: 'time',
        context: null
        attributes: {
          time: { type: 'TIME' }
          color: { type: 'STRING' }
          cut: { type: 'STRING' }
          carat: { type: 'NUMBER' }
        }
      })
    }

    it "resolves all remotes correctly", ->
      ex = $()
        .apply('Cuts',
          $("diamonds").split("$cut", 'Cut')
            .apply('Count', $('diamonds').count())
            .sort('$Count', 'descending')
            .limit(10)
        )
        .apply('Carats',
          $("diamonds").split($('carat').numberBucket(0.5), 'Carat')
            .apply('Count', $('diamonds').count())
            .sort('$Count', 'descending')
            .limit(10)
        )

      ex = ex.referenceCheck(context)

      expect(ex.every((e) ->
        return null unless e.isOp('ref')
        return (String(e.remote) is 'druid:true:diamonds')
      )).to.equal(true)

    it "resolves two dataset remotes", ->
      ex = $()
        .apply('Cuts',
          $("diamonds").split("$cut", 'Cut')
            .apply('Count', $('diamonds').count())
            .sort('$Count', 'descending')
            .limit(10)
        )
        .apply('Carats',
          $("diamonds2").split($('carat').numberBucket(0.5), 'Carat')
            .apply('Count', $('diamonds2').count())
            .sort('$Count', 'descending')
            .limit(10)
        )

      ex = ex.referenceCheck(context)

      expect(ex.actions[0].expression.every((e) ->
        return null unless e.isOp('ref')
        return (String(e.remote) is 'druid:true:diamonds')
      )).to.equal(true)

      expect(ex.actions[1].expression.every((e) ->
        return null unless e.isOp('ref')
        return (String(e.remote) is 'druid:true:diamonds2')
      )).to.equal(true)
