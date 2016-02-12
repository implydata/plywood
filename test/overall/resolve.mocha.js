var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { Expression, Dataset, External, $, ply, r } = plywood;

describe("resolve", function() {
  describe("errors if", function() {
    it("went too deep", function() {
      var ex = ply()
        .apply('num', '$^foo + 1')
        .apply(
          'subData',
          ply()
            .apply('x', '$^num * 3')
            .apply('y', '$^^^foo * 10')
        );

      return expect(function() {
          return ex.resolve({ foo: 7 });
        }
      ).to.throw('went too deep during resolve on: $^^^foo');
    });

    it("could not find something in context", function() {
      var ex = ply()
        .apply('num', '$^foo + 1')
        .apply(
          'subData',
          ply()
            .apply('x', '$^num * 3')
            .apply('y', '$^^foobar * 10')
        );

      return expect(function() {
          return ex.resolve({ foo: 7 });
        }
      ).to.throw('could not resolve $^^foobar because is was not in the context');
    });

    return it("ended up with bad types", function() {
      var ex = ply()
        .apply('num', '$^foo + 1')
        .apply(
          'subData',
          ply()
            .apply('x', '$^num * 3')
            .apply('y', '$^^foo * 10')
        );

      return expect(function() {
          return ex.resolve({ foo: 'bar' });
        }
      ).to.throw('add must have input of type NUMBER (is STRING)');
    });
  });


  describe("resolves", function() {
    it("works in a basic case", function() {
      var ex = $('foo').add('$bar');

      var context = {
        foo: 7
      };

      ex = ex.resolve(context, 'leave');
      return expect(ex.toJS()).to.deep.equal(
        r(7).add('$bar').toJS()
      );
    });

    it("works with null", function() {
      var ex = $('foo').add('$bar');

      var context = {
        foo: null
      };

      ex = ex.resolve(context, 'leave');
      return expect(ex.toJS()).to.deep.equal(
        r(null).add('$bar').toJS()
      );
    });

    it("works with null with is", function() {
      var ex = $('bar', 'STRING').is('$foo');

      var context = {
        foo: null
      };

      ex = ex.resolve(context, 'leave');
      return expect(ex.toJS()).to.deep.equal(
        $('bar', 'STRING').is(null).toJS()
      );
    });

    it("works in a basic case (and simplifies)", function() {
      var ex = $('foo').add(3);

      var context = {
        foo: 7
      };

      ex = ex.resolve(context).simplify();
      return expect(ex.toJS()).to.deep.equal(
        r(10).toJS()
      );
    });

    it("works in a nested case", function() {
      var ex = ply()
        .apply('num', '$^foo + 1')
        .apply(
          'subData',
          ply()
            .apply('x', '$^num * 3')
            .apply('y', '$^^foo * 10')
        );

      var context = {
        foo: 7
      };

      ex = ex.resolve(context);
      expect(ex.toJS()).to.deep.equal(
        ply()
          .apply('num', '7 + 1')
          .apply(
            'subData',
            ply()
              .apply('x', '$^num * 3')
              .apply('y', '7 * 10')
          )
          .toJS()
      );

      ex = ex.simplify();
      return expect(ex.toJS()).to.deep.equal(
        r(Dataset.fromJS([{ num: 8 }]))
          .apply(
            'subData',
            ply()
              .apply('x', '$^num * 3')
              .apply('y', 70)
          )
          .toJS()
      );
    });

    it("works with dataset", function() {
      var data = [
        { cut: 'Good', price: 400 },
        { cut: 'Good', price: 300 },
        { cut: 'Great', price: 124 },
        { cut: 'Wow', price: 160 },
        { cut: 'Wow', price: 100 }
      ];

      var ex = ply()
        .apply('Data', Dataset.fromJS(data))
        .apply('FooPlusCount', '$^foo + $Data.count()')
        .apply('CountPlusBar', '$Data.count() + $^bar');

      var context = {
        foo: 7,
        bar: 8
      };

      ex = ex.resolve(context);
      return expect(ex.toJS()).to.deep.equal(
        ply()
          .apply('Data', Dataset.fromJS(data))
          .apply('FooPlusCount', '7 + $Data.count()')
          .apply('CountPlusBar', '$Data.count() + 8')
          .toJS()
      );
    });

    return it.skip("works with sub-expressions", function() {
      var datum = {
        Count: 5,
        diamonds: External.fromJS({
          engine: 'druid',
          dataSource: 'diamonds',
          timeAttribute: 'time',
          context: null,
          attributes: [
            { name: 'time', type: 'TIME' },
            { name: 'color', type: 'STRING' },
            { name: 'cut', type: 'STRING' },
            { name: 'carat', type: 'NUMBER' }
          ]
        })
      };

      var ex = $("diamonds").split("$cut", 'Cut')
        .apply('Count', $('diamonds').count())
        .apply('PercentOfTotal', '$^Count / $Count');

      ex = ex.resolve(datum);
      return console.log('ex.toString(2)', ex.toString(2));
    });
  });


  return describe.skip("resolves remotes", function() {
    var context = {
      diamonds: External.fromJS({
        engine: 'druid',
        dataSource: 'diamonds',
        timeAttribute: 'time',
        context: null,
        attributes: [
          { name: 'time', type: 'TIME' },
          { name: 'color', type: 'STRING' },
          { name: 'cut', type: 'STRING' },
          { name: 'carat', type: 'NUMBER' }
        ]
      }),
      diamonds2: External.fromJS({
        engine: 'druid',
        dataSource: 'diamonds2',
        timeAttribute: 'time',
        context: null,
        attributes: [
          { name: 'time', type: 'TIME' },
          { name: 'color', type: 'STRING' },
          { name: 'cut', type: 'STRING' },
          { name: 'carat', type: 'NUMBER' }
        ]
      })
    };

    it("resolves all remotes correctly", function() {
      var ex = ply()
        .apply(
          'Cuts',
          $("diamonds").split("$cut", 'Cut')
            .apply('Count', $('diamonds').count())
            .sort('$Count', 'descending')
            .limit(10)
        )
        .apply(
          'Carats',
          $("diamonds").split($('carat').numberBucket(0.5), 'Carat')
            .apply('Count', $('diamonds').count())
            .sort('$Count', 'descending')
            .limit(10)
        );

      ex = ex.referenceCheck(context);

      return expect(ex.every(function(e) {
          if (!e.isOp('ref')) {
            return null;
          }
          return (String(e.remote) === 'druid:true:diamonds');
        }
      )).to.equal(true);
    });

    return it("resolves two dataset remotes", function() {
      var ex = ply()
        .apply(
          'Cuts',
          $("diamonds").split("$cut", 'Cut')
            .apply('Count', $('diamonds').count())
            .sort('$Count', 'descending')
            .limit(10)
        )
        .apply(
          'Carats',
          $("diamonds2").split($('carat').numberBucket(0.5), 'Carat')
            .apply('Count', $('diamonds2').count())
            .sort('$Count', 'descending')
            .limit(10)
        );

      ex = ex.referenceCheck(context);

      expect(ex.actions[0].expression.every(function(e) {
          if (!e.isOp('ref')) {
            return null;
          }
          return (String(e.remote) === 'druid:true:diamonds');
        }
      )).to.equal(true);

      return expect(ex.actions[1].expression.every(function(e) {
          if (!e.isOp('ref')) {
            return null;
          }
          return (String(e.remote) === 'druid:true:diamonds2');
        }
      )).to.equal(true);
    });
  });
});
