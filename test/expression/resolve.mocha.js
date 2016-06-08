var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { Expression, Dataset, External, ExternalExpression, $, ply, r } = plywood;

describe("resolve", () => {
  describe("errors if", () => {
    it("went too deep", () => {
      var ex = ply()
        .apply('num', '$^foo + 1')
        .apply(
          'subData',
          ply()
            .apply('x', '$^num * 3')
            .apply('y', '$^^^foo * 10')
        );

      expect(() => {
        ex.resolve({ foo: 7 });
      }).to.throw('went too deep during resolve on: $^^^foo');
    });

    it("could not find something in context", () => {
      var ex = ply()
        .apply('num', '$^foo + 1')
        .apply(
          'subData',
          ply()
            .apply('x', '$^num * 3')
            .apply('y', '$^^foobar * 10')
        );

      expect(() => {
        ex.resolve({ foo: 7 });
      }).to.throw('could not resolve $^^foobar because is was not in the context');
    });

    it("ended up with bad types", () => {
      var ex = ply()
        .apply('num', '$^foo + 1')
        .apply(
          'subData',
          ply()
            .apply('x', '$^num * 3')
            .apply('y', '$^^foo * 10')
        );

      expect(() => {
        ex.resolve({ foo: 'bar' });
      }).to.throw('add must have input of type NUMBER (is STRING)');
    });
  });

  describe("#contained", () => {
    it('works with agg', () => {
      var ex = $('diamonds').sum('$price');
      expect(ex.contained()).to.equal(true);
    });

    it('works with add and var', () => {
      var ex = $('TotalPrice').add($('diamonds').sum('$price'));
      expect(ex.contained()).to.equal(true);
    });

    it('works with add and ^var', () => {
      var ex = $('TotalPrice', 1).add($('diamonds').sum('$price'));
      expect(ex.contained()).to.equal(false);
    });
  });

  describe("#resolve", () => {
    it("works in a basic case", () => {
      var ex = $('foo').add('$bar');

      var context = {
        foo: 7
      };

      ex = ex.resolve(context, 'leave');
      expect(ex.toJS()).to.deep.equal(
        r(7).add('$bar').toJS()
      );
    });

    it("works with null", () => {
      var ex = $('foo').add('$bar');

      var context = {
        foo: null
      };

      ex = ex.resolve(context, 'leave');
      expect(ex.toJS()).to.deep.equal(
        r(null).add('$bar').toJS()
      );
    });

    it("works with null with is", () => {
      var ex = $('bar', 'STRING').is('$foo');

      var context = {
        foo: null
      };

      ex = ex.resolve(context, 'leave');
      expect(ex.toJS()).to.deep.equal(
        $('bar', 'STRING').is(null).toJS()
      );
    });

    it("works in a basic case (and simplifies)", () => {
      var ex = $('foo').add(3);

      var context = {
        foo: 7
      };

      ex = ex.resolve(context).simplify();
      expect(ex.toJS()).to.deep.equal(
        r(10).toJS()
      );
    });

    it("works in a nested case", () => {
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
      expect(ex.toJS()).to.deep.equal(
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

    it("works with dataset", () => {
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
      expect(ex.toJS()).to.deep.equal(
        ply()
          .apply('Data', Dataset.fromJS(data))
          .apply('FooPlusCount', '7 + $Data.count()')
          .apply('CountPlusBar', '$Data.count() + 8')
          .toJS()
      );
    });

    it("works with sub-expressions", () => {
      var external = External.fromJS({
        engine: 'druid',
        dataSource: 'diamonds',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'color', type: 'STRING' },
          { name: 'cut', type: 'STRING' },
          { name: 'carat', type: 'NUMBER' }
        ]
      });

      var datum = {
        Count: 5,
        diamonds: external
      };

      var ex = $("diamonds").split("$cut", 'Cut')
        .apply('Count', $('diamonds').count())
        .apply('PercentOfTotal', '$Count / $^Count');

      ex = ex.resolve(datum);

      var externalExpression = new ExternalExpression({ external });
      expect(ex.toJS()).to.deep.equal(
        externalExpression.split("$cut", 'Cut', 'diamonds')
          .apply('Count', $('diamonds').count())
          .apply('PercentOfTotal', '$Count / 5')
          .toJS()
      );
    });
  });

});
