var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { Expression, Dataset, $, ply, r } = plywood;

describe("reference check", function() {
  var context = {
    seventy: 70,
    diamonds: Dataset.fromJS([
      { color: 'A', cut: 'great', carat: 1.1, price: 300 }
    ])
  };

  describe("errors", function() {
    it("fails to resolve a variable that does not exist", function() {
      var ex = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$foo * 2')
        );

      return expect(function() {
          return ex.referenceCheck({});
        }
      ).to.throw('could not resolve $foo');
    });

    it("fails to resolve a variable that does not exist (in scope)", function() {
      var ex = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$^x * 2')
        );

      return expect(function() {
          return ex.referenceCheck({});
        }
      ).to.throw('could not resolve $^x');
    });

    it("fails to when a variable goes too deep", function() {
      var ex = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$^^^x * 2')
        );

      return expect(function() {
          return ex.referenceCheck({ x: 5 });
        }
      ).to.throw('went too deep on $^^^x');
    });

    it("fails when discovering that the types mismatch", function() {
      var ex = ply()
        .apply('str', 'Hello')
        .apply(
          'subData',
          ply()
            .apply('x', '$str + 1')
        );

      return expect(function() {
          return ex.referenceCheck({ str: 'Hello World' });
        }
      ).to.throw('add must have input of type NUMBER');
    });

    return it("fails when discovering that the types mismatch via split", function() {
      var ex = ply()
        .apply("diamonds", $("diamonds").filter($('color').is('D')))
        .apply(
          'Cuts',
          $("diamonds").split("$cut", 'Cut')
            .apply('TotalPrice', '$Cut * 10')
        );

      return expect(function() {
          return ex.referenceCheck(context);
        }
      ).to.throw('multiply must have input of type NUMBER');
    });
  });


  return describe("resolves", function() {
    it("works in a basic case", function() {
      var ex = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$x * 2')
        );

      var ex2 = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$^num:NUMBER + 1')
            .apply('y', '$x:NUMBER * 2')
        );

      return expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with simple context", function() {
      var ex = ply()
        .apply('xPlusOne', '$x + 1');

      var ex2 = ply()
        .apply('xPlusOne', '$^x:NUMBER + 1');

      return expect(ex.referenceCheck({ x: 70 }).toJS()).to.deep.equal(ex2.toJS());
    });

    it("works from context 1", function() {
      var ex = $('diamonds')
        .apply('priceOver2', '$price / 2');

      var ex2 = $('diamonds', 'DATASET')
        .apply('priceOver2', '$price:NUMBER / 2');

      return expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("works from context 2", function() {
      var ex = ply()
        .apply('Diamonds', $('diamonds'))
        .apply('countPlusSeventy', '$Diamonds.count() + $seventy');

      var ex2 = ply()
        .apply('Diamonds', $('diamonds', 1, 'DATASET'))
        .apply('countPlusSeventy', '$Diamonds:DATASET.count() + $^seventy:NUMBER');

      return expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with countDistinct", function() {
      var ex = ply()
        .apply('DistinctColors', '$diamonds.countDistinct($color)')
        .apply('DistinctCuts', '$diamonds.countDistinct($cut)')
        .apply('Diff', '$DistinctColors - $DistinctCuts');

      var ex2 = ply()
        .apply('DistinctColors', '$^diamonds:DATASET.countDistinct($color:STRING)')
        .apply('DistinctCuts', '$^diamonds:DATASET.countDistinct($cut:STRING)')
        .apply('Diff', '$DistinctColors:NUMBER - $DistinctCuts:NUMBER');

      return expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("a total", function() {
      var ex = ply()
        .apply("diamonds", $("diamonds").filter($('color').is('D')))
        .apply('Count', '$diamonds.count()')
        .apply('TotalPrice', '$diamonds.sum($price)');

      var ex2 = ply()
        .apply("diamonds", $('diamonds', 1, 'DATASET').filter($('color', 'STRING').is('D')))
        .apply('Count', '$diamonds:DATASET.count()')
        .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)');

      return expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("a split", function() {
      var ex = ply()
        .apply("diamonds", $("diamonds").filter($('color').is('D')))
        .apply('Count', '$diamonds.count()')
        .apply('TotalPrice', '$diamonds.sum($price)')
        .apply(
          'Cuts',
          $("diamonds").split("$cut", 'Cut')
            .apply('Count2', '$diamonds.count()')
            .apply('TotalPrice2', '$diamonds.sum($price)')
            .apply('AvgPrice2', '$TotalPrice2 / $Count2')
            .sort('$AvgPrice2', 'descending')
            .limit(10)
        );

      var ex2 = ply()
        .apply("diamonds", $('diamonds', 1, 'DATASET').filter($('color', 'STRING').is('D')))
        .apply('Count', '$diamonds:DATASET.count()')
        .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)')
        .apply(
          'Cuts',
          $("diamonds", "DATASET").split("$cut:STRING", 'Cut')
            .apply('Count2', '$diamonds:DATASET.count()')
            .apply('TotalPrice2', '$diamonds:DATASET.sum($price:NUMBER)')
            .apply('AvgPrice2', '$TotalPrice2:NUMBER / $Count2:NUMBER')
            .sort('$AvgPrice2:NUMBER', 'descending')
            .limit(10)
        );

      return expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("a base split", function() {
      var ex = $("diamonds").split("$cut", 'Cut')
        .apply('Count', '$diamonds.count()')
        .apply('TotalPrice', '$diamonds.sum($price)');

      var ex2 = $("diamonds", "DATASET").split("$cut:STRING", 'Cut')
        .apply('Count', '$diamonds:DATASET.count()')
        .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)');

      return expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("a base split + filter", function() {
      var ex = $("diamonds").filter($('color').is('D')).split("$cut", 'Cut')
        .apply('Count', '$diamonds.count()')
        .apply('TotalPrice', '$diamonds.sum($price)');

      var ex2 = $("diamonds", "DATASET").filter($('color', 'STRING').is('D')).split("$cut:STRING", 'Cut')
        .apply('Count', '$diamonds:DATASET.count()')
        .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)');

      return expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("two splits", function() {
      var ex = ply()
        .apply("diamonds", $('diamonds').filter($("color").is('D')))
        .apply('Count', $('diamonds').count())
        .apply('TotalPrice', $('diamonds').sum('$price'))
        .apply(
          'Cuts',
          $("diamonds").split("$cut", 'Cut')
            .apply('Count', $('diamonds').count())
            .apply('PercentOfTotal', '$diamonds.sum($price) / $TotalPrice')
            .sort('$Count', 'descending')
            .limit(2)
            .apply(
              'Carats',
              $("diamonds").split($("carat").numberBucket(0.25), 'Carat')
                .apply('Count', $('diamonds').count())
                .sort('$Count', 'descending')
                .limit(3)
            )
        );

      var ex2 = ply()
        .apply("diamonds", $('diamonds', 1, 'DATASET').filter($("color", "STRING").is('D')))
        .apply('Count', $('diamonds', 'DATASET').count())
        .apply('TotalPrice', $('diamonds', 'DATASET').sum('$price:NUMBER'))
        .apply(
          'Cuts',
          $("diamonds", "DATASET").split("$cut:STRING", 'Cut')
            .apply('Count', $('diamonds', 'DATASET').count())
            .apply('PercentOfTotal', '$diamonds:DATASET.sum($price:NUMBER) / $^TotalPrice:NUMBER')
            .sort('$Count:NUMBER', 'descending')
            .limit(2)
            .apply(
              'Carats',
              $("diamonds", "DATASET").split($("carat", "NUMBER").numberBucket(0.25), 'Carat')
                .apply('Count', $('diamonds', 'DATASET').count())
                .sort('$Count:NUMBER', 'descending')
                .limit(3)
            )
        );

      return expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    return it("a join", function() {
      var ex = ply()
        .apply('Data1', $('diamonds').filter($('price').in(105, 305)))
        .apply('Data2', $('diamonds').filter($('price').in(105, 305).not()))
        .apply(
          'Cuts',
          $('Data1').split('$cut', 'Cut', 'K1').join($('Data2').split('$cut', 'Cut', 'K2'))
            .apply('Count1', '$K1.count()')
            .apply('Count2', '$K2.count()')
        );

      var ex2 = ply()
        .apply('Data1', $('diamonds', 1, 'DATASET').filter($('price', 'NUMBER').in(105, 305)))
        .apply('Data2', $('diamonds', 1, 'DATASET').filter($('price', 'NUMBER').in(105, 305).not()))
        .apply(
          'Cuts',
          $('Data1', 'DATASET').split('$cut:STRING', 'Cut', 'K1').join($('Data2', 'DATASET').split('$cut:STRING', 'Cut', 'K2'))
            .apply('Count1', '$K1:DATASET.count()')
            .apply('Count2', '$K2:DATASET.count()')
        );

      return expect(ex.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });
  });
});
