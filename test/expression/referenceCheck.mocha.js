/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2017 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { expect } = require("chai");

let plywood = require('../plywood');
let { External, Dataset, $, i$, ply, r } = plywood;

describe("reference check", () => {
  let context = {
    seventy: 70,
    diamonds: Dataset.fromJS([
      { color: 'A', cut: 'great', carat: 1.1, price: 300, tags: ['A', 'B'] }
    ]),
    wiki: External.fromJS({
      engine: 'druid',
      source: 'wikipedia',
      timeAttribute: 'time',
      attributes: [
        { name: 'time', type: 'TIME' },
        { name: 'language', type: 'STRING' },
        { name: 'page', type: 'STRING' },
        { name: 'tags', type: 'SET/STRING' },
        { name: 'commentLength', type: 'NUMBER' },
        { name: 'isRobot', type: 'BOOLEAN' },
        { name: 'count', type: 'NUMBER', unsplitable: true },
        { name: 'added', type: 'NUMBER', unsplitable: true },
        { name: 'null', type: 'STRING', unsplitable: true }
      ],
      derivedAttributes: {
        pageExtract: '$page.extract("^(lol)")'
      }
    })
  };

  describe("errors", () => {
    it("fails to resolve a variable that does not exist", () => {
      let ex = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$foo * 2')
        );

      expect(() => {
        ex.referenceCheck({});
      }).to.throw('could not resolve $foo');
    });

    it("fails to resolve a variable that does not exist (in scope)", () => {
      let ex = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$^x * 2')
        );

      expect(() => {
        ex.referenceCheck({});
      }).to.throw('could not resolve $^x');
    });

    it("fails to resolve a select of a non existent attribute", () => {
      let ex = ply()
        .apply('num', 5)
        .select('num', 'lol');

      expect(() => {
        ex.referenceCheck({});
      }).to.throw("unknown attribute 'lol' in select");
    });

    it("fails to resolve a variable that does not exist (because of select)", () => {
      let ex = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 1')
            .apply('z', '$num + 1')
            .select('z')
            .apply('y', '$x * 2')
        );

      expect(() => {
        ex.referenceCheck({});
      }).to.throw('could not resolve $x');
    });

    it("fails to when a variable goes too deep", () => {
      let ex = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$^^^x * 2')
        );

      expect(() => {
        ex.referenceCheck({ x: 5 });
      }).to.throw('went too deep on $^^^x');
    });

    it("fails when discovering that the types mismatch", () => {
      let ex = ply()
        .apply('str', 'Hello')
        .apply(
          'subData',
          ply()
            .apply('x', '$str + 1')
        );

      expect(() => {
        ex.referenceCheck({ str: 'Hello World' });
      }).to.throw('add must have operand of type NUMBER');
    });

    it("fails when discovering that the types mismatch via split", () => {
      let ex = ply()
        .apply("diamonds", $("diamonds").filter($('color').is('D')))
        .apply(
          'Cuts',
          $("diamonds").split("$cut", 'Cut')
            .apply('TotalPrice', '$Cut * 10')
        );

      expect(() => {
        ex.referenceCheck(context);
      }).to.throw('multiply must have operand of type NUMBER');
    });

  });


  describe("resolves in type context", () => {
    let typeContext = {
      type: 'DATASET',
      datasetType: {
        x: { type: 'NUMBER' },
        y: { type: 'NUMBER' }
      },
      parent: {
        type: 'DATASET',
        datasetType: {
          z: { type: 'NUMBER' }
        }
      }
    };

    it("works in a simple reference case", () => {
      let ex1 = $('x');
      let ex2 = $('x', 'NUMBER');
      expect(ex1.changeInTypeContext(typeContext).toJS()).to.deep.equal(ex2.toJS());
    });

    it("works in a nested reference case", () => {
      let ex1 = $('z');
      let ex2 = $('z', 1, 'NUMBER');
      expect(ex1.changeInTypeContext(typeContext).toJS()).to.deep.equal(ex2.toJS());
    });

    it("works in a add case", () => {
      let ex1 = $('x').add($('y'));
      let ex2 = $('x', 'NUMBER').add($('y', 'NUMBER'));
      expect(ex1.changeInTypeContext(typeContext).toJS()).to.deep.equal(ex2.toJS());
    });

  });


  describe("resolves in context", () => {
    it("works in a trivial case", () => {
      let ex1 = $('seventy').add(1);
      let ex2 = $('seventy', 'NUMBER').add(1);
      expect(ex1.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("works in a basic case", () => {
      let ex1 = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$x + 2')
        );

      let ex2 = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$^num:NUMBER + 1')
            .apply('y', '$x:NUMBER + 2')
        );

      expect(ex1.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with simple context", () => {
      let ex1 = ply()
        .apply('xPlusOne', '$x + 1');

      let ex2 = ply()
        .apply('xPlusOne', '$^x:NUMBER + 1');

      expect(ex1.referenceCheck({ x: 70 }).toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with function", () => {
      let ex1 = ply()
        .apply('s1', 'hello')
        .apply('s2', '$s1.substr(0, 1)')
        .apply('s3', '$s2');

      let ex2 = ply()
        .apply('s1', 'hello')
        .apply('s2', '$s1:STRING.substr(0, 1)')
        .apply('s3', '$s2:STRING');

      expect(ex1.referenceCheck({}).toJS()).to.deep.equal(ex2.toJS());
    });

    it("works from context 1", () => {
      let ex1 = $('diamonds')
        .apply('pricePlus2', '$price + 2');

      let ex2 = $('diamonds', 'DATASET')
        .apply('pricePlus2', '$price:NUMBER + 2');

      expect(ex1.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("works from context 2", () => {
      let ex1 = ply()
        .apply('Diamonds', $('diamonds'))
        .apply('countPlusSeventy', '$Diamonds.count() + $seventy');

      let ex2 = ply()
        .apply('Diamonds', $('diamonds', 1, 'DATASET'))
        .apply('countPlusSeventy', '$Diamonds:DATASET.count() + $^seventy:NUMBER');

      expect(ex1.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with countDistinct", () => {
      let ex1 = ply()
        .apply('DistinctColors', '$diamonds.countDistinct($color)')
        .apply('DistinctCuts', '$diamonds.countDistinct($cut)')
        .apply('Diff', '$DistinctColors - $DistinctCuts');

      let ex2 = ply()
        .apply('DistinctColors', '$^diamonds:DATASET.countDistinct($color:STRING)')
        .apply('DistinctCuts', '$^diamonds:DATASET.countDistinct($cut:STRING)')
        .apply('Diff', '$DistinctColors:NUMBER - $DistinctCuts:NUMBER');

      expect(ex1.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("a total", () => {
      let ex1 = ply()
        .apply("diamonds", $("diamonds").filter($('color').is('D')))
        .apply('Count', '$diamonds.count()')
        .apply('TotalPrice', '$diamonds.sum($price)');

      let ex2 = ply()
        .apply("diamonds", $('diamonds', 1, 'DATASET').filter($('color', 'STRING').is('D')))
        .apply('Count', '$diamonds:DATASET.count()')
        .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)');

      expect(ex1.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("a split", () => {
      let ex1 = ply()
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

      let ex2 = ply()
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

      expect(ex1.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("a base split", () => {
      let ex1 = $("diamonds").split("$cut", 'Cut')
        .apply('Count', '$diamonds.count()')
        .apply('TotalPrice', '$diamonds.sum($price)');

      let ex2 = $("diamonds", "DATASET").split("$cut:STRING", 'Cut')
        .apply('Count', '$diamonds:DATASET.count()')
        .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)');

      expect(ex1.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("a base split + filter", () => {
      let ex1 = $("diamonds").filter($('color').is('D')).split("$cut", 'Cut')
        .apply('Count', '$diamonds.count()')
        .apply('TotalPrice', '$diamonds.sum($price)');

      let ex2 = $("diamonds", "DATASET").filter($('color', 'STRING').is('D')).split("$cut:STRING", 'Cut')
        .apply('Count', '$diamonds:DATASET.count()')
        .apply('TotalPrice', '$diamonds:DATASET.sum($price:NUMBER)');

      expect(ex1.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with dynamic derived attribute", () => {
      let ex1 = $('wiki')
        .apply('page3', '$page.substr(0, 3)')
        .filter('$page3 == wik');

      let ex2 = $('wiki', 'DATASET')
        .apply('page3', '$page:STRING.substr(0, 3)')
        .filter('$page3:STRING == wik');

      expect(ex1.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("multi-value split", () => {
      let ex1 = ply()
        .apply(
          'Ts',
          $("diamonds").split("$tags", 'Tag')
            .apply('Count', $('diamonds').count())
            .sort('$Count', 'descending')
            .filter('$Tag == "A"')
            .limit(2)
        );

      let ex2 = ply()
        .apply(
          'Ts',
          $("diamonds", 1, "DATASET").split("$tags:SET/STRING", 'Tag')
            .apply('Count', $('diamonds', 'DATASET').count())
            .sort('$Count:NUMBER', 'descending')
            .filter('$Tag:STRING == "A"')
            .limit(2)
        );

      expect(ex1.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("two splits", () => {
      let ex1 = ply()
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

      let ex2 = ply()
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

      expect(ex1.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it.skip("a join", () => {
      let ex1 = ply()
        .apply('Data1', $('diamonds').filter($('price').in(105, 305)))
        .apply('Data2', $('diamonds').filter($('price').in(105, 305).not()))
        .apply(
          'Cuts',
          $('Data1').split('$cut', 'Cut', 'K1').join($('Data2').split('$cut', 'Cut', 'K2'))
            .apply('Count1', '$K1.count()')
            .apply('Count2', '$K2.count()')
        );

      let ex2 = ply()
        .apply('Data1', $('diamonds', 1, 'DATASET').filter($('price', 'NUMBER').in(105, 305)))
        .apply('Data2', $('diamonds', 1, 'DATASET').filter($('price', 'NUMBER').in(105, 305).not()))
        .apply(
          'Cuts',
          $('Data1', 'DATASET').split('$cut:STRING', 'Cut', 'K1').join($('Data2', 'DATASET').split('$cut:STRING', 'Cut', 'K2'))
            .apply('Count1', '$K1:DATASET.count()')
            .apply('Count2', '$K2:DATASET.count()')
        );

      expect(ex1.referenceCheck(context).toJS()).to.deep.equal(ex2.toJS());
    });

    it("key with name null should not return false positive", () => {
      let ex1 = i$('blah');
      expect(() => {
        ex1.referenceCheck({'null': 'STRING'})
      }).to.throw('could not resolve i$blah');
    });

    it("key with name null can still be a valid reference to", () => {
      let ex1 = i$('null');
      expect(ex1.referenceCheck({'null': 'STRING'}).toJS()).to.deep.equal({
        "name": "null",
        "op": "ref",
        "type": "STRING"
      });
    });

  });

});
