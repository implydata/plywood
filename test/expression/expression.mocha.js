/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
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

var { expect } = require("chai");

var { testImmutableClass } = require("immutable-class-tester");

var plywood = require('../plywood');
var { Expression, $, ply, r, RefExpression, LimitExpression, SortExpression } = plywood;

describe("Expression", () => {
  it("is immutable class", () => {
    testImmutableClass(Expression, [
      { op: 'literal', value: null },
      { op: 'literal', value: false },
      { op: 'literal', value: true },
      { op: 'literal', value: 0 },
      { op: 'literal', value: 0.1 },
      { op: 'literal', value: 6 },
      { op: 'literal', value: '' },
      { op: 'literal', value: 'Honda' },
      { op: 'literal', value: '$honda' },
      { op: 'literal', value: { setType: 'STRING', elements: [] }, type: 'SET' },
      { op: 'literal', value: { setType: 'STRING', elements: ['BMW', 'Honda', 'Suzuki'] }, type: 'SET' },
      { op: 'literal', value: { setType: 'NUMBER', elements: [0.05, 0.1] }, type: 'SET' },
      //{ op: 'literal', value: [{}], type: 'DATASET' },
      { op: 'literal', value: new Date('2015-10-10Z'), type: 'TIME' },
      { op: 'ref', name: 'authors' },
      { op: 'ref', name: 'light_time' },
      { op: 'ref', name: 'timestamp' },
      { op: 'ref', name: 'timestamp', nest: 1 },
      { op: 'ref', name: 'timestamp', nest: 2 },
      { op: 'ref', name: 'make', type: 'STRING' },
      { op: 'ref', name: 'a fish will "save" you - lol / (or not)' },
      { op: 'ref', name: 'a thing', ignoreCase: true },

      {
        op: 'add',
        operand: { op: 'ref', name: 'x' },
        expression: { op: 'literal', value: 6 }
      },

      {
        op: 'apply',
        operand: { op: 'ref', name: 'diamonds' },
        name: 'five',
        expression: { op: 'literal', value: 5 }
      },

      {
        op: 'substr',
        operand: { op: 'ref', name: 'x' },
        position: 1,
        len: 3
      },

      {
        "operand": { "op": "ref", "name": "time" },
        "op": "in",
        "expression": {
          "op": "literal",
          "value": { "start": new Date("2013-02-26T19:00:00.000Z"), "end": new Date("2013-02-26T22:00:00.000Z") },
          "type": "TIME_RANGE"
        }
      },

      {
        "operand": { "op": "ref", "name": "language" },
        "op": "in",
        "expression": {
          "op": "literal",
          "value": { "setType": "STRING", "elements": ["en"] },
          "type": "SET"
        }
      },

      {
        "operand": { "op": "ref", "name": "language" },
        "op": "in",
        "expression": {
          "op": "literal",
          "value": { "setType": "STRING", "elements": ["he"] },
          "type": "SET"
        }
      },

      {
        "op": "add",
        "operand": { "op": "ref", "name": "x" },
        "expression": {
          "op": "add",
          "operand": { "op": "ref", "name": "y" },
          "expression": { "op": "ref", "name": "z" }
        }
      },

      {
        op: "external",
        external: {
          engine: 'druid',
          version: '0.8.1',
          source: 'moon_child',
          attributes: [
            { name: 'color', type: 'STRING' },
            { name: 'cut', type: 'STRING' },
            { name: 'carat', type: 'STRING' },
            { name: 'price', type: 'NUMBER', unsplitable: true }
          ]
        }
      },

      {
        op: 'split',
        operand: { op: 'ref', name: '_' },
        splits: {
          'Page': { op: 'ref', name: 'page' },
          'User': { op: 'ref', name: 'user' }
        },
        dataName: 'myData'
      },
      {
        op: 'apply',
        operand: { op: 'ref', name: '_' },
        name: 'Five',
        expression: { op: 'literal', value: 5 }
      },
      {
        op: 'sort',
        operand: { op: 'ref', name: '_' },
        expression: { op: 'ref', name: 'myVar' },
        direction: 'ascending'
      },
      { op: 'limit', operand: { op: 'ref', name: '_' }, value: 10 },
      { op: 'select', operand: { op: 'ref', name: '_' }, attributes: ['a', 'b', 'c'] },
      { op: 'select', operand: { op: 'ref', name: '_' }, attributes: ['b', 'c'] },

      { op: 'fallback', operand: { op: 'ref', name: '_' }, expression: { op: 'ref', name: 'myVar2' } },
      { op: 'count', operand: { op: 'ref', name: '_' } },
      { op: 'sum', operand: { op: 'ref', name: '_' }, expression: { op: 'ref', name: 'myVar' } },
      { op: 'power', operand: { op: 'ref', name: '_' }, expression: { op: 'ref', name: 'myVar' } },
      { op: 'absolute', operand: { op: 'ref', name: '_' } },
      { op: 'min', operand: { op: 'ref', name: '_' }, expression: { op: 'ref', name: 'myVar' } },
      { op: 'max', operand: { op: 'ref', name: '_' }, expression: { op: 'ref', name: 'myVar' } },
      { op: 'average', operand: { op: 'ref', name: '_' }, expression: { op: 'ref', name: 'myVar' } },
      { op: 'countDistinct', operand: { op: 'ref', name: '_' }, expression: { op: 'ref', name: 'myVar' } },
      { op: 'quantile', operand: { op: 'ref', name: '_' }, expression: { op: 'ref', name: 'myVar' }, value: 0.5 },
      { op: 'collect', operand: { op: 'ref', name: '_' }, expression: { op: 'ref', name: 'myVar' } },
      { op: 'cast', operand: { op: 'ref', name: '_' }, outputType: 'TIME' },
      { op: 'cast', operand: { op: 'ref', name: '_' }, outputType: 'NUMBER' },

      { op: 'customAggregate', operand: { op: 'ref', name: '_' }, custom: 'blah' },
      { op: 'customTransform', operand: { op: 'ref', name: '_' }, custom: 'decodeURIComponentToLowerCaseAndTrim' },
      { op: 'customTransform', operand: { op: 'ref', name: '_' }, custom: 'includes', outputType: 'BOOLEAN' },

      { op: 'concat', operand: { op: 'ref', name: '_' }, expression: { op: 'literal', value: 'myVar' } },

      { op: 'contains', operand: { op: 'ref', name: '_' }, expression: { op: 'ref', name: 'myVar' }, compare: 'normal' },
      { op: 'contains', operand: { op: 'ref', name: '_' }, expression: { op: 'ref', name: 'myVar' }, compare: 'ignoreCase' },

      { op: 'match', operand: { op: 'ref', name: '_' }, regexp: 'A[B]' },
      { op: 'match', operand: { op: 'ref', name: '_' }, regexp: '^fu*$' },

      { op: 'lessThan', operand: { op: 'ref', name: '_' }, expression: { op: 'literal', type: 'TIME', value: new Date('2015-10-10Z') } },

      { op: 'overlap', operand: { op: 'ref', name: '_' }, expression: { op: 'ref', name: 'myVar' } },
      { op: 'overlap', operand: { op: 'ref', name: '_' }, expression: { op: 'literal', value: { setType: 'STRING', elements: ['BMW', 'Honda', 'Suzuki'] }, type: 'SET' } },

      { op: 'numberBucket', operand: { op: 'ref', name: '_' }, size: 5 },
      { op: 'numberBucket', operand: { op: 'ref', name: '_' }, size: 5, offset: 1 },

      { op: 'length', operand: { op: 'ref', name: '_' } },
      { op: 'indexOf', operand: { op: 'ref', name: '_' }, expression: { op: 'literal', value: 'string' } },

      //{ op: 'cardinality', operand: { op: 'ref', name: '_' } }, // ToDo: wtf?!

      { op: 'timeFloor', operand: { op: 'ref', name: '_' }, duration: 'P1D' },
      { op: 'timeFloor', operand: { op: 'ref', name: '_' }, duration: 'PT2H', timezone: 'Etc/UTC' },
      { op: 'timeFloor', operand: { op: 'ref', name: '_' }, duration: 'PT2H', timezone: 'America/Los_Angeles' },

      { op: 'timeBucket', operand: { op: 'ref', name: '_' }, duration: 'P1D' },
      { op: 'timeBucket', operand: { op: 'ref', name: '_' }, duration: 'PT2H', timezone: 'Etc/UTC' },
      { op: 'timeBucket', operand: { op: 'ref', name: '_' }, duration: 'PT2H', timezone: 'America/Los_Angeles' },

      { op: 'timePart', operand: { op: 'ref', name: '_' }, part: 'DAY_OF_WEEK' },
      { op: 'timePart', operand: { op: 'ref', name: '_' }, part: 'DAY_OF_MONTH', timezone: 'Etc/UTC' },
      { op: 'timePart', operand: { op: 'ref', name: '_' }, part: 'DAY_OF_MONTH', timezone: 'America/Los_Angeles' },

      { op: 'timeShift', operand: { op: 'ref', name: '_' }, duration: 'P1D', step: 1 },
      { op: 'timeShift', operand: { op: 'ref', name: '_' }, duration: 'P1D', step: -2 },
      { op: 'timeShift', operand: { op: 'ref', name: '_' }, duration: 'P2D', step: 3, timezone: 'Etc/UTC' },
      { op: 'timeShift', operand: { op: 'ref', name: '_' }, duration: 'P2D', step: 3, timezone: 'America/Los_Angeles' },

      { op: 'timeRange', operand: { op: 'ref', name: '_' }, duration: 'P1D', step: 1 },
      { op: 'timeRange', operand: { op: 'ref', name: '_' }, duration: 'P1D', step: -2 },
      { op: 'timeRange', operand: { op: 'ref', name: '_' }, duration: 'P2D', step: 3, timezone: 'Etc/UTC' },
      { op: 'timeRange', operand: { op: 'ref', name: '_' }, duration: 'P2D', step: 3, timezone: 'America/Los_Angeles' },

      { op: 'transformCase', operand: { op: 'ref', name: '_' }, transformType: 'upperCase'},
      { op: 'transformCase', operand: { op: 'ref', name: '_' }, transformType: 'lowerCase'},

      { op: 'customAggregate', operand: { op: 'ref', name: '_' }, custom: 'lol1' },
      { op: 'customAggregate', operand: { op: 'ref', name: '_' }, custom: 'lol2' }

    ], {
      newThrows: true
    });
  });

  describe("does not die with hasOwnProperty", () => {
    it("survives", () => {
      expect(Expression.fromJS({
        op: 'literal',
        value: 'Honda',
        hasOwnProperty: 'troll'
      }).toJS()).to.deep.equal({
        op: 'literal',
        value: 'Honda'
      });
    });
  });

  describe("errors", () => {
    it("does not like an expression without op", () => {
      expect(() => {
        Expression.fromJS({
          name: 'hello'
        });
      }).to.throw('op must be defined');
    });

    it("does not like an expression with a bad op", () => {
      expect(() => {
        Expression.fromJS({
          op: 42
        });
      }).to.throw('op must be a string');
    });

    it("does not like an expression with a unknown op", () => {
      expect(() => {
        Expression.fromJS({
          op: 'this was once an empty file'
        });
      }).to.throw("unsupported expression op 'this was once an empty file'");
    });

    it("does not like an expression that needs and expression but does not have one", () => {
      expect(() => {
        new SortExpression({});
      }).to.throw("must have an expression");
    });
  });


  describe("fancy names", () => {
    it("behaves corretly with spaces", () => {
      expect($("I blame your mother").toJS()).to.deep.equal({
        "op": "ref",
        "name": "I blame your mother"
      });
    });

    it("works with fromJSLoose", () => {
      expect(Expression.fromJSLoose("$^^{and do don't call me shirley}").toJS()).to.deep.equal({
        "op": "ref",
        "name": "and do don't call me shirley",
        "nest": 2
      });
    });

    it("works with ref expression parse", () => {
      expect(RefExpression.parse("{how are you today?}:NUMBER").toJS()).to.deep.equal({
        "op": "ref",
        "name": "how are you today?",
        "type": "NUMBER"
      });
    });

    it("parses", () => {
      expect(Expression.parse("$^{hello 'james'} + ${how are you today?}:NUMBER").toJS()).to.deep.equal({
        "op": "add",
        "operand": {
          "name": "hello 'james'",
          "nest": 1,
          "op": "ref"
        },
        "expression": {
          "name": "how are you today?",
          "op": "ref",
          "type": "NUMBER"
        }
      });
    });
  });


  describe("#getFn", () => {
    it("works in a simple case of IS", () => {
      var ex = $('x').is(8);
      var exFn = ex.getFn();
      expect(exFn({ x: 5 })).to.equal(false);
      expect(exFn({ x: 8 })).to.equal(true);
    });

    it("works in a simple case of addition", () => {
      var ex = $('x').add('$y', 5);

      var exFn = ex.getFn();
      expect(exFn({ x: 5, y: 1 })).to.equal(11);
      expect(exFn({ x: 8, y: -3 })).to.equal(10);
    });

    it("works with calc", () => {
      var ex = Expression.fromJS({
        op: 'add',
        operand: { op: 'ref', name: 'x' },
        expression: { op: 'ref', name: 'y' }
      });
      var exFn = ex.getFn();
      expect(exFn({ x: 5, y: 2 })).to.equal(7);
      expect(ex.calc({ x: 8, y: -3 })).to.equal(5);
    });

    it('works with case insensitive', () => {
      var exp = Expression.parse("i$cUT");
      expect(exp.toJS()).to.deep.equal({
        "ignoreCase": true,
        "name": "cUT",
        "op": "ref",
      });

      expect(exp.getFn()({ cut : 'good'})).to.equal('good');
      expect(exp.getFn()({ cutS : 'good'})).to.equal(null);
      expect(exp.getFn()({})).to.equal(null);
      expect(exp.getFn()(null)).to.equal(null);
    });
  });

  describe("#getJS", () => {
    it('throws with case insensitive flag still set', () => {
      var exp = Expression.parse("i$cUT");
      expect(() => exp.getJS()).to.throw('can not express ignore case as js expression');
    });
  });


  describe('#decomposeAverage', () => {
    it('works in simple case', () => {
      var ex1 = $('data').average('$x');
      var ex2 = $('data').sum('$x').divide($('data').count());
      expect(ex1.decomposeAverage().toJS()).to.deep.equal(ex2.toJS());
    });

    it('works in more nested case', () => {
      var ex1 = $('w').add(
        $('data').average('$x'),
        $('data').average('$y + $z')
      );
      var ex2 = $('w').add(
        $('data').sum('$x').divide($('data').count()),
        $('data').sum('$y + $z').divide($('data').count())
      );
      expect(ex1.decomposeAverage().toJS()).to.deep.equal(ex2.toJS());
    });

    it('works in custom count case', () => {
      var ex1 = $('data').average('$x');
      var ex2 = $('data').sum('$x').divide($('data').sum('$count'));
      expect(ex1.decomposeAverage($('count')).toJS()).to.deep.equal(ex2.toJS());
    });
  });


  describe('#distribute', () => {
    it('works in simple - case', () => {
      var ex1 = $('data').sum('-$x');
      var ex2 = $('data').sum('$x').negate();
      expect(ex1.distribute().toJS()).to.deep.equal(ex2.toJS());
    });

    it('works in simple + case', () => {
      var ex1 = $('data').sum('$x + $y');
      var ex2 = $('data').sum('$x').add('$data.sum($y)');
      expect(ex1.distribute().toJS()).to.deep.equal(ex2.toJS());
    });

    it('turns sum in count 1', () => {
      var ex1 = $('data').sum('6');
      var ex2 = $('data').count().multiply(6);
      expect(ex1.distribute().toJS()).to.deep.equal(ex2.toJS());
    });

    it('turns sum in count 2', () => {
      var ex1 = $('data').sum('1');
      var ex2 = $('data').count();
      expect(ex1.distribute().toJS()).to.deep.equal(ex2.toJS());
    });

    it('works in constant * case', () => {
      var ex1 = $('data').sum('$x * 6');
      var ex2 = $('data').sum('$x').multiply(6);
      expect(ex1.distribute().toJS()).to.deep.equal(ex2.toJS());
    });

    it.skip('works in constant * case (multiple operands)', () => {
      var ex1 = $('data').sum('$x * 6 * $y');
      var ex2 = $('data').sum('$x * $y').multiply(6);
      expect(ex1.distribute().toJS()).to.deep.equal(ex2.toJS());
    });

    it.skip('works in complex case', () => {
      var ex1 = $('data').sum('$x + $y - $z * 5 + 6');
      var ex2 = $('data').sum($x).add('$data.sum($y)', '($data.sum($z) * 5).negate()', '6 * $data.count()');
      expect(ex1.distribute().toJS()).to.deep.equal(ex2.toJS());
    });

  });

  describe("RefExpression.toJavaScriptSafeName", () => {
    it('works', () => {
      expect(RefExpression.toJavaScriptSafeName('hello')).to.equal('_hello');
      expect(RefExpression.toJavaScriptSafeName('try')).to.equal('_try');
      expect(RefExpression.toJavaScriptSafeName('i-love-you')).to.equal('_i$45love$45you');
      expect(RefExpression.toJavaScriptSafeName('ру́сский')).to.equal('_$1088$1091$769$1089$1089$1082$1080$1081');
    });

  });

  describe('fancy actions', () => {
    it('works with no operand', () => {
      expect(new SortExpression({ expression: $('x'), direction: SortExpression.DESCENDING }).toJS()).to.deep.equal({
        "op": "sort",
        operand: { op: 'ref', name: '_' },
        expression: { op: 'ref', name: 'x' },
        direction: 'descending'
      });
    });

    it('limit works with Infinity', () => {
      expect(new LimitExpression({ value: Infinity }).toJS()).to.deep.equal({
        "op": "limit",
        operand: { op: 'ref', name: '_' },
        "value": Infinity
      });
    });

  });

  describe('fromJS API back comparability', () => {
    it('works in complex case', () => {
      var js = {
        "actions": [
          {
            "action": "apply",
            "expression": {
              "actions": [
                {
                  "action": "filter",
                  "expression": {
                    "action": {
                      "action": "is",
                      "expression": {
                        "op": "literal",
                        "value": "D"
                      }
                    },
                    "expression": {
                      "name": "color",
                      "op": "ref"
                    },
                    "op": "chain"
                  }
                },
                {
                  "action": "apply",
                  "expression": {
                    "action": {
                      "action": "divide",
                      "expression": {
                        "op": "literal",
                        "value": 2
                      }
                    },
                    "expression": {
                      "name": "price",
                      "op": "ref"
                    },
                    "op": "chain"
                  },
                  "name": "priceOver2"
                }
              ],
              "expression": {
                "op": "literal",
                "type": "DATASET",
                "value": [
                  {}
                ]
              },
              "op": "chain"
            },
            "name": "Diamonds"
          },
          {
            "action": "apply",
            "expression": {
              "action": {
                "action": "count"
              },
              "expression": {
                "name": "Diamonds",
                "op": "ref"
              },
              "op": "chain"
            },
            "name": "Count"
          },
          {
            "action": "apply",
            "expression": {
              "action": {
                "action": "sum",
                "expression": {
                  "name": "priceOver2",
                  "op": "ref"
                }
              },
              "expression": {
                "name": "Diamonds",
                "op": "ref"
              },
              "op": "chain"
            },
            "name": "TotalPrice"
          }
        ],
        "expression": {
          "op": "literal",
          "type": "DATASET",
          "value": [
            {}
          ]
        },
        "op": "chain"
      };

      var ex2 = ply()
        .apply("Diamonds", ply().filter("$color == 'D'").apply("priceOver2", "$price/2"))
        .apply('Count', $('Diamonds').count())
        .apply('TotalPrice', $('Diamonds').sum('$priceOver2'));

      expect(Expression.fromJS(js).toJS()).to.deep.equal(ex2.toJS());
    });

  });

});
