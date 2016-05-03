var { expect } = require("chai");

var { testImmutableClass } = require("immutable-class/build/tester");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { Expression, $, r, RefExpression } = plywood;

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
      { op: 'literal', value: [{}], type: 'DATASET' },
      { op: 'literal', value: new Date('2015-10-10Z'), type: 'TIME' },
      { op: 'ref', name: 'authors' },
      { op: 'ref', name: 'light_time' },
      { op: 'ref', name: 'timestamp' },
      { op: 'ref', name: 'timestamp', nest: 1 },
      { op: 'ref', name: 'timestamp', nest: 2 },
      { op: 'ref', name: 'make', type: 'STRING' },
      { op: 'ref', name: 'a fish will "save" you - lol / (or not)' },

      {
        op: 'chain',
        expression: { op: 'ref', name: 'diamonds' },
        action: {
          action: 'apply',
          name: 'five',
          expression: { op: 'literal', value: 5 }
        }
      },

      {
        "op": "chain", "expression": { "op": "ref", "name": "time" },
        "action": {
          "action": "in",
          "expression": {
            "op": "literal",
            "value": { "start": new Date("2013-02-26T19:00:00.000Z"), "end": new Date("2013-02-26T22:00:00.000Z") },
            "type": "TIME_RANGE"
          }
        }
      },

      {
        "op": "chain", "expression": { "op": "ref", "name": "language" },
        "action": {
          "action": "in",
          "expression": {
            "op": "literal",
            "value": { "setType": "STRING", "elements": ["en"] },
            "type": "SET"
          }
        }
      },

      {
        "op": "chain", "expression": { "op": "ref", "name": "language" },
        "action": {
          "action": "in",
          "expression": {
            "op": "literal",
            "value": { "setType": "STRING", "elements": ["he"] },
            "type": "SET"
          }
        }
      },

      {
        "op": "chain", "expression": { "op": "ref", "name": "x" },
        "actions": [
          {
            "action": "add",
            "expression": { "op": "ref", "name": "y" }
          },
          {
            "action": "add",
            "expression": { "op": "ref", "name": "z" }
          }
        ]
      },

      {
        "op": "external",
        external: {
          engine: 'druid',
          version: '0.8.1',
          dataSource: 'moon_child',
          attributes: [
            { name: 'color', type: 'STRING' },
            { name: 'cut', type: 'STRING' },
            { name: 'carat', type: 'STRING' },
            { name: 'price', type: 'NUMBER', unsplitable: true }
          ]
        }
      }

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

    it("does not like an expression with a unknown op", () => {
      expect(() => {
        Expression.fromJS({
          op: 'chain',
          expression: { op: 'ref', name: 'diamonds' },
          actions: {
            action: 'apply',
            name: 'five',
            expression: { op: 'literal', value: 5 }
          }
        });
      }).to.throw("chain `actions` must be an array");
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
        "op": "chain",
        "expression": {
          "name": "hello 'james'",
          "nest": 1,
          "op": "ref"
        },
        "action": {
          "action": "add",
          "expression": {
            "name": "how are you today?",
            "op": "ref",
            "type": "NUMBER"
          }
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
      var ex2 = r(6).multiply('$data.count()');
      expect(ex1.distribute().toJS()).to.deep.equal(ex2.toJS());
    });

    it('turns sum in count 2', () => {
      var ex1 = $('data').sum('1');
      var ex2 = $('data').count();
      expect(ex1.distribute().toJS()).to.deep.equal(ex2.toJS());
    });

    it.skip('works in constant * case', () => {
      var ex1 = $('data').sum('$x * 6');
      var ex2 = r(6).multiply('$data.sum($x)');
      expect(ex1.distribute().toJS()).to.deep.equal(ex2.toJS());
    });

    it.skip('works in constant * case (multiple operands)', () => {
      var ex1 = $('data').sum('$x * 6 * $y');
      var ex2 = r(6).multiply('$data.sum($x * $y)');
      expect(ex1.distribute().toJS()).to.deep.equal(ex2.toJS());
    });

    it.skip('works in complex case', () => {
      var ex1 = $('data').sum('$x + $y - $z * 5 + 6');
      var ex2 = $('data').sum($x).add('$data.sum($y)', '(5 * $data.sum($z)).negate()', '6 * $data.count()');
      expect(ex1.distribute().toJS()).to.deep.equal(ex2.toJS());
    });
  });
});
