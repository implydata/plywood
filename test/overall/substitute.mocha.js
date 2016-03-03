var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { $, ply, r } = plywood;

describe("substitute", () => {
  it("should substitute on IS", () => {
    var ex1 = r(5).is('$hello');

    var subs = (ex) => {
      if (ex.op === 'literal' && ex.type === 'NUMBER') {
        return r(ex.value + 10);
      } else {
        return null;
      }
    };

    var ex2 = r(15).is('$hello');

    expect(ex1.substitute(subs).toJS()).to.deep.equal(ex2.toJS());
  });

  it("should substitute on complex expression", () => {
    var ex1 = ply()
      .apply('num', 5)
      .apply(
        'subData',
        ply()
          .apply('x', '$num + 1')
          .apply('y', '$foo * 2')
          .apply('z', ply().sum('$a + 3'))
          .apply('w', ply().sum('$a + 4 + $b'))
      );

    var subs = (ex) => {
      if (ex.op === 'literal' && ex.type === 'NUMBER') {
        return r(ex.value + 10);
      } else {
        return null;
      }
    };

    var ex2 = ply()
      .apply('num', 15)
      .apply(
        'subData',
        ply()
          .apply('x', '$num + 11')
          .apply('y', '$foo * 12')
          .apply('z', ply().sum('$a + 13'))
          .apply('w', ply().sum('$a + 14 + $b'))
      );

    expect(ex1.substitute(subs).toJS()).to.deep.equal(ex2.toJS());
  });

  it("has sequential indexes", () => {
    var ex = ply()
      .apply('num', 5)
      .apply(
        'subData',
        ply()
          .apply('x', '$num + 1')
          .apply('y', '$foo * 2')
          .apply('z', ply().sum('$a + 3'))
          .apply('w', ply().sum('$a + 4 + $b'))
      );

    var indexes = [];
    var subs = (ex, index) => {
      indexes.push(index);
      return null;
    };

    var expressionCount = ex.expressionCount();
    ex.substitute(subs);
    expect(expressionCount).to.equal(22);


    var range = [];
    for (var i = 0; i < expressionCount; i++) range.push(i);

    expect(indexes).to.deep.equal(range);
  });
});

