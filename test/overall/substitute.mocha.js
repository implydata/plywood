var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { Expression, $, ply, r } = plywood;

describe("substitute", () => {
  it("should substitute on IS", () => {
    var ex = r(5).is('$hello');

    var subs = (ex) => {
      if (ex.op === 'literal' && ex.type === 'NUMBER') {
        return Expression.fromJSLoose(ex.value + 10);
      } else {
        return null;
      }
    };

    expect(ex.substitute(subs).toJS()).to.deep.equal(
      r(15).is('$hello').toJS()
    );
  });

  it("should substitute on complex expression", () => {
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

    var subs = (ex) => {
      if (ex.op === 'literal' && ex.type === 'NUMBER') {
        return Expression.fromJSLoose(ex.value + 10);
      } else {
        return null;
      }
    };

    expect(ex.substitute(subs).toJS()).to.deep.equal(
      ply()
        .apply('num', 15)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 11')
            .apply('y', '$foo * 12')
            .apply('z', ply().sum('$a + 13'))
            .apply('w', ply().sum('$a + 14 + $b'))
        )
        .toJS()
    );
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
    expect(indexes).to.deep.equal(((() => {
      var result = [];
      var i = 0;
      if (0 <= expressionCount) {
        while (i < expressionCount) {
          result.push(i++);
        }
      } else {
        while (i > expressionCount) {
          result.push(i--);
        }
      }
      return result;
    })()));
  });
});

