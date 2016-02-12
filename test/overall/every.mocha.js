var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { Expression, $, ply, r } = plywood;

describe("every", () => {
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
    var everyFn = (ex, index) => {
      indexes.push(index);
      return null;
    };

    var expressionCount = ex.expressionCount();
    ex.every(everyFn);
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

  it("has the right parameters", () => {
    var ex = ply()
      .apply('num', 2001001)
      .apply(
        'subData',
        ply()
          .apply('x', '$num +  7003002')
          .apply('y', '$foo * 10003002')
          .apply('z', ply().sum(13003003).add(14003002))
          .apply('w', ply().sum('$a + 19004003 + $b'))
          .split('$x', 'X', 'data')
          .apply('x', '$num + 24003002')
          .apply('y', '$data:DATASET.sum(27003003) + 28003002')
          .apply('z', ply().sum(31003003).add(32003002))
          .apply('w', '34003002 + $data:DATASET.sum(37004003) + 38003002 + 39003002 == 40003002')
      );

    var everyFn = (ex, index, depth, nestDiff) => {
      if (ex.op === 'literal' && ex.type === 'NUMBER') {
        expect(ex.value).to.equal(index * 1e6 + depth * 1e3 + nestDiff);
      }
      return null;
    };

    return ex.every(everyFn);
  });
});
