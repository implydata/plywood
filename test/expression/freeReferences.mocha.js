var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { Expression, Action, Dataset, $, ply, r } = plywood;

describe("reference check", () => {
  var context = {
    diamonds: Dataset.fromJS([
      { color: 'A', cut: 'great', carat: 1.1, price: 300 }
    ])
  };

  describe("works as expected", () => {
    it("works when there are no free references", () => {
      var ex = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$x * 2')
        );

      ex = ex.referenceCheck({});
      expect(ex.getFreeReferences()).to.deep.equal([]);
    });

    it("works in a basic case", () => {
      var ex = Expression.parse('$x + $y * $z + $data.sum($revenue)');

      expect(ex.getFreeReferences()).to.deep.equal(['data', 'x', 'y', 'z']);
    });

    it("works in a actions case", () => {
      var ex = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$x * 2')
            .apply('z', '$diamonds.sum($price)')
        );

      ex = ex.referenceCheck(context);
      expect(ex.getFreeReferences()).to.deep.equal(['diamonds']);

      expect(ex.actions[1].getFreeReferences()).to.deep.equal(['^diamonds', 'num']);
    });

    it("works in a consecutive actions case", () => {
      var ex = ply()
        .apply('one', 1)
        .apply('two', '$one + 1')
        .apply('three', '$two + 1')
        .apply('four', '$three + 1');


      ex = ex.referenceCheck({});
      expect(ex.getFreeReferences()).to.deep.equal([]);
    });
  });


  describe('checks action decencies', () => {
    var ex = ply()
      .apply('two', '$one + 1')
      .apply('three', '$two + 1')
      .apply('four', '$three + 1');

    it("finds something that is needed", () => {
      expect(Action.actionsDependOn(ex.actions, 'one')).to.be.true;
    });

    it("knows what is not needed because it is not referred to", () => {
      expect(Action.actionsDependOn(ex.actions, 'kaka')).to.be.false;
    });

    it("knows what is not needed because it is shadowed", () => {
      expect(Action.actionsDependOn(ex.actions, 'three')).to.be.false;
    });
  });
});
