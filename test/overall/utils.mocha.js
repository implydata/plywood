var { expect } = require("chai");

var plywood = require('../../build/plywood');

describe("utils", function() {
  describe("safeAdd", function() {
    it("works on 0.2 + 0.1", function() {
      expect(plywood.safeAdd(0.2, 0.1)).to.equal(0.3);
      return expect(plywood.safeAdd(0.2, 0.1)).to.not.equal(0.2 + 0.1);
    });

    it("works on 0.7 + 0.1", function() {
      expect(plywood.safeAdd(0.7, 0.1)).to.equal(0.8);
      return expect(plywood.safeAdd(0.7, 0.1)).to.not.equal(0.7 + 0.1);
    });

    return it("works on unrepresentable", function() {
      return expect(plywood.safeAdd(1, 1 / 3)).to.equal(1 + 1 / 3);
    });
  });


  return describe("continuousFloorExpression", function() {
    it("should be minimalistic (no size / no offset)", function() {
      return expect(plywood.continuousFloorExpression("x", "Math.floor", 1, 0)).to.equal('Math.floor(x)');
    });

    it("should be minimalistic (no size)", function() {
      return expect(plywood.continuousFloorExpression("x", "Math.floor", 1, 0.3)).to.equal('Math.floor(x - 0.3) + 0.3');
    });

    it("should be minimalistic (no offset)", function() {
      return expect(plywood.continuousFloorExpression("x", "Math.floor", 5, 0)).to.equal('Math.floor(x / 5) * 5');
    });

    return it("should be work in general", function() {
      return expect(plywood.continuousFloorExpression("x", "Math.floor", 5, 3)).to.equal('Math.floor((x - 3) / 5) * 5 + 3');
    });
  });
});
