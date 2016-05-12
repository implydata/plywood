var { expect } = require("chai");

var { helper } = require("../../build/plywood");

describe("utils", () => {
  describe("overrideByName", () => {
    var someList = [
      { name: 'UK', score: 1 },
      { name: 'USA', score: 2 },
      { name: 'Italy', score: 3 }
    ];
    
    it('overrides (in order)', () => {
      expect(helper.overrideByName(someList, { name: 'USA', score: 5 })).to.deep.equal([
        { name: 'UK', score: 1 },
        { name: 'USA', score: 5 },
        { name: 'Italy', score: 3 }
      ])
    });

    it('overrides appends', () => {
      expect(helper.overrideByName(someList, { name: 'Russia', score: 5 })).to.deep.equal([
        { name: 'UK', score: 1 },
        { name: 'USA', score: 2 },
        { name: 'Italy', score: 3 },
        { name: 'Russia', score: 5 }
      ])
    });

  });

});
