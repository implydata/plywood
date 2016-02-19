var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { $, ply, r, SplitAction } = plywood;

describe("SplitAction", () => {

  describe("#maxBucketNumber", () => {
    it("works with boolean ref case", () => {
      var splitAction = new SplitAction({
        splits: {
          bool: $('bool', 'BOOLEAN')
        }
      });

      expect(splitAction.maxBucketNumber()).to.equal(3);
    });

    it("works with boolean expression case", () => {
      var splitAction = new SplitAction({
        splits: {
          isBlah: $('x').is('blah')
        }
      });

      expect(splitAction.maxBucketNumber()).to.equal(3);
    });

    it("works in multi-split case", () => {
      var splitAction = new SplitAction({
        splits: {
          timePart: $('time').timePart('HOUR_OF_DAY'),
          isBlah: $('x').is('blah')
        }
      });

      expect(splitAction.maxBucketNumber()).to.equal(81);
    });

    it("works in unknown", () => {
      var splitAction = new SplitAction({
        splits: {
          isBlah: $('x')
        }
      });

      expect(splitAction.maxBucketNumber()).to.equal(Infinity);
    });

  });

});
