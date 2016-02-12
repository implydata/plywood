var { expect } = require("chai");

var { testImmutableClass } = require("immutable-class/build/tester");

var plywood = require('../../build/plywood');
var { Set, $, ply, r } = plywood;

describe("Set", function() {
  it("is immutable class", function() {
    return testImmutableClass(Set, [
      {
        setType: 'NULL',
        elements: []
      },
      {
        setType: 'BOOLEAN',
        elements: [true]
      },
      {
        setType: 'STRING',
        elements: []
      },
      {
        setType: 'STRING',
        elements: ['A']
      },
      {
        setType: 'STRING',
        elements: ['B']
      },
      {
        setType: 'STRING',
        elements: ['B', 'C']
      },
      {
        setType: 'STRING',
        elements: ['B', 'hasOwnProperty', 'troll']
      },
      {
        setType: 'NUMBER',
        elements: []
      },
      {
        setType: 'NUMBER',
        elements: [0, 1, 2]
      },
      {
        setType: 'NUMBER_RANGE',
        elements: [
          { start: 1, end: 2 },
          { start: 3, end: 5 }
        ]
      },
      {
        setType: 'TIME',
        elements: [new Date("2015-02-20T00:00:00"), new Date("2015-02-21T00:00:00")]
      },
      {
        setType: 'TIME_RANGE',
        elements: [
          { start: new Date("2015-02-20T00:00:00"), end: new Date("2015-02-21T00:00:00") },
          { start: new Date("2015-02-22T00:00:00"), end: new Date("2015-02-24T00:00:00") }
        ]
      }
    ]);
  });


  describe("general", function() {
    it("does not die with hasOwnProperty", function() {
      return expect(Set.fromJS({
        setType: 'NUMBER',
        elements: [1, 2],
        hasOwnProperty: 'troll'
      }).toJS()).to.deep.equal({
        setType: 'NUMBER',
        elements: [1, 2]
      });
    });

    return it("has EMPTY", function() {
      return expect(Set.EMPTY.empty()).to.equal(true);
    });
  });


  describe("unifies", function() {
    it("works for booleans", function() {
      return expect(Set.fromJS({
        setType: 'BOOLEAN',
        elements: [true, true, true]
      }).toJS()).to.deep.equal({
        setType: 'BOOLEAN',
        elements: [true]
      });
    });

    it("works for numbers", function() {
      return expect(Set.fromJS({
        setType: 'NUMBER',
        elements: [1, 2, 1, 2, 1, 2, 1, 2]
      }).toJS()).to.deep.equal({
        setType: 'NUMBER',
        elements: [1, 2]
      });
    });

    it("works for strings", function() {
      return expect(Set.fromJS({
        setType: 'STRING',
        elements: ['A', 'B', 'C', 'A', 'B', 'C', 'A', 'B', 'C', 'A', 'B', 'C']
      }).toJS()).to.deep.equal({
        setType: 'STRING',
        elements: ['A', 'B', 'C']
      });
    });

    it("works for number range", function() {
      return expect(Set.fromJS({
        setType: 'NUMBER_RANGE',
        elements: [
          { start: 1, end: 2 },
          { start: 2, end: 4 },
          { start: 4, end: 5 },

          { start: 6, end: 8 },
          { start: 7, end: 9 },

          { start: 10, end: null }
        ]
      }).toJS()).to.deep.equal({
        setType: 'NUMBER_RANGE',
        elements: [
          { start: 1, end: 5 },
          { start: 6, end: 9 },
          { start: 10, end: null }
        ]
      });
    });

    return it("works for time range", function() {
      return expect(Set.fromJS({
        setType: 'TIME_RANGE',
        elements: [
          { start: new Date("2015-02-20T00:00:00"), end: new Date("2015-02-21T00:00:00") },
          { start: new Date("2015-02-21T00:00:00"), end: new Date("2015-02-22T00:00:00") },
          { start: new Date("2015-02-22T00:00:00"), end: new Date("2015-02-23T00:00:00") },

          { start: new Date("2015-02-25T00:00:00"), end: new Date("2015-02-26T00:00:00") },
          { start: new Date("2015-02-26T00:00:00"), end: new Date("2015-02-27T00:00:00") },

          { start: new Date("2015-02-28T00:00:00"), end: null }
        ]
      }).toJS()).to.deep.equal({
        setType: 'TIME_RANGE',
        elements: [
          { start: new Date("2015-02-20T00:00:00"), end: new Date("2015-02-23T00:00:00") },
          { start: new Date("2015-02-25T00:00:00"), end: new Date("2015-02-27T00:00:00") },
          { start: new Date("2015-02-28T00:00:00"), end: null }
        ]
      });
    });
  });


  describe("#add", function() {
    it('works correctly', function() {
      return expect(
        Set.fromJS(['A', 'B']).add('C').toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A', 'B', 'C']
      });
    });

    it('works with empty', function() {
      return expect(
        Set.EMPTY.add('A').toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A']
      });
    });

    return it('works with null', function() {
      return expect(
        Set.fromJS(['A']).add(null).toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A', null]
      });
    });
  });


  describe("#remove", function() {
    it('works correctly', function() {
      return expect(
        Set.fromJS(['A', 'B']).remove('B').toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A']
      });
    });

    it('works with empty', function() {
      return expect(
        Set.EMPTY.remove('A').toJS()
      ).to.deep.equal(
        Set.EMPTY.toJS()
      );
    });

    return it('works with null', function() {
      return expect(
        Set.fromJS(['A', null]).remove(null).toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A']
      });
    });
  });


  describe("#toggle", function() {
    it('works correctly', function() {
      return expect(
        Set.fromJS(['A', 'B']).toggle('B').toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A']
      });
    });

    it('works with empty', function() {
      return expect(
        Set.EMPTY.toggle('A').toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A']
      });
    });

    return it('works with null', function() {
      return expect(
        Set.fromJS(['A', null]).toggle(null).toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A']
      });
    });
  });


  describe("#union", function() {
    it('works correctly', function() {
      return expect(
        Set.fromJS(['A', 'B']).union(Set.fromJS(['B', 'C'])).toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A', 'B', 'C']
      });
    });

    it('works with troll', function() {
      return expect(
        Set.fromJS(['A', 'B']).union(Set.fromJS(['B', 'C', 'hasOwnProperty'])).toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A', 'B', 'C', 'hasOwnProperty']
      });
    });

    it('works with time ranges', function() {
      return expect(Set.fromJS({
        setType: 'NUMBER_RANGE',
        elements: [
          { start: 1, end: 2 },
          { start: 4, end: 5 },

          { start: 10, end: null }
        ]
      }).union(Set.fromJS({
        setType: 'NUMBER_RANGE',
        elements: [
          { start: 1, end: 2 },
          { start: 2, end: 4 },

          { start: 6, end: 8 },
          { start: 7, end: 9 }
        ]
      })).toJS()).to.deep.equal({
        setType: 'NUMBER_RANGE',
        elements: [
          { start: 10, end: null },
          { start: 1, end: 5 },
          { start: 6, end: 9 }
        ]
      });
    });

    it('works with empty set as lhs', function() {
      return expect(Set.EMPTY.union(Set.fromJS(['A', 'B'])).toJS()).to.deep.equal({
        setType: "STRING",
        elements: ["A", "B"]
      });
    });

    it('works with empty set as rhs', function() {
      return expect(Set.fromJS(['A', 'B']).union(Set.EMPTY).toJS()).to.deep.equal({
        setType: "STRING",
        elements: ["A", "B"]
      });
    });

    return it('works with empty set as lhs & rhs', function() {
      return expect(Set.EMPTY.union(Set.EMPTY).toJS()).to.deep.equal({
        setType: "NULL",
        elements: []
      });
    });
  });


  return describe("#intersect", function() {
    it('works correctly', function() {
      return expect(
        Set.fromJS(['A', 'B']).intersect(Set.fromJS(['B', 'C'])).toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['B']
      });
    });

    it('works with troll', function() {
      return expect(
        Set.fromJS(['A', 'B', 'hasOwnProperty']).intersect(Set.fromJS(['B', 'C', 'hasOwnProperty'])).toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['B', 'hasOwnProperty']
      });
    });

    return it('works with NUMBER_RANGEs', function() {
      var a = Set.fromJS({
        setType: 'NUMBER_RANGE',
        elements: [
          { start: 1, end: 3 },
          { start: 4, end: 7 }
        ]
      });
      var b = Set.fromJS({
        setType: 'NUMBER_RANGE',
        elements: [
          { start: 2, end: 5 },
          { start: 10, end: 11 }
        ]
      });
      return expect(a.intersect(b).toJS()).to.deep.equal({
        "setType": "NUMBER_RANGE",
        "elements": [
          {
            "start": 2,
            "end": 3
          },
          {
            "start": 4,
            "end": 5
          }
        ]
      });
    });
  });
});
