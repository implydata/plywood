/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2017 Imply Data, Inc.
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

const { expect } = require("chai");

let { testImmutableClass } = require("immutable-class-tester");

let { Timezone } = require('chronoshift');
let plywood = require('../plywood');
let { Set, $, ply, r } = plywood;

describe("Set", () => {
  it("is immutable class", () => {
    testImmutableClass(Set, [
      {
        setType: 'NULL',
        elements: []
      },
      {
        setType: 'NULL',
        elements: [null]
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

  describe("time things toString", () => {
    it("works with timezone", () => {
      expect(Set.fromJS({
        setType: 'TIME_RANGE',
        elements: [
          { start: new Date("2015-02-20T00:00:00"), end: new Date("2015-02-21T00:00:00") },
          { start: new Date("2015-02-22T00:00:00"), end: new Date("2015-02-24T00:00:00") }
        ]
      }).toString()).to.deep.equal('[2015-02-20T00:00:00Z,2015-02-21T00:00:00Z], [2015-02-22T00:00:00Z,2015-02-24T00:00:00Z]');

      expect(Set.fromJS({
        setType: 'TIME_RANGE',
        elements: [
          { start: new Date("2015-02-20T00:00:00"), end: new Date("2015-02-21T00:00:00") },
          { start: new Date("2015-02-22T00:00:00"), end: new Date("2015-02-24T00:00:00") }
        ]
      }).toString(Timezone.fromJS('Asia/Kathmandu'))).to.deep.equal('[2015-02-20T05:45:00+05:45,2015-02-21T05:45:00+05:45], [2015-02-22T05:45:00+05:45,2015-02-24T05:45:00+05:45]');

      expect(Set.fromJS({
        setType: 'TIME_RANGE',
        elements: [
          null,
          { start: new Date("2015-02-22T00:00:00.001Z"), end: new Date("2015-02-24T00:00:00.002Z") }
        ]
      }).toString(Timezone.fromJS('Asia/Kathmandu'))).to.deep.equal('null, [2015-02-22T05:45:00.001+05:45,2015-02-24T05:45:00.002+05:45]');
    })

  });


  describe("dedupes", () => {
    it("works with a SET/STRING", () => {
      expect(Set.fromJS(['A', 'A', 'B', 'B']).toJS()).to.deep.equal({
        "elements": [
          "A",
          "B"
        ],
        "setType": "STRING"
      });
    });
  });



  describe("general", () => {
    it("does not die with hasOwnProperty", () => {
      expect(Set.fromJS({
        setType: 'NUMBER',
        elements: [1, 2],
        hasOwnProperty: 'troll'
      }).toJS()).to.deep.equal({
        setType: 'NUMBER',
        elements: [1, 2]
      });
    });

    it("has EMPTY", () => {
      expect(Set.EMPTY.empty()).to.equal(true);
    });
  });


  describe("dedupes", () => {
    it("works for booleans", () => {
      expect(Set.fromJS({
        setType: 'BOOLEAN',
        elements: [true, true, true]
      }).toJS()).to.deep.equal({
        setType: 'BOOLEAN',
        elements: [true]
      });
    });

    it("works for numbers", () => {
      expect(Set.fromJS({
        setType: 'NUMBER',
        elements: [1, 2, 1, 2, 1, 2, 1, 2]
      }).toJS()).to.deep.equal({
        setType: 'NUMBER',
        elements: [1, 2]
      });
    });

    it("works for strings", () => {
      expect(Set.fromJS({
        setType: 'STRING',
        elements: ['A', 'B', 'C', 'A', 'B', 'C', 'A', 'B', 'C', 'A', 'B', 'C']
      }).toJS()).to.deep.equal({
        setType: 'STRING',
        elements: ['A', 'B', 'C']
      });
    });

  });


  describe("unifyElements", () => {
    it("works for number range", () => {
      expect(Set.fromJS({
        setType: 'NUMBER_RANGE',
        elements: [
          { start: 1, end: 2 },
          { start: 2, end: 4 },
          { start: 4, end: 5 },

          { start: 6, end: 8 },
          { start: 7, end: 9 },

          { start: 10, end: null }
        ]
      }).unifyElements().toJS()).to.deep.equal({
        setType: 'NUMBER_RANGE',
        elements: [
          { start: 1, end: 5 },
          { start: 6, end: 9 },
          { start: 10, end: null }
        ]
      });
    });

    it("works for time range", () => {
      expect(Set.fromJS({
        setType: 'TIME_RANGE',
        elements: [
          { start: new Date("2015-02-20T00:00:00"), end: new Date("2015-02-21T00:00:00") },
          { start: new Date("2015-02-21T00:00:00"), end: new Date("2015-02-22T00:00:00") },
          { start: new Date("2015-02-22T00:00:00"), end: new Date("2015-02-23T00:00:00") },

          { start: new Date("2015-02-25T00:00:00"), end: new Date("2015-02-26T00:00:00") },
          { start: new Date("2015-02-26T00:00:00"), end: new Date("2015-02-27T00:00:00") },

          { start: new Date("2015-02-28T00:00:00"), end: null }
        ]
      }).unifyElements().toJS()).to.deep.equal({
        setType: 'TIME_RANGE',
        elements: [
          { start: new Date("2015-02-20T00:00:00"), end: new Date("2015-02-23T00:00:00") },
          { start: new Date("2015-02-25T00:00:00"), end: new Date("2015-02-27T00:00:00") },
          { start: new Date("2015-02-28T00:00:00"), end: null }
        ]
      });
    });
  });


  describe("#add", () => {
    it('works correctly', () => {
      expect(
        Set.fromJS(['A', 'B']).add('C').toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A', 'B', 'C']
      });
    });

    it('works with empty', () => {
      expect(
        Set.EMPTY.add('A').toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A']
      });
    });

    it('works with null', () => {
      expect(
        Set.fromJS(['A']).add(null).toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A', null]
      });
    });
  });


  describe("#remove", () => {
    it('works correctly', () => {
      expect(
        Set.fromJS(['A', 'B']).remove('B').toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A']
      });
    });

    it('works with empty', () => {
      expect(
        Set.EMPTY.remove('A').toJS()
      ).to.deep.equal(
        Set.EMPTY.toJS()
      );
    });

    it('works with null', () => {
      expect(
        Set.fromJS(['A', null]).remove(null).toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A']
      });
    });
  });


  describe("#toggle", () => {
    it('works correctly', () => {
      expect(
        Set.fromJS(['A', 'B']).toggle('B').toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A']
      });
    });

    it('works with empty', () => {
      expect(
        Set.EMPTY.toggle('A').toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A']
      });
    });

    it('works with null', () => {
      expect(
        Set.fromJS(['A', null]).toggle(null).toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A']
      });
    });
  });


  describe("#union", () => {
    it('works correctly', () => {
      expect(
        Set.fromJS(['A', 'B']).union(Set.fromJS(['B', 'C'])).toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A', 'B', 'C']
      });
    });

    it('works with troll', () => {
      expect(
        Set.fromJS(['A', 'B']).union(Set.fromJS(['B', 'C', 'hasOwnProperty'])).toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['A', 'B', 'C', 'hasOwnProperty']
      });
    });

    it('works with time ranges', () => {
      expect(Set.fromJS({
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

    it('works with empty set as lhs', () => {
      expect(Set.EMPTY.union(Set.fromJS(['A', 'B'])).toJS()).to.deep.equal({
        setType: "STRING",
        elements: ["A", "B"]
      });
    });

    it('works with empty set as rhs', () => {
      expect(Set.fromJS(['A', 'B']).union(Set.EMPTY).toJS()).to.deep.equal({
        setType: "STRING",
        elements: ["A", "B"]
      });
    });

    it('works with empty set as lhs & rhs', () => {
      expect(Set.EMPTY.union(Set.EMPTY).toJS()).to.deep.equal({
        setType: "NULL",
        elements: []
      });
    });
  });


  describe("#intersect", () => {
    it('works correctly', () => {
      expect(
        Set.fromJS(['A', 'B']).intersect(Set.fromJS(['B', 'C'])).toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['B']
      });
    });

    it('works with troll', () => {
      expect(
        Set.fromJS(['A', 'B', 'hasOwnProperty']).intersect(Set.fromJS(['B', 'C', 'hasOwnProperty'])).toJS()
      ).to.deep.equal({
        setType: 'STRING',
        elements: ['B', 'hasOwnProperty']
      });
    });

    it('works with NUMBER_RANGEs', () => {
      let a = Set.fromJS({
        setType: 'NUMBER_RANGE',
        elements: [
          { start: 1, end: 3 },
          { start: 4, end: 7 }
        ]
      });
      let b = Set.fromJS({
        setType: 'NUMBER_RANGE',
        elements: [
          { start: 2, end: 5 },
          { start: 10, end: 11 }
        ]
      });
      expect(a.intersect(b).toJS()).to.deep.equal({
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

  describe("#overlap", () => {
    it('works correctly', () => {
      expect(
        Set.fromJS(['A', 'B']).overlap(Set.fromJS(['B', 'C']))
      ).to.equal(true);
    });

    it('works correctly when false', () => {
      expect(
        Set.fromJS(['A', 'B']).overlap(Set.fromJS(['D', 'E']))
      ).to.equal(false);
    });

    it('works with troll', () => {
      expect(
        Set.fromJS(['A', 'B', 'hasOwnProperty']).overlap(Set.fromJS(['B', 'C', 'hasOwnProperty']))
      ).to.equal(true);
    });
  });

});
