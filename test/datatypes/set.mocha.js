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
let { Set, $, ply, r, NumberRange, TimeRange } = plywood;

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
        elements: [new Date('2015-02-20T00:00:00Z'), new Date('2015-02-21T00:00:00Z')]
      },
      {
        setType: 'TIME_RANGE',
        elements: [
          { start: new Date('2015-02-20T00:00:00Z'), end: new Date('2015-02-21T00:00:00Z') },
          { start: new Date('2015-02-22T00:00:00Z'), end: new Date('2015-02-24T00:00:00Z') }
        ]
      }
    ]);
  });

  describe("time things toString", () => {
    it("works with timezone", () => {
      expect(Set.fromJS({
        setType: 'TIME_RANGE',
        elements: [
          { start: new Date('2015-02-20T00:00:00Z'), end: new Date('2015-02-21T00:00:00Z') },
          { start: new Date('2015-02-22T00:00:00Z'), end: new Date('2015-02-24T00:00:00Z') }
        ]
      }).toString()).to.deep.equal('[2015-02-20T00:00:00Z,2015-02-21T00:00:00Z], [2015-02-22T00:00:00Z,2015-02-24T00:00:00Z]');

      expect(Set.fromJS({
        setType: 'TIME_RANGE',
        elements: [
          { start: new Date('2015-02-20T00:00:00Z'), end: new Date('2015-02-21T00:00:00Z') },
          { start: new Date('2015-02-22T00:00:00Z'), end: new Date('2015-02-24T00:00:00Z') }
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
          { start: new Date('2015-02-20T00:00:00Z'), end: new Date('2015-02-21T00:00:00Z') },
          { start: new Date('2015-02-21T00:00:00Z'), end: new Date('2015-02-22T00:00:00Z') },
          { start: new Date('2015-02-22T00:00:00Z'), end: new Date('2015-02-23T00:00:00Z') },

          { start: new Date('2015-02-25T00:00:00Z'), end: new Date('2015-02-26T00:00:00Z') },
          { start: new Date('2015-02-26T00:00:00Z'), end: new Date('2015-02-27T00:00:00Z') },

          { start: new Date('2015-02-28T00:00:00Z'), end: null }
        ]
      }).unifyElements().toJS()).to.deep.equal({
        setType: 'TIME_RANGE',
        elements: [
          { start: new Date('2015-02-20T00:00:00Z'), end: new Date('2015-02-23T00:00:00Z') },
          { start: new Date('2015-02-25T00:00:00Z'), end: new Date('2015-02-27T00:00:00Z') },
          { start: new Date('2015-02-28T00:00:00Z'), end: null }
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


  describe("#simplify", () => {
    it('works correctly', () => {
      let s = Set.fromJS({
        setType: 'NUMBER_RANGE',
        elements: [
          { start: 1, end: 3 },
          { start: 2, end: 5 },
        ]
      });

      expect(s.simplifyCover().toJS()).to.deep.equal({
        "end": 5,
        "start": 1
      });
    });
  });


  describe("#has", () => {
    let strNullSet = Set.fromJS({
      setType: 'STRING',
      elements: [
        "null"
      ]
    });

    let nrs = Set.fromJS({
      setType: 'NUMBER_RANGE',
      elements: [
        { start: 1, end: 3 },
        { start: 10, end: 30 }
      ]
    });

    let trs = Set.fromJS({
      setType: 'TIME_RANGE',
      elements: [
        { start: new Date('2015-09-12T22:00:00Z'), end: new Date('2015-09-12T23:00:00Z') },
        { start: new Date('2015-09-12T23:00:00Z'), end: new Date('2015-09-13T00:00:00Z') }
      ]
    });

    it('works correctly with atomics', () => {
      expect(nrs.has(3), '3').to.equal(false);
      expect(nrs.has(4), '4').to.equal(false);

      expect(strNullSet.has("null"), '"null"').to.equal(true);
      expect(strNullSet.has(null), 'null').to.equal(false);
    });

    it('works correctly with number ranges', () => {
      expect(nrs.has(NumberRange.fromJS({ start: 1, end: 3 })), '1-3').to.equal(true);
      expect(nrs.has(NumberRange.fromJS({ start: 2, end: 3 })), '2-3').to.equal(false);

      expect(nrs.has("lol"), '"lol"').to.equal(false);
      expect(nrs.has("null"), '"null"').to.equal(false);
      expect(nrs.has(null), 'null').to.equal(false);
    });

    it('works correctly with time ranges', () => {
      expect(trs.has(TimeRange.fromJS({ start: new Date('2015-09-12T22:00:00Z'), end: new Date('2015-09-12T23:00:00Z') }))).to.equal(true);
      expect(trs.has(TimeRange.fromJS({ start: new Date('2015-09-12T22:00:00Z'), end: new Date('2015-09-12T22:30:00Z') }))).to.equal(false);
    });
  });

  describe("#contains", () => {
    let strNullSet = Set.fromJS({
      setType: 'STRING',
      elements: [
        "null"
      ]
    });

    let nrs = Set.fromJS({
      setType: 'NUMBER_RANGE',
      elements: [
        { start: 1, end: 3 },
        { start: 10, end: 30 }
      ]
    });

    let trs = Set.fromJS({
      setType: 'TIME_RANGE',
      elements: [
        { start: new Date('2015-09-12T22:00:00Z'), end: new Date('2015-09-12T23:00:00Z') },
        { start: new Date('2015-09-12T23:00:00Z'), end: new Date('2015-09-13T00:00:00Z') }
      ]
    });

    it('works correctly with atomics', () => {
      expect(nrs.contains(1), '1').to.equal(true);
      expect(nrs.contains(2), '2').to.equal(true);
      expect(nrs.contains(3), '3').to.equal(false);
      expect(nrs.contains(4), '4').to.equal(false);

      expect(nrs.contains(15), '15').to.equal(true);

      expect(strNullSet.contains("null"), '"null"').to.equal(true);
      expect(strNullSet.contains(null), 'null').to.equal(false);
    });

    it('works correctly with number ranges', () => {
      expect(nrs.contains(NumberRange.fromJS({ start: 1, end: 2 })), '1-2').to.equal(true);
      expect(nrs.contains(NumberRange.fromJS({ start: 2, end: 3 })), '2-3').to.equal(true);
      expect(nrs.contains(NumberRange.fromJS({ start: 3, end: 4 })), '3-4').to.equal(false);
    });

    it('works correctly with time ranges', () => {
      expect(trs.contains(NumberRange.fromJS({ start: new Date('2015-09-12T23:00:00Z'), end: new Date('2015-09-13T00:00:00Z') }))).to.equal(true);
    });

    it('works correctly with string sets', () => {
      expect(strNullSet.contains(Set.fromJS(["lol"])), '["lol"]').to.equal(false);
      expect(strNullSet.contains(Set.fromJS(["null"])), '["null"]').to.equal(true);
      expect(strNullSet.contains(Set.fromJS([null])), '[null]').to.equal(false);
    });

    it('works correctly with number sets', () => {
      expect(nrs.contains(Set.fromJS([{ start: 1, end: 2 }])), '[1-2]').to.equal(true);
      expect(nrs.contains(Set.fromJS([{ start: 2, end: 3 }])), '[2-3]').to.equal(true);
      expect(nrs.contains(Set.fromJS([{ start: 1, end: 3 }])), '[1-3]').to.equal(true);
      expect(nrs.contains(Set.fromJS([{ start: 3, end: 4 }])), '[3-4]').to.equal(false);

      expect(nrs.contains(Set.fromJS([{ start: 2, end: 3 }, { start: 15, end: 16 }])), '[2-3, 15-16]').to.equal(true);
      expect(nrs.contains(Set.fromJS([{ start: 2, end: 3 }, { start: 15, end: 36 }])), '[2-3, 15-36]').to.equal(false);

      expect(nrs.contains(Set.fromJS(["lol"])), '["lol"]').to.equal(false);
      expect(nrs.contains(Set.fromJS(["null"])), '["null"]').to.equal(false);
      expect(nrs.contains(Set.fromJS([null])), '[null]').to.equal(false);
    });

    it('works correctly with time sets', () => {
      expect(trs.contains(Set.fromJS([
        { start: new Date('2015-09-12T23:00:00Z'), end: new Date('2015-09-13T00:00:00Z') }
      ]))).to.equal(true);

      expect(trs.contains(Set.fromJS([
        { start: new Date('2015-09-12T23:00:00Z'), end: new Date('2015-09-12T23:20:00Z') },
        { start: new Date('2015-09-12T23:40:00Z'), end: new Date('2015-09-13T00:00:00Z') }
      ]))).to.equal(true);
    });

  });

});
