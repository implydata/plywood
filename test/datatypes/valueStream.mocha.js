/*
 * Copyright 2016-2017 Imply Data, Inc.
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
const { Timezone } = require('chronoshift');

const { sane } = require('../utils');
const plywood = require('../plywood');
const { Dataset, AttributeInfo, $, Set, r, iteratorFactory, PlywoodValueBuilder } = plywood;

function toJSON(obj) {
  return JSON.parse(JSON.stringify(obj));
}

describe("ValueStream", () => {

  describe("iteratorFactory", () => {
    it("works for simple value", () => {
      let dsi = iteratorFactory(5);

      let bits = [];
      let bit;
      while (bit = dsi()) {
        bits.push(bit);
      }

      expect(toJSON(bits)).to.deep.equal([
        {
          "type": "value",
          "value": 5
        }
      ]);
    });

    it("works for Dataset", () => {
      let ds = Dataset.fromJS([
        {
          "time": new Date("2015-09-12T00:46:58.771Z"),
          "channel": "#en.wikipedia",
          "isAnonymous": false,
          "deleted": 0,
          "users": [
            { "name": "Vadim", x: 2 },
            { "name": "Eva", x: 3 }
          ]
        },
        {
          "time": new Date("2015-09-12T00:48:20.157Z"),
          "channel": "#en.wikipedia",
          "isAnonymous": true,
          "deleted": 26,
          "users": [
            { "name": "James", x: 8 },
            { "name": "Charlie", x: 30 }
          ]
        }
      ]);

      let dsi = iteratorFactory(ds);

      let bits = [];
      let bit;
      while (bit = dsi()) {
        bits.push(bit);
      }

      expect(toJSON(bits)).to.deep.equal([
        {
          "attributes": [
            {
              "name": "time",
              "type": "TIME"
            },
            {
              "name": "channel",
              "type": "STRING"
            },
            {
              "name": "isAnonymous",
              "type": "BOOLEAN"
            },
            {
              "name": "deleted",
              "type": "NUMBER"
            },
            {
              "name": "users",
              "type": "DATASET"
            }
          ],
          "type": "init"
        },
        {
          "datum": {
            "channel": "#en.wikipedia",
            "deleted": 0,
            "isAnonymous": false,
            "time": "2015-09-12T00:46:58.771Z"
          },
          "type": "datum"
        },
        {
          "attribute": "users",
          "type": "within",
          "within": {
            "attributes": [
              {
                "name": "name",
                "type": "STRING"
              },
              {
                "name": "x",
                "type": "NUMBER"
              }
            ],
            "type": "init"
          }
        },
        {
          "attribute": "users",
          "type": "within",
          "within": {
            "datum": {
              "name": "Vadim",
              "x": 2
            },
            "type": "datum"
          }
        },
        {
          "attribute": "users",
          "type": "within",
          "within": {
            "datum": {
              "name": "Eva",
              "x": 3
            },
            "type": "datum"
          }
        },
        {
          "datum": {
            "channel": "#en.wikipedia",
            "deleted": 26,
            "isAnonymous": true,
            "time": "2015-09-12T00:48:20.157Z"
          },
          "type": "datum"
        },
        {
          "attribute": "users",
          "type": "within",
          "within": {
            "attributes": [
              {
                "name": "name",
                "type": "STRING"
              },
              {
                "name": "x",
                "type": "NUMBER"
              }
            ],
            "type": "init"
          }
        },
        {
          "attribute": "users",
          "type": "within",
          "within": {
            "datum": {
              "name": "James",
              "x": 8
            },
            "type": "datum"
          }
        },
        {
          "attribute": "users",
          "type": "within",
          "within": {
            "datum": {
              "name": "Charlie",
              "x": 30
            },
            "type": "datum"
          }
        }
      ]);
    });

    it("works for Dataset with keys", () => {
      let ds = Dataset.fromJS({
        keys: ['channel'],
        data: [
          {
            "channel": "#en.wikipedia",
            "deleted": 0,
          },
          {
            "channel": "#en.wikipedia",
            "deleted": 26,
          }
        ]
      });

      let dsi = iteratorFactory(ds);

      let bits = [];
      let bit;
      while (bit = dsi()) {
        bits.push(bit);
      }

      expect(toJSON(bits)).to.deep.equal([
        {
          "type": "init",
          "attributes": [
            {
              "name": "channel",
              "type": "STRING"
            },
            {
              "name": "deleted",
              "type": "NUMBER"
            }
          ],
          "keys": [
            "channel"
          ]
        },
        {
          "type": "datum",
          "datum": {
            "channel": "#en.wikipedia",
            "deleted": 0
          }
        },
        {
          "type": "datum",
          "datum": {
            "channel": "#en.wikipedia",
            "deleted": 26
          }
        }
      ]);
    });

  });

  describe("PlywoodValueBuilder", () => {
    it("works in base case", () => {
      let pvb = new PlywoodValueBuilder();
      expect(pvb.getValue()).to.deep.equal(null);
    });

    it("works in null case", () => {
      let pvb = new PlywoodValueBuilder();
      pvb.processBit({ type: 'value', value: null });
      expect(pvb.getValue()).to.equal(null);
    });

    it("works in false case", () => {
      let pvb = new PlywoodValueBuilder();
      pvb.processBit({ type: 'value', value: false });
      expect(pvb.getValue()).to.equal(false);
    });

    it("works in zero case", () => {
      let pvb = new PlywoodValueBuilder();
      pvb.processBit({ type: 'value', value: 0 });
      expect(pvb.getValue()).to.equal(0);
    });

    it("works in value case", () => {
      let pvb = new PlywoodValueBuilder();
      pvb.processBit({ type: 'value', value: 5 });
      expect(pvb.getValue()).to.equal(5);
    });

    it("works in dataset case (no init)", () => {
      const bits = [
        {
          "type": "datum",
          "datum": {
            "channel": "#en.wikipedia",
            "deleted": 0,
            "isAnonymous": false,
            "time": new Date('2015-09-12T00:46:58.771Z')
          }
        },
        {
          "type": "within",
          "attribute": "users",
          "within": {
            "type": "datum",
            "datum": {
              "name": "Vadim",
              "x": 2
            }
          }
        },
        {
          "type": "within",
          "attribute": "users",
          "within": {
            "type": "datum",
            "datum": {
              "name": "Eva",
              "x": 3
            }
          }
        }
      ];

      let pvb = new PlywoodValueBuilder();
      for (let bit of bits) pvb.processBit(bit);

      expect(pvb.getValue().toJS().data).to.deep.equal([
        {
          "channel": "#en.wikipedia",
          "deleted": 0,
          "isAnonymous": false,
          "time": new Date('2015-09-12T00:46:58.771Z'),
          "users": {
            "attributes": [
              {
                "name": "name",
                "type": "STRING"
              },
              {
                "name": "x",
                "type": "NUMBER"
              }
            ],
            "data": [
              {
                "name": "Vadim",
                "x": 2
              },
              {
                "name": "Eva",
                "x": 3
              }
            ]
          }
        }
      ]);

    });

    it("works in dataset case (with init)", () => {
      const bits = [
        {
          "type": "init",
          "keys": [],
          "attributes": AttributeInfo.fromJSs([
            {
              "name": "time",
              "type": "TIME"
            },
            {
              "name": "channel",
              "type": "STRING"
            },
            {
              "name": "isAnonymous",
              "type": "BOOLEAN"
            },
            {
              "name": "deleted",
              "type": "NUMBER"
            },
            {
              "name": "users",
              "type": "DATASET"
            }
          ])
        },
        {
          "type": "datum",
          "datum": {
            "channel": "#en.wikipedia",
            "deleted": 0,
            "isAnonymous": false,
            "time": new Date('2015-09-12T00:46:58.771Z')
          }
        },
        {
          "type": "within",
          "attribute": "users",
          "within": {
            "type": "init",
            "keys": ["name"],
            "attributes": AttributeInfo.fromJSs([
              {
                "name": "name",
                "type": "STRING"
              },
              {
                "name": "x",
                "type": "NUMBER"
              }
            ])
          }
        },
        {
          "type": "within",
          "attribute": "users",
          "within": {
            "type": "datum",
            "datum": {
              "name": "Vadim",
              "x": 2
            }
          }
        },
        {
          "type": "within",
          "attribute": "users",
          "within": {
            "type": "datum",
            "datum": {
              "name": "Eva",
              "x": 3
            }
          }
        }
      ];

      let pvb = new PlywoodValueBuilder();
      for (let bit of bits) pvb.processBit(bit);

      expect(pvb.getValue().toJS()).to.deep.equal({
        "attributes": [
          {
            "name": "time",
            "type": "TIME"
          },
          {
            "name": "channel",
            "type": "STRING"
          },
          {
            "name": "isAnonymous",
            "type": "BOOLEAN"
          },
          {
            "name": "deleted",
            "type": "NUMBER"
          },
          {
            "name": "users",
            "type": "DATASET"
          }
        ],
        "data": [
          {
            "channel": "#en.wikipedia",
            "deleted": 0,
            "isAnonymous": false,
            "time": new Date('2015-09-12T00:46:58.771Z'),
            "users": {
              "attributes": [
                {
                  "name": "name",
                  "type": "STRING"
                },
                {
                  "name": "x",
                  "type": "NUMBER"
                }
              ],
              "data": [
                {
                  "name": "Vadim",
                  "x": 2
                },
                {
                  "name": "Eva",
                  "x": 3
                }
              ],
              "keys": [
                "name"
              ]
            }
          }
        ]
      });

    });

  });

  describe("iteratorFactory => PlywoodValueBuilder", () => {
    it("in empty Dataset case", () => {
      let ds = Dataset.fromJS([]);

      let dsi = iteratorFactory(ds);
      let pvb = new PlywoodValueBuilder();

      let bit;
      while (bit = dsi()) {
        pvb.processBit(bit);
      }

      expect(pvb.getValue().toJS()).to.deep.equal(ds.toJS());
    });

    it("in flat Dataset case", () => {
      let ds = Dataset.fromJS({
        keys: ['cityName'],
        data: [
          {
            "time": new Date("2015-09-12T00:46:58.771Z"),
            "channel": "#en.wikipedia",
            "cityName": "SF",
            "comment": "added project",
            "countryIsoCode": "US",
            "countryName": "United States",
            "isAnonymous": false,
            "delta": 36,
            "added": 36,
            "deleted": 0
          },
          {
            "time": new Date("2015-09-12T00:48:20.157Z"),
            "channel": "#en.wikipedia",
            "cityName": "Campbell",
            "comment": "Rectifying someone's mischief",
            "countryIsoCode": "US",
            "countryName": "United States",
            "isAnonymous": true,
            "delta": -26,
            "added": 0,
            "deleted": 26
          }
        ]
      });

      let dsi = iteratorFactory(ds);
      let pvb = new PlywoodValueBuilder();

      let bit;
      while (bit = dsi()) {
        pvb.processBit(bit);
      }

      expect(pvb.getValue().toJS()).to.deep.equal(ds.toJS());
    });

    it("in nested Dataset case", () => {
      let ds = Dataset.fromJS([
        {
          "time": new Date("2015-09-12T00:46:58.771Z"),
          "channel": "#en.wikipedia",
          "isAnonymous": false,
          "deleted": 0,
          "users": [
            { "name": "Vadim", x: 2, nicknames: [{ nick: "Vadimon" }] },
            { "name": "Eva", x: 3, nicknames: null }
          ]
        },
        {
          "time": new Date("2015-09-12T00:48:20.157Z"),
          "channel": "#en.wikipedia",
          "isAnonymous": true,
          "deleted": 26,
          "users": [
            { "name": "James", x: 8, nicknames: null },
            { "name": "Charlie", x: 30, nicknames: null }
          ]
        }
      ]);

      let dsi = iteratorFactory(ds);
      let pvb = new PlywoodValueBuilder();

      let bit;
      while (bit = dsi()) {
        pvb.processBit(bit);
      }

      expect(pvb.getValue().toJS()).to.deep.equal(ds.toJS());
    });

  });

});
