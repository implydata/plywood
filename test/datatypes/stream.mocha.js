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
const { Dataset, AttributeInfo, $, Set, r, dfsDatasetIteratorFactory, PlywoodValueBuilder } = plywood;

describe("Stream", () => {

  describe("works", () => {
    it("in value case", () => {
      let pvb = new PlywoodValueBuilder();
      pvb.processBit({ __$$type: 'value', value: 5 });

      expect(pvb.getValue()).to.equal(5);

    });

    it("in real Dataset case", () => {
      let ds = Dataset.fromJS([
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
      ]);

      let dsi = dfsDatasetIteratorFactory(ds);
      let pvb = new PlywoodValueBuilder();

      let bit;
      while (bit = dsi()) {
        pvb.processBit(bit);
      }

      expect(pvb.getValue().toJS()).to.deep.equal(ds.toJS());

    });
  });

});
