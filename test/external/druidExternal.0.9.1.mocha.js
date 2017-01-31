/*
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
let Promise = require('any-promise');
let { sane } = require('../utils');

let plywood = require('../plywood');
let { External, TimeRange, $, ply, r, AttributeInfo } = plywood;

let timeFilter = $('time').in(TimeRange.fromJS({
  start: new Date("2013-02-26T00:00:00Z"),
  end: new Date("2013-02-27T00:00:00Z")
}));

let context = {
  wiki: External.fromJS({
    engine: 'druid',
    source: 'wikipedia',
    timeAttribute: 'time',
    attributes: [
      { name: 'time', type: 'TIME' },
      { name: 'sometimeLater', type: 'TIME' },
      { name: 'language', type: 'STRING' },
      { name: 'page', type: 'STRING' },
      { name: 'tags', type: 'SET/STRING' },
      { name: 'commentLength', type: 'NUMBER' },
      { name: 'isRobot', type: 'BOOLEAN' },
      { name: 'count', type: 'NUMBER', unsplitable: true },
      { name: 'added', type: 'NUMBER', unsplitable: true },
      { name: 'deleted', type: 'NUMBER', unsplitable: true },
      { name: 'inserted', type: 'NUMBER', unsplitable: true },
      { name: 'delta_hist', special: 'histogram' }
    ],
    derivedAttributes: {
      pageInBrackets: "'[' ++ $page ++ ']'"
    },
    filter: timeFilter,
    allowSelectQueries: true,
    version: '0.9.1',
    customAggregations: {
      crazy: {
        accessType: 'getSomeCrazy',
        aggregation: {
          type: 'crazy',
          the: 'borg will rise again',
          activate: false
        }
      },
      stupid: {
        accessType: 'iAmWithStupid',
        aggregation: {
          type: 'stoopid',
          onePlusOne: 3,
          globalWarming: 'hoax'
        }
      }
    }
  })
};


describe("DruidExternal 0.9.1", () => {

  describe("filters", () => {

    it("works with .timePart().in()", () => {
      let ex = $('wiki').filter($('time').timePart('HOUR_OF_DAY').in([3, 5]));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      let druidExternal = ex.external;
      expect(() => {
        druidExternal.getQueryAndPostTransform();
      }).to.throw('can not do secondary filtering on primary time dimension (https://github.com/druid-io/druid/issues/2816)');
    });

  });

});
