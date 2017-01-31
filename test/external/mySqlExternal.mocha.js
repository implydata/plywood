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
const { PassThrough } = require('readable-stream');
let { sane } = require('../utils');

let plywood = require('../plywood');
let { External, TimeRange, $, ply, r, AttributeInfo } = plywood;

describe("MySQLExternal", () => {

  describe("should work when getting back no data", () => {
    let emptyExternal = External.fromJS({
      engine: 'mysql',
      source: 'wikipedia',
      attributes: [
        { name: 'time', type: 'TIME' },
        { name: 'language', type: 'STRING' },
        { name: 'page', type: 'STRING' },
        { name: 'added', type: 'NUMBER' }
      ]
    }, () => {
      const stream = new PassThrough({ objectMode: true });
      setTimeout(() => { stream.end(); }, 1);
      return stream;
    });

    it("should return null correctly on a totals query", () => {
      let ex = ply()
        .apply('Count', '$wiki.count()');

      return ex.compute({ wiki: emptyExternal })
        .then((result) => {
          expect(result.toJS()).to.deep.equal([
            { Count: 0 }
          ]);
        });
    });

    it("should return null correctly on a timeseries query", () => {
      let ex = $('wiki').split("$time.timeBucket(P1D, 'Etc/UTC')", 'Time')
        .apply('Count', '$wiki.count()')
        .sort('$Time', 'ascending');

      return ex.compute({ wiki: emptyExternal })
        .then((result) => {
          expect(result.toJS()).to.deep.equal([]);
        });
    });

    it("should return null correctly on a topN query", () => {
      let ex = $('wiki').split("$page", 'Page')
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5);

      return ex.compute({ wiki: emptyExternal })
        .then((result) => {
          expect(result.toJS()).to.deep.equal([]);
        });
    });

    it("should return null correctly on a select query", () => {
      let ex = $('wiki');

      return ex.compute({ wiki: emptyExternal })
        .then((result) => {
          expect(AttributeInfo.toJSs(result.attributes)).to.deep.equal([
            { name: 'time', type: 'TIME' },
            { name: 'language', type: 'STRING' },
            { name: 'page', type: 'STRING' },
            { name: 'added', type: 'NUMBER' }
          ]);

          expect(result.toJS()).to.deep.equal([]);
          expect(result.toCSV()).to.equal('time,language,page,added');
        });
    });
  });

});
