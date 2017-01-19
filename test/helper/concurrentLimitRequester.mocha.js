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

const { PassThrough } = require('readable-stream');
let Promise = require('any-promise');
const toArray = require('stream-to-array');

let { concurrentLimitRequesterFactory } = require("../../build/plywood");

describe("Concurrent Limit Requester", () => {
  let makeRequester = () => {
    let streams = {};

    let requester = (request) => {
      return streams[request.query] = new PassThrough({ objectMode: true });
    };

    requester.hasQuery = (query) => {
      return Boolean(streams[query]);
    };
    requester.resolve = (query) => {
      const s = streams[query];
      s.push(1);
      s.push(2);
      s.push(3);
      s.push(null);
    };
    requester.reject = (query) => {
      streams[query].emit('error', new Error('fail'));
    };
    return requester;
  };

  it("basic works", () => {
    let requester = makeRequester();
    let concurrentLimitRequester = concurrentLimitRequesterFactory({
      requester,
      concurrentLimit: 2
    });

    let p = toArray(concurrentLimitRequester({ query: 'a' }))
      .then((res) => {
        expect(res).to.deep.equal([1, 2, 3]);
      });

    requester.resolve('a');
    return p;
  });

  it("limit works", () => {
    let requester = makeRequester();
    let concurrentLimitRequester = concurrentLimitRequesterFactory({
      requester,
      concurrentLimit: 2
    });

    let nextQuery = 'a';
    let ra = toArray(concurrentLimitRequester({ query: 'a' }))
      .then((res) => {
        expect(res).to.deep.equal([1, 2, 3]);
        expect(nextQuery).to.equal('a');
        nextQuery = 'b';
      });

    let rb = toArray(concurrentLimitRequester({ query: 'b' }))
      .then((res) => {
        expect(res).to.deep.equal([1, 2, 3]);
        expect(nextQuery).to.equal('b');
        nextQuery = 'c';
        expect(requester.hasQuery('c', 'has c')).to.equal(true);
        requester.resolve('c');
      });

    let rc = toArray(concurrentLimitRequester({ query: 'c' }))
      .then((res) => {
        expect(res).to.deep.equal([1, 2, 3]);
        expect(nextQuery).to.equal('c');
      });

    expect(requester.hasQuery('a'), 'has a').to.equal(true);
    expect(requester.hasQuery('b'), 'has b').to.equal(true);
    expect(requester.hasQuery('c'), 'has c').to.equal(false);
    requester.resolve('a');
    requester.resolve('b');
    return Promise.all([ra, rb, rc]);
  });

});
