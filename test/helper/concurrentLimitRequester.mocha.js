/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2019 Imply Data, Inc.
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
      const s = streams[query];
      s.emit('error', new Error('fail'));
      s.end();
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

  it("limit works when ok", () => {
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

  it("limit works when error", () => {
    let requester = makeRequester();
    let concurrentLimitRequester = concurrentLimitRequesterFactory({
      requester,
      concurrentLimit: 2
    });

    let nextQuery = 'a';
    let ra = toArray(concurrentLimitRequester({ query: 'a' }))
      .catch((e) => {
        expect(e.message).to.equal('fail');
        expect(nextQuery).to.equal('a');
        nextQuery = 'b';
        expect(requester.hasQuery('c', 'has c')).to.equal(true);
        expect(requester.hasQuery('d', 'has d')).to.equal(false);

        requester.reject('b');
      });

    let rb = toArray(concurrentLimitRequester({ query: 'b' }))
      .catch((e) => {
        expect(e.message).to.equal('fail');
        expect(nextQuery).to.equal('b');

        expect(requester.hasQuery('d', 'has d')).to.equal(true);

        nextQuery = 'c';
        requester.resolve('c');
      });

    let rc = toArray(concurrentLimitRequester({ query: 'c' }))
      .then((res) => {
        expect(res).to.deep.equal([1, 2, 3]);
        expect(nextQuery).to.equal('c');
        nextQuery = 'd';
        requester.resolve('d');
      });

      let rd = toArray(concurrentLimitRequester({ query: 'd' }))
      .then((res) => {
        expect(res).to.deep.equal([1, 2, 3]);
        expect(nextQuery).to.equal('d');
      });

    expect(requester.hasQuery('a'), 'has a').to.equal(true);
    expect(requester.hasQuery('b'), 'has b').to.equal(true);
    expect(requester.hasQuery('c'), 'has c').to.equal(false);
    expect(requester.hasQuery('d'), 'has d').to.equal(false);
    requester.reject('a');

    return Promise.all([ra, rb, rc, rd]);
  });
});
