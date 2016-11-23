/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
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

let { expect } = require("chai");

let Q = require('q');

let { concurrentLimitRequesterFactory } = require("../../build/plywood");

describe("Retry requester", () => {
  let makeRequester = () => {
    let deferreds = {};

    let requester = (request) => {
      let deferred = Q.defer();
      deferreds[request.query] = deferred;
      return deferred.promise;
    };

    requester.hasQuery = (query) => {
      return Boolean(deferreds[query]);
    };
    requester.resolve = (query) => {
      return deferreds[query].resolve([1, 2, 3]);
    };
    requester.reject = (query) => {
      return deferreds[query].reject(new Error('fail'));
    };
    return requester;
  };

  it("basic works", (testComplete) => {
    let requester = makeRequester();
    let concurrentLimitRequester = concurrentLimitRequesterFactory({
      requester,
      concurrentLimit: 2
    });

    concurrentLimitRequester({ query: 'a' })
      .then((res) => {
        expect(res).to.be.an('array');
        testComplete();
      })
      .done();

    return requester.resolve('a');
  });

  it("limit works", (testComplete) => {
    let requester = makeRequester();
    let concurrentLimitRequester = concurrentLimitRequesterFactory({
      requester,
      concurrentLimit: 2
    });

    let nextQuery = 'a';
    concurrentLimitRequester({ query: 'a' })
      .then((res) => {
        expect(res).to.be.an('array');
        expect(nextQuery).to.equal('a');
        return nextQuery = 'b';
      })
      .done();

    concurrentLimitRequester({ query: 'b' })
      .then((res) => {
        expect(res).to.be.an('array');
        expect(nextQuery).to.equal('b');
        nextQuery = 'c';
        expect(requester.hasQuery('c', 'has c')).to.equal(true);
        return requester.resolve('c');
      })
      .done();

    concurrentLimitRequester({ query: 'c' })
      .then((res) => {
        expect(res).to.be.an('array');
        expect(nextQuery).to.equal('c');
        testComplete();
      })
      .done();

    expect(requester.hasQuery('a'), 'has a').to.equal(true);
    expect(requester.hasQuery('b'), 'has b').to.equal(true);
    expect(requester.hasQuery('c'), 'has c').to.equal(false);
    requester.resolve('a');
    return requester.resolve('b');
  });
});
