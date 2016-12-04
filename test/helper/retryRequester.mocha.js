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

let Promise = require('any-promise');

let { retryRequesterFactory } = require("../../build/plywood");

describe("Retry requester", () => {
  let makeRequester = (failNumber, isTimeout) => {
    return (request) => {
      if (failNumber > 0) {
        failNumber--;
        return Promise.reject(new Error(isTimeout ? 'timeout' : 'some error'));
      } else {
        return Promise.resolve([1, 2, 3]);
      }
    };
  };


  it("no retry needed (no fail)", () => {
    let retryRequester = retryRequesterFactory({
      requester: makeRequester(0),
      delay: 20,
      retry: 2
    });

    return retryRequester({})
      .then((res) => {
        expect(res).to.be.an('array');
      });
  });

  it("one fail", () => {
    let retryRequester = retryRequesterFactory({
      requester: makeRequester(1),
      delay: 20,
      retry: 2
    });

    return retryRequester({})
      .then((res) => {
        expect(res).to.be.an('array');
      });
  });

  it("two fails", () => {
    let retryRequester = retryRequesterFactory({
      requester: makeRequester(2),
      delay: 20,
      retry: 2
    });

    return retryRequester({})
      .then((res) => {
        expect(res).to.be.an('array');
      });
  });

  it("three fails", () => {
    let retryRequester = retryRequesterFactory({
      requester: makeRequester(3),
      delay: 20,
      retry: 2
    });

    return retryRequester({})
      .then(() => {
        throw new Error('DID_NOT_THROW');
      })
      .catch((err) => {
        expect(err.message).to.equal('some error');
      });
  });

  it("timeout", () => {
    let retryRequester = retryRequesterFactory({
      requester: makeRequester(1, true),
      delay: 20,
      retry: 2
    });

    return retryRequester({})
      .then(() => {
        throw new Error('DID_NOT_THROW');
      })
      .catch((err) => {
        expect(err.message).to.equal('timeout');
      });
  });
});
