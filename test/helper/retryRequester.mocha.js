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

var { expect } = require("chai");

var Q = require('q');

var plywood = require("../../build/plywood");
var { retryRequesterFactory } = plywood.helper;

describe("Retry requester", () => {
  var makeRequester = (failNumber, isTimeout) => {
    return (request) => {
      if (failNumber > 0) {
        failNumber--;
        return Q.reject(new Error(isTimeout ? 'timeout' : 'some error'));
      } else {
        return Q([1, 2, 3]);
      }
    };
  };


  it("no retry needed (no fail)", (testComplete) => {
    var retryRequester = retryRequesterFactory({
      requester: makeRequester(0),
      delay: 20,
      retry: 2
    });

    return retryRequester({})
      .then((res) => {
        expect(res).to.be.an('array');
        testComplete();
      })
      .done();
  });

  it("one fail", (testComplete) => {
    var retryRequester = retryRequesterFactory({
      requester: makeRequester(1),
      delay: 20,
      retry: 2
    });

    return retryRequester({})
      .then((res) => {
        expect(res).to.be.an('array');
        testComplete();
      })
      .done();
  });

  it("two fails", (testComplete) => {
    var retryRequester = retryRequesterFactory({
      requester: makeRequester(2),
      delay: 20,
      retry: 2
    });

    return retryRequester({})
      .then((res) => {
        expect(res).to.be.an('array');
        testComplete();
      })
      .done();
  });

  it("three fails", (testComplete) => {
    var retryRequester = retryRequesterFactory({
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
        testComplete();
      })
      .done();
  });

  it("timeout", (testComplete) => {
    var retryRequester = retryRequesterFactory({
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
        testComplete();
      })
      .done();
  });
});
