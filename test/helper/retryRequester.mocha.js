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
const toArray = require('stream-to-array');

let { retryRequesterFactory } = require("../../build/plywood");

describe("Retry Requester", () => {
  let makeRequester = (failNumber, isTimeout) => {
    return (request) => {
      const stream = new PassThrough({ objectMode: true });
      setTimeout(() => {
        if (failNumber > 0) {
          failNumber--;
          stream.emit('error', new Error(isTimeout ? 'timeout' : 'some error'));
        } else {
          stream.emit('meta', { lol: 33 });
          stream.write(1);
          stream.write(2);
          stream.write(3);
          stream.end();
        }
      }, 1);
      return stream;
    };
  };


  it("no retry needed (no fail)", () => {
    let retryRequester = retryRequesterFactory({
      requester: makeRequester(0),
      delay: 20,
      retry: 2
    });

    return toArray(retryRequester({}))
      .then((res) => {
        expect(res).to.deep.equal([1, 2, 3])
      });
  });

  it("one fail", () => {
    let retryRequester = retryRequesterFactory({
      requester: makeRequester(1),
      delay: 20,
      retry: 2
    });

    return toArray(retryRequester({}))
      .then((res) => {
        expect(res).to.deep.equal([1, 2, 3])
      });
  });

  it("two fails", () => {
    let retryRequester = retryRequesterFactory({
      requester: makeRequester(2),
      delay: 20,
      retry: 2
    });

    return toArray(retryRequester({}))
      .then((res) => {
        expect(res).to.deep.equal([1, 2, 3])
      });
  });

  it("two fails forwards meta", () => {
    let retryRequester = retryRequesterFactory({
      requester: makeRequester(2),
      delay: 20,
      retry: 2
    });

    const rs = retryRequester({});

    let seenMeta = false;
    rs.on('meta', (meta) => {
      seenMeta = true;
      expect(meta).to.deep.equal({ lol: 33 });
    });

    return toArray(rs)
      .then((res) => {
        expect(seenMeta).to.equal(true);
        expect(res).to.deep.equal([1, 2, 3])
      });
  });

  it("three fails", () => {
    let retryRequester = retryRequesterFactory({
      requester: makeRequester(3),
      delay: 20,
      retry: 2
    });

    return toArray(retryRequester({}))
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

    return toArray(retryRequester({}))
      .then(() => {
        throw new Error('DID_NOT_THROW');
      })
      .catch((err) => {
        expect(err.message).to.equal('timeout');
      });
  });
});
