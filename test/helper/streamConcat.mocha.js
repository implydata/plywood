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
const { Readable } = require('readable-stream');
const toArray = require('stream-to-array');

const { StreamConcat } = require("../../build/plywood");

function emits(arr) {
  let out = new Readable({
    objectMode: true,
    read: function() {
      if (!out.index) out.index = 0;
      out.push(arr[out.index++] || null);
    }
  });
  return out;
}

function emitsError(message) {
  let out = new Readable({
    objectMode: true,
    read: function() {
      out.emit('error', new Error(message));
    }
  });
  return out;
}

describe('Stream Concat', () => {
  it('concatenates', () => {
    return toArray(new StreamConcat({
      streams: [emits('abc'), emits('def')],
      objectMode: true
    }))
      .then((a) => {
        expect(a).to.deep.equal(['a', 'b', 'c', 'd', 'e', 'f']);
      });
  });

  it('concatenates with function', () => {
    let run = 0;

    const sc = new StreamConcat({
      streams: () => {
        run++;
        switch (run) {
          case 1: return emits('abc');
          case 2: return emits('def');
          default: return null
        }
      },
      objectMode: true
    });

    return toArray(sc)
      .then((a) => {
        expect(a).to.deep.equal(['a', 'b', 'c', 'd', 'e', 'f']);
      });
  });

  it('forward errors 1', () => {
    return toArray(new StreamConcat({
      streams: [emitsError('oops')],
      objectMode: true
    }))
      .then(() => {
        throw new Error('DID_NOT_THROW');
      })
      .catch((e) => {
        expect(e.message).to.equal('oops');
      });
  });

  it('forward errors 2', () => {
    return toArray(new StreamConcat({
      streams: [emits('abc'), emitsError('oops')],
      objectMode: true
    }))
      .then(() => {
        throw new Error('DID_NOT_THROW');
      })
      .catch((e) => {
        expect(e.message).to.equal('oops');
      });
  });

});
