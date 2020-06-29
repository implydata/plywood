/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2020 Imply Data, Inc.
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

const { expect } = require('chai');

const { PassThrough } = require('readable-stream');
const toArray = require('stream-to-array');

let { verboseRequesterFactory } = require('../../build/plywood');

describe('Verbose requester', () => {
  let requester = request => {
    const stream = new PassThrough({ objectMode: true });
    setTimeout(() => {
      if (/^fail/.test(request.query)) {
        stream.emit('error', new Error('some error'));
        stream.end();
      } else {
        stream.write(1);
        stream.write(2);
        stream.write(3);
        stream.end();
      }
    }, 1);
    return stream;
  };

  it('works on success', () => {
    let lines = [];
    let verboseRequester = verboseRequesterFactory({
      name: 'rq1',
      requester: requester,
      printLine(line) {
        return lines.push(line);
      },
    });

    return toArray(verboseRequester({ query: 'Query1' })).then(res => {
      expect(res).to.be.an('array');
      expect(lines.join('\n').replace(/\d+ms/, 'Xms')).to.equal(
        `vvvvvvvvvvvvvvvvvvvvvvvvvv
Requester rq1 sending query 1:
"Query1"
^^^^^^^^^^^^^^^^^^^^^^^^^^
vvvvvvvvvvvvvvvvvvvvvvvvvv
Requester rq1 got result from query 1: (in Xms)
[
  1,
  2,
  3
]
^^^^^^^^^^^^^^^^^^^^^^^^^^`,
      );
    });
  });

  it('works on failure', () => {
    let lines = [];
    let verboseRequester = verboseRequesterFactory({
      name: 'rq2',
      requester: requester,
      printLine(line) {
        return lines.push(line);
      },
    });

    return toArray(verboseRequester({ query: 'failThis' }))
      .then(() => {
        throw new Error('did not fail');
      })
      .catch(error => {
        expect(lines.join('\n').replace(/\d+ms/, 'Xms')).to.equal(
          `vvvvvvvvvvvvvvvvvvvvvvvvvv
Requester rq2 sending query 1:
"failThis"
^^^^^^^^^^^^^^^^^^^^^^^^^^
vvvvvvvvvvvvvvvvvvvvvvvvvv
Requester rq2 got error in query 1: some error (in Xms)
^^^^^^^^^^^^^^^^^^^^^^^^^^`,
        );
      });
  });
});
