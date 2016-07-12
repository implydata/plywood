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
var { verboseRequesterFactory } = plywood.helper;

describe("Verbose requester", () => {
  var requester = (request) => {
    if (/^fail/.test(request.query)) {
      return Q.reject(new Error('some error'));
    } else {
      return Q([1, 2, 3]);
    }
  };

  it("works on success", (testComplete) => {
    var lines = [];
    var verboseRequester = verboseRequesterFactory({
      requester: requester,
      printLine(line) {
        return lines.push(line);
      }
    });

    return verboseRequester({ query: 'Query1' })
      .then((res) => {
        expect(res).to.be.an('array');
        expect(lines.join('\n').replace(/\d+ms/, 'Xms')).to.equal(
`vvvvvvvvvvvvvvvvvvvvvvvvvv
Sending query 1:
"Query1"
^^^^^^^^^^^^^^^^^^^^^^^^^^
vvvvvvvvvvvvvvvvvvvvvvvvvv
Got result from query 1: (in Xms)
[
  1,
  2,
  3
]
^^^^^^^^^^^^^^^^^^^^^^^^^^`);
        testComplete();
      })
      .done();
  });

  it("works on failure", (testComplete) => {
    var lines = [];
    var verboseRequester = verboseRequesterFactory({
      requester: requester,
      printLine(line) {
        return lines.push(line);
      }
    });

    return verboseRequester({ query: 'failThis' })
      .then(() => {
        throw new Error('did not fail');
      })
      .catch((error) => {
        expect(lines.join('\n').replace(/\d+ms/, 'Xms')).to.equal(
`vvvvvvvvvvvvvvvvvvvvvvvvvv
Sending query 1:
"failThis"
^^^^^^^^^^^^^^^^^^^^^^^^^^
vvvvvvvvvvvvvvvvvvvvvvvvvv
Got error in query 1: some error (in Xms)
^^^^^^^^^^^^^^^^^^^^^^^^^^`);
        testComplete();
      })
      .done();
  });
});
