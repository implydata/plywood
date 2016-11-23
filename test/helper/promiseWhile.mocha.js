/*
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
let { promiseWhile } = require("../../build/plywood");

describe('Promise While', () => {

  it('should loop three times asynchronously', (testComplete) => {
    let res = [];
    let i = 0;
    promiseWhile(
      function () {
        return i < 3
      },
      function () {
        return new Promise(function (resolve, reject) {
          setTimeout(function () {
            res.push('aye' + i);
            resolve(i++);
          }, 10)
        })
      }
    )
      .then(function () {
        expect(res).to.deep.equal(['aye0', 'aye1', 'aye2']);
        testComplete();
      })
      .done();
  });

  it('should propagate rejection', (testComplete) => {
    function TestError() {}

    promiseWhile(
      function () {
        return true;
      },
      function () {
        return Q.reject(new TestError('test'));
      }
    )
      .then(() => {
        throw new Error('did not error');
      })
      .catch(function (err) {
        expect(err).be.instanceof(TestError);
        testComplete();
      })
      .done();
  });

  it('should propagate conditions throw', (testComplete) => {
    function TestError() {}

    promiseWhile(
      function () {
        throw new TestError('test');
      },
      function () {
        return Q();
      }
    )
      .then(() => {
        throw new Error('did not error');
      })
      .catch(function (err) {
        expect(err).be.instanceof(TestError);
        testComplete();
      })
      .done();
  });

  it('should propagate action throw', (testComplete) => {
    function TestError() {}

    promiseWhile(
      function () {
        return true;
      },
      function () {
        throw new TestError('test');
      }
    )
      .then(() => {
        throw new Error('did not error');
      })
      .catch(function (err) {
        expect(err).be.instanceof(TestError);
        testComplete();
      })
      .done();
  });

});
