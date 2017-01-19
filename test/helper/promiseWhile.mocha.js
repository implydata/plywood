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

let Promise = require('any-promise');
let { promiseWhile } = require("../../build/plywood");

describe('Promise While', () => {
  it('should loop three times asynchronously', () => {
    let res = [];
    let i = 0;

    return promiseWhile(
      function () {
        return i < 3
      },
      function () {
        return new Promise(function (resolve) {
          setTimeout(function () {
            res.push('aye' + i);
            resolve(i++);
          }, 10)
        })
      }
    )
      .then(function () {
        expect(res).to.deep.equal(['aye0', 'aye1', 'aye2']);
      });
  });

  it('should propagate rejection', () => {
    function TestError() {}

    promiseWhile(
      function () {
        return true;
      },
      function () {
        return Promise.reject(new TestError('test'));
      }
    )
      .then(() => {
        throw new Error('did not error');
      })
      .catch(function (err) {
        expect(err).be.instanceof(TestError);
      });
  });

  it('should propagate conditions throw', () => {
    function TestError() {}

    promiseWhile(
      function () {
        throw new TestError('test');
      },
      function () {
        return Promise.resolve();
      }
    )
      .then(() => {
        throw new Error('did not error');
      })
      .catch(function (err) {
        expect(err).be.instanceof(TestError);
      });
  });

  it('should propagate action throw', () => {
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
      });
  });

});
