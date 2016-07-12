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

var plywood = require("../../build/plywood");
var { simpleLocator } = plywood.helper;

describe('Simple locator', () => {
  describe('shortcut function', () => {
    var locator = simpleLocator("localhost:8080");

    it("works", (testComplete) => {
      return locator()
        .then((location) => {
          expect(location).to.deep.equal({
            hostname: 'localhost',
            port: 8080
          });
          testComplete();
        })
        .done();
    });
  });

  describe('full option function', () => {
    var locator = simpleLocator({
      resource: "localhost;koalastothemax.com:80",
      defaultPort: 8181
    });

    it("works", (testComplete) => {
      return locator()
        .then((location) => {
          for (var i = 1; i <= 20; i++) {
            if (location.hostname === 'localhost') {
              expect(location).to.deep.equal({
                hostname: 'localhost',
                port: 8181
              });
            } else {
              expect(location).to.deep.equal({
                hostname: 'koalastothemax.com',
                port: 80
              });
            }
          }
          testComplete();
        })
        .done();
    });
  });
});
