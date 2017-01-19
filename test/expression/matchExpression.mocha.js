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

let plywood = require('../plywood');
let { $, ply, r, MatchExpression } = plywood;

describe("MatchExpression", () => {
  it(".likeToRegExp", () => {
    expect(MatchExpression.likeToRegExp('%David\\_R_ss%')).to.equal('^.*David_R.ss.*$');

    expect(MatchExpression.likeToRegExp('%David|_R_ss||%', '|')).to.equal('^.*David_R.ss\\|.*$');
  });
});
