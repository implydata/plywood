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

const { s$, containsSqlFunction } = require('../../build/plywood');

describe('containsSqlFunction', () => {
  it('should return false if function is not a part of the SqlRefExpression', () => {
    expect(containsSqlFunction(s$(`IP_SEARCH('192', "t"."net_dst")`, 'IP'), ['ip_match'])).to.equal(
      false,
    );
  });

  it('should return true if function is a part of the SqlRefExpression', () => {
    expect(
      containsSqlFunction(s$(`IP_SEARCH('192', "t"."net_dst")`, 'IP'), ['ip_search']),
    ).to.equal(true);
  });

  it('should return false if column name is the same as function name', () => {
    expect(containsSqlFunction(s$(`"t"."ip_search"`, 'IP'), ['ip_search'])).to.equal(false);
  });

  it('should be case insensitive', () => {
    expect(
      containsSqlFunction(s$(`ip_search('192', "t"."net_dst")`, 'IP'), ['IP_SEARCH']),
    ).to.equal(true);
  });
});
