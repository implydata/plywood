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

const plywood = require('../plywood');

const { SqlRefExpression, s$ } = plywood;

describe('SqlRefExpression', () => {
  it('.toString should work with type', () => {
    const sqlRefExpression = SqlRefExpression.fromJS({
      op: 'sqlRef',
      sql: `t.\"ip\"`,
      type: 'IP',
    });

    expect(sqlRefExpression.toString()).to.equal('s${t."ip"}:IP');
  });

  it('.toString should work without type', () => {
    const sqlRefExpression = SqlRefExpression.fromJS({
      op: 'sqlRef',
      sql: `t.\"ip\"`,
    });

    expect(sqlRefExpression.toString()).to.equal('s${t."ip"}');
  });

  describe('#isSqlFunction', () => {
    it('should return false if function is not a part of the SqlRefExpression', () => {
      expect(s$(`IP_SEARCH('192', "t"."net_dst")`, 'IP').isSqlFunction('ip_match')).to.equal(false);
    });

    it('should return true if function is a part of the SqlRefExpression', () => {
      expect(s$(`IP_SEARCH('192', "t"."net_dst")`, 'IP').isSqlFunction('ip_search')).to.equal(true);
    });

    it('should return false if column name is the same as function name', () => {
      expect(s$(`"t"."ip_search"`, 'IP').isSqlFunction('ip_search')).to.equal(false);
    });

    it('should be case insensitive', () => {
      expect(s$(`ip_search('192', "t"."net_dst")`, 'IP').isSqlFunction('IP_SEARCH')).to.equal(true);
    });
  });
});
