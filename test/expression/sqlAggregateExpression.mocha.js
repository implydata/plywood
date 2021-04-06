/*
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
const { SqlExpression } = require('druid-query-toolkit');

let plywood = require('../plywood');
let { $, ply, r, Expression, SqlAggregateExpression } = plywood;

describe('SqlAggregateExpression', () => {
  describe('errors', () => {
    it('errors on non-parsable SQL', () => {
      expect(() => {
        Expression.fromJS({
          op: 'sqlAggregate',
          sql: 'SUM(A',
        });
      }).to.throw('Expected');
    });
  });

  describe('.substituteFilter', () => {
    it('works with simple function', () => {
      expect(
        String(
          SqlAggregateExpression.substituteFilter(
            SqlExpression.parse(`SUM(t."lol")`),
            SqlExpression.parse(`t."browser" = 'Chrome'`),
          ),
        ),
      ).to.equal(`SUM(CASE WHEN t."browser" = 'Chrome' THEN t."lol" END)`);
    });

    it('works with COUNT(*) function', () => {
      expect(
        String(
          SqlAggregateExpression.substituteFilter(
            SqlExpression.parse(`COUNT(*)`),
            SqlExpression.parse(`t."browser" = 'Chrome'`),
          ),
        ),
      ).to.equal(`COUNT(*) FILTER (WHERE t."browser" = 'Chrome')`);
    });

    it('works with filtered COUNT(*) function', () => {
      expect(
        String(
          SqlAggregateExpression.substituteFilter(
            SqlExpression.parse(`COUNT(*) FILTER (WHERE t."os" = 'Windows')`),
            SqlExpression.parse(`t."browser" = 'Chrome'`),
          ),
        ),
      ).to.equal(`COUNT(*) FILTER (WHERE t."os" = 'Windows' AND t."browser" = 'Chrome')`);
    });
  });
});
