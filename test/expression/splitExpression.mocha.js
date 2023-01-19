/*
 * Copyright 2016-2020 Imply Data, Inc.
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

const { s$, $, ply, r, Expression, DruidDialect } = plywood;

const dialect = new DruidDialect();

describe('SplitExpression', () => {
  describe('#maxBucketNumber', () => {
    it('works with boolean ref case', () => {
      const splitExpression = Expression._.split({
        bool: $('bool', 'BOOLEAN'),
      });

      expect(splitExpression.maxBucketNumber()).to.equal(3);
    });

    it('works with boolean expression case', () => {
      const splitExpression = Expression._.split({
        isBlah: $('x').is('blah'),
      });

      expect(splitExpression.maxBucketNumber()).to.equal(3);
    });

    it('works in multi-split case', () => {
      const splitExpression = Expression._.split({
        timePart: $('time').timePart('HOUR_OF_DAY'),
        isBlah: $('x').is('blah'),
      });

      expect(splitExpression.maxBucketNumber()).to.equal(81);
    });

    it('works in unknown', () => {
      const splitExpression = Expression._.split({
        isBlah: $('x'),
      });

      expect(splitExpression.maxBucketNumber()).to.equal(Infinity);
    });
  });

  describe('getSelectSql', () => {
    it('should not add IP_STRINGIFY to IP_MATCH expression', () => {
      const splitExpression = Expression._.split({
        ip: s$(`IP_MATCH('192', "t"."net_dst")`, 'IP'),
      });

      expect(splitExpression.getSelectSQL(dialect)[0]).to.equal(
        `(IP_MATCH('192', "t"."net_dst")) AS "ip"`,
      );
    });

    it('should not add IP_STRINGIFY to IP_SEARCH expression', () => {
      const splitExpression = Expression._.split({
        ip: s$(`IP_SEARCH('192', "t"."net_dst")`, 'IP'),
      });

      expect(splitExpression.getSelectSQL(dialect)[0]).to.equal(
        `(IP_SEARCH('192', "t"."net_dst")) AS "ip"`,
      );
    });

    it('should add IP_STRINGIFY to ip expression without functions', () => {
      const splitExpression = Expression._.split({
        ip: s$(`"t"."net_dst"`, 'IP'),
      });

      expect(splitExpression.getSelectSQL(dialect)[0]).to.equal(
        `IP_STRINGIFY(("t"."net_dst")) AS "ip"`,
      );
    });
  });
});
