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
const { SQLDialect } = require('../plywood');

const { $, ply, r, Expression, MvOverlapExpression, Set } = plywood;

class TestingDialect extends SQLDialect {
  constructor() {
    super();
  }
}

describe('MvOverlapExpression', () => {

  describe('_getSQLChainableUnaryHelper', () => {
    it('works', () => {
      const mvOverlapExpression = Expression._.mvOverlap([
        'thing',
        'otherThing',
        'otherOtherThing',
      ]);
      const dialect = new TestingDialect();

      expect(mvOverlapExpression._getSQLChainableUnaryHelper(dialect, ['thing'])).to.equal(
        `MV_OVERLAP(['thing', 'otherThing', 'otherOtherThing'], ['thing'])`,
      );
    });
  });

  describe('_calcChainableHelper', () => {
    it('works with single string', () => {
      const mvOverlapExpression = Expression._.mvOverlap([
        'thing',
        'otherThing',
        'otherOtherThing',
      ]);

      expect(mvOverlapExpression._getSQLChainableUnaryHelper('thing')).to.equal(true);
      expect(mvOverlapExpression._getSQLChainableUnaryHelper('not a thing')).to.equal(false);
    });

    it('works with array of strings', () => {
      const mvOverlapExpression = Expression._.mvOverlap([
        'thing',
        'otherThing',
        'otherOtherThing',
      ]);

      expect(mvOverlapExpression._getSQLChainableUnaryHelper(['thing', 'otherThing'])).to.equal(
        true,
      );
      expect(mvOverlapExpression._getSQLChainableUnaryHelper(['not a thing'])).to.equal(false);
    });
  });
});
