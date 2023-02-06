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

const { Expression } = plywood;

describe('MvOverlapExpression', () => {
  describe('_calcChainableHelper', () => {
    it('works with single string', () => {
      const mvOverlapExpression = Expression._.mvOverlap([
        'thing',
        'otherThing',
        'otherOtherThing',
      ]);

      expect(mvOverlapExpression._calcChainableHelper('thing')).to.equal(true);
      expect(mvOverlapExpression._calcChainableHelper('not a thing')).to.equal(false);
    });

    it('works with array of strings', () => {
      const mvOverlapExpression = Expression._.mvOverlap([
        'thing',
        'otherThing',
        'otherOtherThing',
      ]);

      expect(mvOverlapExpression._calcChainableHelper(['thing', 'otherThing'])).to.equal(true);
      expect(mvOverlapExpression._calcChainableHelper(['not a thing'])).to.equal(false);
      expect(mvOverlapExpression._calcChainableHelper(['thing', 'not a thing'])).to.equal(true);
    });
  });
});
