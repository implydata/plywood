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
const { SQLDialect } = require("../plywood");

const { $, ply, r, Expression } = plywood;

class TestingDialect extends SQLDialect {
  constructor() {
    super();
  }
}

describe('InExpression', () => {
  describe('_getSQLChainableUnaryHelper', () => {
    it('works with more than one column', () => {
      const inExpression = Expression._.in(['thing', 'otherThing', 'otherOtherThing']);
      const dialect = new TestingDialect()

      expect(inExpression._getSQLChainableUnaryHelper(dialect, 't."column"')).to.equal(`(t."column"='thing' OR t."column"='otherThing' OR t."column"='otherOtherThing')`);
    });

    it('works with single column', () => {
      const inExpression = Expression._.in(['thing']);
      const dialect = new TestingDialect()

      expect(inExpression._getSQLChainableUnaryHelper(dialect, 't."column"')).to.equal(`(t."column"='thing')`);
    });
  })
});
