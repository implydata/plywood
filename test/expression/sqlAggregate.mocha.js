/*
 * Copyright 2020 Imply Data, Inc.
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
let { SqlAggregateExpression } = plywood;

describe('SqlAggregateExpression', () => {
  it('.substituteFilter', () => {
    expect(
      String(
        SqlAggregateExpression.substituteFilter(
          SqlExpression.parse(`SUM(t.revenue) / MIN(t.lol)`),
          SqlExpression.parse(`t.channel = 'en'`),
        ),
      ),
    ).to.equal(
      `SUM(CASE WHEN t.channel = 'en' THEN t.revenue END) / MIN(CASE WHEN t.channel = 'en' THEN t.lol END)`,
    );
  });
});
