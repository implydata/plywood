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

const plywood = require('../plywood');
const { TimeBucketExpression } = plywood;

describe('TimeBucketExpression', () => {
  const timeBucketWithoutBounds = TimeBucketExpression.fromJS({
    duration: 'PT1H',
    step: -2,
    timezone: 'Etc/UTC',
  });

  describe('Accepts bounds', () => {
    it('Can create an expression with bounds', () => {
      const timeBucket = TimeBucketExpression.fromJS({
        duration: 'PT1H',
        timezone: 'Etc/UTC',
        bounds: '[]',
      });
      expect(timeBucket.toJS()).to.deep.equal({
        duration: 'PT1H',
        timezone: 'Etc/UTC',
        bounds: '[]',
        op: 'timeBucket',
      });
    });

    it('Can create an expression without bounds', () => {
      expect(timeBucketWithoutBounds.toJS()).to.deep.equal({
        duration: 'PT1H',
        timezone: 'Etc/UTC',
        op: 'timeBucket',
      });
    });

    it('Can change bounds', () => {
      expect(timeBucketWithoutBounds.changeBounds('(]').toJS()).to.deep.equal({
        duration: 'PT1H',
        timezone: 'Etc/UTC',
        op: 'timeBucket',
        bounds: '(]',
      });
    });

    it('preforms equals properly with bounds', () => {
      expect(timeBucketWithoutBounds.equals(timeBucketWithoutBounds.changeBounds('[)'))).to.equal(
        true,
      );
      expect(timeBucketWithoutBounds.equals(timeBucketWithoutBounds.changeBounds('[]'))).to.equal(
        false,
      );
      expect(timeBucketWithoutBounds.changeBounds('[)').equals(timeBucketWithoutBounds)).to.equal(
        true,
      );
    });
  });
});
