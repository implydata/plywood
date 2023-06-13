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

import { Duration, Timezone } from 'chronoshift';
import { immutableEqual } from 'immutable-class';

import { PlywoodValue } from '../datatypes';
import { TimeRange } from '../datatypes/timeRange';
import { SQLDialect } from '../dialect/baseDialect';

import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
import { HasTimezone } from './mixins/hasTimezone';

export class TimeBucketExpression extends ChainableExpression {
  static op = 'TimeBucket';
  static fromJS(parameters: ExpressionJS): TimeBucketExpression {
    const value = ChainableExpression.jsToValue(parameters);
    value.duration = Duration.fromJS(parameters.duration);
    if (parameters.timezone) value.timezone = Timezone.fromJS(parameters.timezone);
    return new TimeBucketExpression(value);
  }

  public duration: Duration;
  public timezone: Timezone;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    const duration = parameters.duration;
    this.duration = duration;
    this.timezone = parameters.timezone;
    this._ensureOp('timeBucket');
    this._checkOperandTypes('TIME');
    if (!(duration instanceof Duration)) {
      throw new Error('`duration` must be a Duration');
    }
    if (!duration.isFloorable()) {
      throw new Error(`duration '${duration.toString()}' is not floorable`);
    }
    this.type = 'TIME_RANGE';
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.duration = this.duration;
    if (this.timezone) value.timezone = this.timezone;
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    js.duration = this.duration.toJS();
    if (this.timezone) js.timezone = this.timezone.toJS();
    return js;
  }

  public equals(other: TimeBucketExpression | undefined): boolean {
    return (
      super.equals(other) &&
      this.duration.equals(other.duration) &&
      immutableEqual(this.timezone, other.timezone)
    );
  }

  protected _toStringParameters(_indent?: int): string[] {
    const ret = [this.duration.toString()];
    if (this.timezone) ret.push(Expression.safeString(this.timezone.toString()));
    return ret;
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    return operandValue
      ? TimeRange.timeBucket(operandValue, this.duration, this.getTimezone())
      : null;
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return dialect.timeBucketExpression(operandSQL, this.duration, this.getTimezone());
  }

  // HasTimezone mixin:
  public getTimezone: () => Timezone;
  public changeTimezone: (timezone: Timezone) => this;
}

Expression.applyMixins(TimeBucketExpression, [HasTimezone]);
Expression.register(TimeBucketExpression);
