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

import { PlywoodValue } from '../datatypes/index';
import { TimeRange } from '../datatypes/timeRange';
import { SQLDialect } from '../dialect/baseDialect';
import { pluralIfNeeded } from '../helper/utils';

import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
import { HasTimezone } from './mixins/hasTimezone';

export class TimeRangeExpression extends ChainableExpression implements HasTimezone {
  static DEFAULT_STEP = 1;

  static op = 'TimeRange';
  static fromJS(parameters: ExpressionJS): TimeRangeExpression {
    const value = ChainableExpression.jsToValue(parameters);
    value.duration = Duration.fromJS(parameters.duration);
    value.step = parameters.step;
    if (parameters.timezone) value.timezone = Timezone.fromJS(parameters.timezone);
    return new TimeRangeExpression(value);
  }

  public duration: Duration;
  public step: number;
  public timezone: Timezone;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this.duration = parameters.duration;
    this.step = parameters.step || TimeRangeExpression.DEFAULT_STEP;
    this.timezone = parameters.timezone;
    this._ensureOp('timeRange');
    this._checkOperandTypes('TIME');
    if (!(this.duration instanceof Duration)) {
      throw new Error('`duration` must be a Duration');
    }
    this.type = 'TIME_RANGE';
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.duration = this.duration;
    value.step = this.step;
    if (this.timezone) value.timezone = this.timezone;
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    js.duration = this.duration.toJS();
    js.step = this.step;
    if (this.timezone) js.timezone = this.timezone.toJS();
    return js;
  }

  public equals(other: TimeRangeExpression | undefined): boolean {
    return (
      super.equals(other) &&
      this.duration.equals(other.duration) &&
      this.step === other.step &&
      immutableEqual(this.timezone, other.timezone)
    );
  }

  protected _toStringParameters(_indent?: int): string[] {
    const ret = [this.duration.toString(), this.step.toString()];
    if (this.timezone) ret.push(Expression.safeString(this.timezone.toString()));
    return ret;
  }

  public getQualifiedDurationDescription(capitalize?: boolean) {
    const step = Math.abs(this.step);
    const durationDescription = this.duration.getDescription(capitalize);
    return step !== 1 ? pluralIfNeeded(step, durationDescription) : durationDescription;
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    const duration = this.duration;
    const step = this.step;
    const timezone = this.getTimezone();

    if (operandValue === null) return null;
    const other = duration.shift(operandValue, timezone, step);
    if (step > 0) {
      return new TimeRange({ start: operandValue, end: other });
    } else {
      return new TimeRange({ start: other, end: operandValue });
    }
  }

  protected _getSQLChainableHelper(_dialect: SQLDialect, _operandSQL: string): string {
    throw new Error('implement me');
  }

  // HasTimezone mixin:
  public getTimezone: () => Timezone;
  public changeTimezone: (timezone: Timezone) => TimeRangeExpression;
}

Expression.applyMixins(TimeRangeExpression, [HasTimezone]);
Expression.register(TimeRangeExpression);
