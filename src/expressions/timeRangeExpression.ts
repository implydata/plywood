/*
 * Copyright 2016-2017 Imply Data, Inc.
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

import { immutableEqual } from 'immutable-class';
import { Timezone, Duration } from 'chronoshift';
import { r, ExpressionJS, ExpressionValue, Expression, ChainableExpression } from './baseExpression';
import { HasTimezone } from './mixins/hasTimezone';
import { SQLDialect } from '../dialect/baseDialect';
import { PlywoodValue, Set } from '../datatypes/index';
import { TimeRange } from '../datatypes/timeRange';
import { pluralIfNeeded } from "../helper/utils";

export class TimeRangeExpression extends ChainableExpression implements HasTimezone {
  static DEFAULT_STEP = 1;

  static op = "TimeRange";
  static fromJS(parameters: ExpressionJS): TimeRangeExpression {
    let value = ChainableExpression.jsToValue(parameters);
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
    this._ensureOp("timeRange");
    this._checkOperandTypes('TIME');
    if (!Duration.isDuration(this.duration)) {
      throw new Error("`duration` must be a Duration");
    }
    this.type = 'TIME_RANGE';
  }

  public valueOf(): ExpressionValue {
    let value = super.valueOf();
    value.duration = this.duration;
    value.step = this.step;
    if (this.timezone) value.timezone = this.timezone;
    return value;
  }

  public toJS(): ExpressionJS {
    let js = super.toJS();
    js.duration = this.duration.toJS();
    js.step = this.step;
    if (this.timezone) js.timezone = this.timezone.toJS();
    return js;
  }

  public equals(other: TimeRangeExpression): boolean {
    return super.equals(other) &&
      this.duration.equals(other.duration) &&
      this.step === other.step &&
      immutableEqual(this.timezone, other.timezone);
  }

  protected _toStringParameters(indent?: int): string[] {
    let ret = [this.duration.toString(), this.step.toString()];
    if (this.timezone) ret.push(this.timezone.toString());
    return ret;
  }

  public getQualifiedDurationDescription() {
    const step = Math.abs(this.step);
    const durationDescription = this.duration.getDescription(true);
    return step !== 1 ? pluralIfNeeded(step, durationDescription) : durationDescription;
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    let duration = this.duration;
    let step = this.step;
    let timezone = this.getTimezone();

    if (operandValue === null) return null;
    let other = duration.shift(operandValue, timezone, step);
    if (step > 0) {
      return new TimeRange({ start: operandValue, end: other });
    } else {
      return new TimeRange({ start: other, end: operandValue });
    }
  }

  protected _getJSChainableHelper(operandJS: string): string {
    throw new Error("implement me");
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    throw new Error("implement me");
  }

  // HasTimezone mixin:
  public getTimezone: () => Timezone;
  public changeTimezone: (timezone: Timezone) => TimeRangeExpression;
}

Expression.applyMixins(TimeRangeExpression, [HasTimezone]);
Expression.register(TimeRangeExpression);
