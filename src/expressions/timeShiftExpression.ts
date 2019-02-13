/*
 * Copyright 2016-2019 Imply Data, Inc.
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
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
import { HasTimezone } from './mixins/hasTimezone';

export class TimeShiftExpression extends ChainableExpression implements HasTimezone {
  static DEFAULT_STEP = 1;

  static op = "TimeShift";
  static fromJS(parameters: ExpressionJS): TimeShiftExpression {
    let value = ChainableExpression.jsToValue(parameters);
    value.duration = Duration.fromJS(parameters.duration);
    value.step = parameters.step;
    if (parameters.timezone) value.timezone = Timezone.fromJS(parameters.timezone);
    return new TimeShiftExpression(value);
  }

  public duration: Duration;
  public step: number;
  public timezone: Timezone;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this.duration = parameters.duration;
    this.step = parameters.step != null ? parameters.step : TimeShiftExpression.DEFAULT_STEP;
    this.timezone = parameters.timezone;
    this._ensureOp("timeShift");
    this._checkOperandTypes('TIME');
    if (!Duration.isDuration(this.duration)) {
      throw new Error("`duration` must be a Duration");
    }
    this.type = 'TIME';
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

  public equals(other: TimeShiftExpression): boolean {
    return super.equals(other) &&
      this.duration.equals(other.duration) &&
      this.step === other.step &&
      immutableEqual(this.timezone, other.timezone);
  }

  protected _toStringParameters(indent?: int): string[] {
    let ret = [this.duration.toString(), this.step.toString()];
    if (this.timezone) ret.push(Expression.safeString(this.timezone.toString()));
    return ret;
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    return operandValue ? this.duration.shift(operandValue, this.getTimezone(), this.step) : null;
  }

  protected _getJSChainableHelper(operandJS: string): string {
    throw new Error("implement me");
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return dialect.timeShiftExpression(operandSQL, this.duration, this.getTimezone());
  }

  protected changeStep(step: int): Expression {
    if (this.step === step) return this;
    let value = this.valueOf();
    value.step = step;
    return new TimeShiftExpression(value);
  }

  public specialSimplify(): Expression {
    const { operand, duration, step, timezone } = this;

    // operand.timeShift(_, 0, _)
    if (step === 0) return operand;

    // X.timeShift(d, s, tz).timeShift(duration, step, timezone)
    if (operand instanceof TimeShiftExpression) {
      const { operand: x, duration: d, step: s, timezone: tz } = operand;
      if (duration.equals(d) && immutableEqual(timezone, tz)) {
        return x.timeShift(d, step + s, tz);
      }
    }

    return this;
  }

  // HasTimezone mixin:
  public getTimezone: () => Timezone;
  public changeTimezone: (timezone: Timezone) => TimeShiftExpression;
}

Expression.applyMixins(TimeShiftExpression, [HasTimezone]);
Expression.register(TimeShiftExpression);
