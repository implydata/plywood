/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
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

import { Timezone, Duration } from 'chronoshift';
import { Action, ActionJS, ActionValue, Environment } from './baseAction';
import { PlyType, DatasetFullType, PlyTypeSingleValue, FullType } from '../types';
import { SQLDialect } from '../dialect/baseDialect';
import { Datum, ComputeFn } from '../datatypes/dataset';
import { TimeRange } from '../datatypes/timeRange';
import { immutableEqual } from 'immutable-class';

export class TimeBucketAction extends Action {
  static fromJS(parameters: ActionJS): TimeBucketAction {
    var value = Action.jsToValue(parameters);
    value.duration = Duration.fromJS(parameters.duration);
    if (parameters.timezone) value.timezone = Timezone.fromJS(parameters.timezone);
    return new TimeBucketAction(value);
  }

  public duration: Duration;
  public timezone: Timezone;

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    var duration = parameters.duration;
    this.duration = duration;
    this.timezone = parameters.timezone;
    this._ensureAction("timeBucket");
    if (!Duration.isDuration(duration)) {
      throw new Error("`duration` must be a Duration");
    }
    if (!duration.isFloorable()) {
      throw new Error(`duration '${duration.toString()}' is not floorable`);
    }
  }

  public valueOf(): ActionValue {
    var value = super.valueOf();
    value.duration = this.duration;
    if (this.timezone) value.timezone = this.timezone;
    return value;
  }

  public toJS(): ActionJS {
    var js = super.toJS();
    js.duration = this.duration.toJS();
    if (this.timezone) js.timezone = this.timezone.toJS();
    return js;
  }

  public equals(other: TimeBucketAction): boolean {
    return super.equals(other) &&
      this.duration.equals(other.duration) &&
      immutableEqual(this.timezone, other.timezone);
  }

  protected _toStringParameters(expressionString: string): string[] {
    var ret = [this.duration.toString()];
    if (this.timezone) ret.push(this.timezone.toString());
    return ret;
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return ['TIME' as PlyType, 'TIME_RANGE' as PlyType];
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'TIME_RANGE';
  }

  public _fillRefSubstitutions(): FullType {
    return {
      type: 'TIME_RANGE'
    };
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
    var duration = this.duration;
    var timezone = this.getTimezone();
    return (d: Datum, c: Datum) => {
      var inV = inputFn(d, c);
      if (inV === null) return null;
      return TimeRange.timeBucket(inV, duration, timezone);
    };
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string): string {
    throw new Error("implement me");
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return dialect.timeBucketExpression(inputSQL, this.duration, this.getTimezone());
  }

  public needsEnvironment(): boolean {
    return !this.timezone;
  }

  public defineEnvironment(environment: Environment): Action {
    if (this.timezone || !environment.timezone) return this;
    var value = this.valueOf();
    value.timezone = environment.timezone;
    return new TimeBucketAction(value);
  }

  public getTimezone(): Timezone {
    return this.timezone || Timezone.UTC;
  }
}

Action.register(TimeBucketAction);
