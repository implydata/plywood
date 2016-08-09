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
import { dummyObject } from '../helper/dummy';
import { Action, ActionJS, ActionValue } from './baseAction';
import { Expression, Indexer, Alterations } from '../expressions/baseExpression';
import { SQLDialect } from '../dialect/baseDialect';
import { Datum, ComputeFn } from '../datatypes/dataset';

export class TimeShiftAction extends Action {
  static DEFAULT_STEP = 1;

  static fromJS(parameters: ActionJS): TimeShiftAction {
    var value = Action.jsToValue(parameters);
    value.duration = Duration.fromJS(parameters.duration);
    value.step = parameters.step;
    if (parameters.timezone) value.timezone = Timezone.fromJS(parameters.timezone);
    return new TimeShiftAction(value);
  }

  public duration: Duration;
  public step: number;
  public timezone: Timezone;

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this.duration = parameters.duration;
    this.step = parameters.step || TimeShiftAction.DEFAULT_STEP;
    this.timezone = parameters.timezone;
    this._ensureAction("timeShift");
    if (!Duration.isDuration(this.duration)) {
      throw new Error("`duration` must be a Duration");
    }
  }

  public valueOf(): ActionValue {
    var value = super.valueOf();
    value.duration = this.duration;
    value.step = this.step;
    if (this.timezone) value.timezone = this.timezone;
    return value;
  }

  public toJS(): ActionJS {
    var js = super.toJS();
    js.duration = this.duration.toJS();
    js.step = this.step;
    if (this.timezone) js.timezone = this.timezone.toJS();
    return js;
  }

  public equals(other: TimeShiftAction): boolean {
    return super.equals(other) &&
      this.duration.equals(other.duration) &&
      this.step === other.step &&
      immutableEqual(this.timezone, other.timezone);
  }

  protected _toStringParameters(expressionString: string): string[] {
    var ret = [this.duration.toString(), this.step.toString()];
    if (this.timezone) ret.push(this.timezone.toString());
    return ret;
  }

  public getNecessaryInputTypes(): PlyType | PlyType[] {
    return 'TIME';
  }

  public getOutputType(inputType: PlyType): PlyType {
    this._checkInputTypes(inputType);
    return 'TIME';
  }

  public _fillRefSubstitutions(): FullType {
    return {
      type: 'TIME',
    };
  }

  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
    var duration = this.duration;
    var step = this.step;
    var timezone = this.getTimezone();
    return (d: Datum, c: Datum) => {
      var inV = inputFn(d, c);
      if (inV === null) return null;
      return duration.shift(inV, timezone, step);
    }
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string): string {
    throw new Error("implement me");
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return dialect.timeShiftExpression(inputSQL, this.duration, this.getTimezone());
  }

  protected _foldWithPrevAction(prevAction: Action): Action {
    if (prevAction instanceof TimeShiftAction) {
      if (
        this.duration.equals(prevAction.duration) &&
        Boolean(this.timezone) === Boolean(prevAction.timezone) &&
        (!this.timezone || this.timezone.equals(prevAction.timezone))
      ) {
        var value = this.valueOf();
        value.step += prevAction.step;
        return new TimeShiftAction(value);
      }
    }
    return null;
  }

  public needsEnvironment(): boolean {
    return !this.timezone;
  }

  public defineEnvironment(environment: Environment): Action {
    if (this.timezone || !environment.timezone) return this;
    var value = this.valueOf();
    value.timezone = environment.timezone;
    return new TimeShiftAction(value);
  }

  public getTimezone(): Timezone {
    return this.timezone || Timezone.UTC;
  }
}

Action.register(TimeShiftAction);
