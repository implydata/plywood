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

import * as moment from 'moment-timezone';
import { Timezone, Duration, parseISODate } from 'chronoshift';
import { Class, Instance } from 'immutable-class';
import { Range } from './range';
import { Expression } from '../expressions/baseExpression';

export interface TimeRangeValue {
  start: Date;
  end: Date;
  bounds?: string;
}

export interface TimeRangeJS {
  start: Date | string;
  end: Date | string;
  bounds?: string;
}

function toDate(date: any, name: string): Date | null {
  if (date === null) return null;
  if (typeof date === "undefined") throw new TypeError(`timeRange must have a ${name}`);
  if (typeof date === 'string' || typeof date === 'number') date = parseISODate(date as string, Expression.defaultParserTimezone);
  if (!date.getDay) throw new TypeError(`timeRange must have a ${name} that is a Date`);
  return date;
}

const START_OF_TIME = "1000";
const END_OF_TIME = "3000";

function dateToIntervalPart(date: Date): string {
  return date.toISOString()
    .replace('.000Z', 'Z')
    .replace(':00Z', 'Z')
    .replace(':00Z', 'Z'); // Do not do a final .replace('T00Z', 'Z');
}

let check: Class<TimeRangeValue, TimeRangeJS>;
export class TimeRange extends Range<Date> implements Instance<TimeRangeValue, TimeRangeJS> {
  static type = 'TIME_RANGE';

  static isTimeRange(candidate: any): candidate is TimeRange {
    return candidate instanceof TimeRange;
  }

  static intervalFromDate(date: Date): string {
    return dateToIntervalPart(date) + '/' + dateToIntervalPart(new Date(date.valueOf() + 1));
  }

  static timeBucket(date: Date, duration: Duration, timezone: Timezone): TimeRange {
    if (!date) return null;
    let start = duration.floor(date, timezone);
    return new TimeRange({
      start: start,
      end: duration.shift(start, timezone, 1),
      bounds: Range.DEFAULT_BOUNDS
    });
  }

  static fromTime(t: Date): TimeRange {
    return new TimeRange({ start: t, end: t, bounds: '[]' });
  }

  static fromJS(parameters: TimeRangeJS): TimeRange {
    if (typeof parameters !== "object") {
      throw new Error("unrecognizable timeRange");
    }
    return new TimeRange({
      start: toDate(parameters.start, 'start'),
      end: toDate(parameters.end, 'end'),
      bounds: parameters.bounds
    });
  }

  constructor(parameters: TimeRangeValue) {
    super(parameters.start, parameters.end, parameters.bounds);
  }

  protected _zeroEndpoint(): Date {
    return new Date(0);
  }

  protected _endpointEqual(a: Date, b: Date): boolean {
    if (a === null) {
      return b === null;
    } else {
      return b !== null && a.valueOf() === b.valueOf();
    }
  }

  protected _endpointToString(a: Date, tz: Timezone = Timezone.UTC): string {
    if (!a) return 'null';
    return moment.tz(a, tz.toString()).format();
  }

  public valueOf(): TimeRangeValue {
    return {
      start: this.start,
      end: this.end,
      bounds: this.bounds
    };
  }

  public toJS(): TimeRangeJS {
    let js: TimeRangeJS = {
      start: this.start,
      end: this.end
    };
    if (this.bounds !== Range.DEFAULT_BOUNDS) js.bounds = this.bounds;
    return js;
  }

  public toJSON(): TimeRangeJS {
    return this.toJS();
  }

  public equals(other: TimeRange): boolean {
    return other instanceof TimeRange && this._equalsHelper(other);
  }

  /**
   * Produces an inclusive exclusive [s, e) interval that represents this timeRange
   * If the timeRange itself is not inclusive/exclusive then a second is added to account for that
   *
   * @returns {string}
   */
  public toInterval(): string {
    let { start, end, bounds } = this;
    let interval: string[] = [START_OF_TIME, END_OF_TIME];
    if (start) {
      if (bounds[0] === '(') start = new Date(start.valueOf() + 1); // add a m.sec
      interval[0] = dateToIntervalPart(start);
    }
    if (end) {
      if (bounds[1] === ']') end = new Date(end.valueOf() + 1); // add a m.sec
      interval[1] = dateToIntervalPart(end);
    }
    return interval.join("/");
  }

  public midpoint(): Date {
    return new Date((this.start.valueOf() + this.end.valueOf()) / 2);
  }

  public isAligned(duration: Duration, timezone: Timezone): boolean {
    const { start, end } = this;
    return (!start || duration.isAligned(start, timezone)) && (!end || duration.isAligned(end, timezone));
  }
}
check = TimeRange;
Range.register(TimeRange);
