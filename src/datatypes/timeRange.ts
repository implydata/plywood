module Plywood {
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

  function toDate(date: any, name: string): Date {
    if (date === null) return null;
    if (typeof date === "undefined") throw new TypeError(`timeRange must have a ${name}`);
    if (typeof date === 'string' || typeof date === 'number') date = new Date(date);
    if (!date.getDay) throw new TypeError(`timeRange must have a ${name} that is a Date`);
    return date;
  }

  const START_OF_TIME = "1000-01-01";
  const END_OF_TIME = "3000-01-01";

  function dateToIntervalPart(date: Date): string {
    return date.toISOString()
      .replace('Z', '')
      .replace('.000', '')
      .replace(/:00$/, '')
      .replace(/:00$/, '')
      .replace(/T00$/, '');
  }

  var check: Class<TimeRangeValue, TimeRangeJS>;
  export class TimeRange extends Range<Date> implements Instance<TimeRangeValue, TimeRangeJS> {
    static type = 'TIME_RANGE';

    static isTimeRange(candidate: any): boolean {
      return isInstanceOf(candidate, TimeRange);
    }

    static intervalFromDate(date: Date): string {
      return dateToIntervalPart(date) + '/' + dateToIntervalPart(new Date(date.valueOf() + 1));
    }

    static timeBucket(date: Date, duration: Duration, timezone: Timezone): TimeRange {
      if (!date) return null;
      var start = duration.floor(date, timezone);
      return new TimeRange({
        start: start,
        end: duration.move(start, timezone, 1),
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

    protected _endpointToString(a: Date): string {
      if (!a) return 'null';
      return a.toISOString();
    }

    public valueOf(): TimeRangeValue {
      return {
        start: this.start,
        end: this.end,
        bounds: this.bounds
      };
    }

    public toJS(): TimeRangeJS {
      var js: TimeRangeJS = {
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
      return TimeRange.isTimeRange(other) && this._equalsHelper(other);
    }

    /**
     * Produces an inclusive exclusive [s, e) interval that represents this timeRange
     * If the timeRange itself is not inclusive/exclusive then a second is added to account for that
     *
     * @returns {string}
     */
    public toInterval(): string {
      var { start, end, bounds } = this;
      var interval: string[] = [START_OF_TIME, END_OF_TIME];
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
  }
  check = TimeRange;
}
