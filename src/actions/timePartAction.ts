module Plywood {
  interface Parter {
    (d: Date): number;
  }

  const PART_TO_FUNCTION: Lookup<Parter> = {
    SECOND_OF_MINUTE: d => d.getSeconds(),
    SECOND_OF_HOUR: d => d.getMinutes() * 60 + d.getSeconds(),
    SECOND_OF_DAY: d => (d.getHours() * 60 + d.getMinutes()) * 60 + d.getSeconds(),
    SECOND_OF_WEEK: d => ((d.getDay() * 24) + d.getHours() * 60 + d.getMinutes()) * 60 + d.getSeconds(),
    SECOND_OF_MONTH: d => (((d.getDate() - 1) * 24) + d.getHours() * 60 + d.getMinutes()) * 60 + d.getSeconds(),
    SECOND_OF_YEAR: null,

    MINUTE_OF_HOUR: d => d.getMinutes(),
    MINUTE_OF_DAY: d => d.getHours() * 60 + d.getMinutes(),
    MINUTE_OF_WEEK: d => (d.getDay() * 24) + d.getHours() * 60 + d.getMinutes(),
    MINUTE_OF_MONTH: d => ((d.getDate() - 1) * 24) + d.getHours() * 60 + d.getMinutes(),
    MINUTE_OF_YEAR: null,

    HOUR_OF_DAY: d => d.getHours(),
    HOUR_OF_WEEK: d => d.getDay() * 24 + d.getHours(),
    HOUR_OF_MONTH: d => (d.getDate() - 1) * 24 + d.getHours(),
    HOUR_OF_YEAR: null,

    DAY_OF_WEEK: d => d.getDay(),
    DAY_OF_MONTH: d => d.getDate() - 1,
    DAY_OF_YEAR: null,

    WEEK_OF_MONTH: null,
    WEEK_OF_YEAR: null,

    MONTH_OF_YEAR: d => d.getMonth()
  };

  const PART_TO_MAX_VALUES: Lookup<number> = {
    SECOND_OF_MINUTE: 61, // Leap seconds
    SECOND_OF_HOUR: 3601,
    SECOND_OF_DAY: 93601,
    SECOND_OF_WEEK: null,
    SECOND_OF_MONTH: null,
    SECOND_OF_YEAR: null,

    MINUTE_OF_HOUR: 60,
    MINUTE_OF_DAY: 26 * 60,
    MINUTE_OF_WEEK: null,
    MINUTE_OF_MONTH: null,
    MINUTE_OF_YEAR: null,

    HOUR_OF_DAY: 26, // Timezones
    HOUR_OF_WEEK: null,
    HOUR_OF_MONTH: null,
    HOUR_OF_YEAR: null,

    DAY_OF_WEEK: 7,
    DAY_OF_MONTH: 31,
    DAY_OF_YEAR: 366,

    WEEK_OF_MONTH: 5,
    WEEK_OF_YEAR: 53,

    MONTH_OF_YEAR: 12
  };

  export class TimePartAction extends Action {
    static fromJS(parameters: ActionJS): TimePartAction {
      var value = Action.jsToValue(parameters);
      value.part = parameters.part;
      if (parameters.timezone) value.timezone = Timezone.fromJS(parameters.timezone);
      return new TimePartAction(value);
    }

    public part: string;
    public timezone: Timezone;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.part = parameters.part;
      this.timezone = parameters.timezone;
      this._ensureAction("timePart");
      if (typeof this.part !== 'string') {
        throw new Error("`part` must be a string");
      }
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.part = this.part;
      if (this.timezone) value.timezone = this.timezone;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.part = this.part;
      if (this.timezone) js.timezone = this.timezone.toJS();
      return js;
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'TIME');
      return 'NUMBER';
    }

    protected _toStringParameters(expressionString: string): string[] {
      var ret = [this.part];
      if (this.timezone) ret.push(this.timezone.toString());
      return ret;
    }

    public equals(other: TimePartAction): boolean {
      return super.equals(other) &&
        this.part === other.part &&
        Boolean(this.timezone) === Boolean(other.timezone) &&
        (!this.timezone || this.timezone.equals(other.timezone));
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      const { part, timezone } = this;
      var parter = PART_TO_FUNCTION[part];
      if (!parter) throw new Error(`unsupported part '${part}'`);
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (!inV) return null;
        inV = WallTime.UTCToWallTime(inV, timezone.toString());
        return parter(inV);
      }
    }

    protected _getJSHelper(inputJS: string): string {
      throw new Error("implement me");
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      const { part, timezone } = this;
      return dialect.timePartExpression(inputSQL, part, timezone);
    }

    public materializeWithinRange(extentRange: TimeRange, values: int[]): Set {
      var partUnits = this.part.toLowerCase().split('_of_');
      var unitSmall = partUnits[0];
      var unitBig = partUnits[1];
      var timezone = this.timezone;
      var smallTimeMover = <Chronoshift.TimeMover>(<any>Chronoshift)[unitSmall];
      var bigTimeMover = <Chronoshift.TimeMover>(<any>Chronoshift)[unitBig];

      var start = extentRange.start;
      var end = extentRange.end;

      var ranges: TimeRange[] = [];
      var iter = bigTimeMover.floor(start, timezone);
      while (iter <= end) {
        for (let value of values) {
          let subIter = smallTimeMover.move(iter, timezone, value);
          ranges.push(new TimeRange({
            start: subIter,
            end: smallTimeMover.move(subIter, timezone, 1)
          }));
        }
        iter = bigTimeMover.move(iter, timezone, 1);
      }

      return Set.fromJS({
        setType: 'TIME_RANGE',
        elements: ranges
      })
    }

    public maxPossibleSplitValues(): number {
      var maxValue = PART_TO_MAX_VALUES[this.part];
      if (!maxValue) return Infinity;
      return maxValue + 1; // +1 for null
    }
  }

  Action.register(TimePartAction);
}
