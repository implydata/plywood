module Plywood {
  export class TimeRangeAction extends Action {
    static DEFAULT_STEP = 1;

    static fromJS(parameters: ActionJS): TimeRangeAction {
      var value = Action.jsToValue(parameters);
      value.duration = Duration.fromJS(parameters.duration);
      value.step = parameters.step;
      if (parameters.timezone) value.timezone = Timezone.fromJS(parameters.timezone);
      return new TimeRangeAction(value);
    }

    public duration: Duration;
    public step: number;
    public timezone: Timezone;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.duration = parameters.duration;
      this.step = parameters.step || TimeRangeAction.DEFAULT_STEP;
      this.timezone = parameters.timezone;
      this._ensureAction("timeRange");
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

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'TIME');
      return 'TIME_RANGE';
    }

    protected _toStringParameters(expressionString: string): string[] {
      var ret = [this.duration.toString(), this.step.toString()];
      if (this.timezone) ret.push(this.timezone.toString());
      return ret;
    }

    public equals(other: TimeRangeAction): boolean {
      return super.equals(other) &&
        this.duration.equals(other.duration) &&
        this.step === other.step &&
        Boolean(this.timezone) === Boolean(other.timezone) &&
        (!this.timezone || this.timezone.equals(other.timezone));
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      var duration = this.duration;
      var step = this.step;
      var timezone = this.timezone;
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return null;
        timezone = timezone || (c['timezone'] ? Timezone.fromJS(c['timezone']) : Timezone.UTC);
        var other = duration.move(inV, timezone, step);
        if (step > 0) {
          return new TimeRange({ start: inV, end: other });
        } else {
          return new TimeRange({ start: other, end: inV });
        }
      }
    }

    protected _getJSHelper(inputJS: string): string {
      throw new Error("implement me");
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      throw new Error("implement me");
    }
  }

  Action.register(TimeRangeAction);
}
