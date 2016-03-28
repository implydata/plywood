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

    public equals(other: TimeRangeAction): boolean {
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

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'TIME');
      return 'TIME_RANGE';
    }

    public _fillRefSubstitutions(): FullType {
      return {
        type: 'TIME_RANGE',
      };
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      var duration = this.duration;
      var step = this.step;
      var timezone = this.getTimezone();
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return null;
        var other = duration.shift(inV, timezone, step);
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

    public needsEnvironment(): boolean {
      return !this.timezone;
    }

    public defineEnvironment(environment: Environment): Action {
      if (this.timezone || !environment.timezone) return this;
      var value = this.valueOf();
      value.timezone = environment.timezone;
      return new TimeRangeAction(value);
    }

    public getTimezone(): Timezone {
      return this.timezone || Timezone.UTC;
    }
  }

  Action.register(TimeRangeAction);
}
