module Plywood {
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

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'TIME', 'TIME_RANGE');
      return 'TIME_RANGE';
    }

    public _fillRefSubstitutions(): FullType {
      return {
        type: 'TIME_RANGE',
      };
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      var duration = this.duration;
      var timezone = this.getTimezone();
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return null;
        return TimeRange.timeBucket(inV, duration, timezone);
      }
    }

    protected _getJSHelper(inputJS: string): string {
      throw new Error("implement me");
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
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
}
