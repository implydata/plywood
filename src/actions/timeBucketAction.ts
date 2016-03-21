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
      this.duration = parameters.duration;
      this.timezone = parameters.timezone;
      this._ensureAction("timeBucket");
      if (!Duration.isDuration(this.duration)) {
        throw new Error("`duration` must be a Duration");
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
        Boolean(this.timezone) === Boolean(other.timezone) &&
        (!this.timezone || this.timezone.equals(other.timezone));
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
      var timezone = this.timezone;
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return null;
        timezone = timezone || (c['timezone'] ? Timezone.fromJS(<string>c['timezone']) : Timezone.UTC);
        return TimeRange.timeBucket(inV, duration, timezone);
      }
    }

    protected _getJSHelper(inputJS: string): string {
      throw new Error("implement me");
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return dialect.timeBucketExpression(inputSQL, this.duration, this.timezone);
    }
  }

  Action.register(TimeBucketAction);
}
