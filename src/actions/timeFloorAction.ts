module Plywood {
  export class TimeFloorAction extends Action {
    static fromJS(parameters: ActionJS): TimeFloorAction {
      var value = Action.jsToValue(parameters);
      value.duration = Duration.fromJS(parameters.duration);
      if (parameters.timezone) value.timezone = Timezone.fromJS(parameters.timezone);
      return new TimeFloorAction(value);
    }

    public duration: Duration;
    public timezone: Timezone;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.duration = parameters.duration;
      this.timezone = parameters.timezone;
      this._ensureAction("timeFloor");
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

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'TIME');
      return 'TIME';
    }

    protected _toStringParameters(expressionString: string): string[] {
      var ret = [this.duration.toString()];
      if (this.timezone) ret.push(this.timezone.toString());
      return ret;
    }

    public equals(other: TimeBucketAction): boolean {
      return super.equals(other) &&
        this.duration.equals(other.duration) &&
        Boolean(this.timezone) === Boolean(other.timezone) &&
        (!this.timezone || this.timezone.equals(other.timezone));
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      var duration = this.duration;
      var timezone = this.timezone;
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return null;
        timezone = timezone || (c['timezone'] ? Timezone.fromJS(c['timezone']) : Timezone.UTC);
        return duration.floor(inV, timezone);
      }
    }

    protected _getJSHelper(inputJS: string): string {
      throw new Error("implement me");
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return dialect.timeFloorExpression(inputSQL, this.duration, this.timezone);
    }

    protected _foldWithPrevAction(prevAction: Action): Action {
      if (prevAction.equals(this)) {
        return this;
      }
      return null;
    }
  }

  Action.register(TimeFloorAction);
}
