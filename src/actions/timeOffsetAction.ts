module Plywood {
  export class TimeOffsetAction extends Action {
    static fromJS(parameters: ActionJS): TimeOffsetAction {
      var value = Action.jsToValue(parameters);
      value.duration = Duration.fromJS(parameters.duration);
      value.timezone = Timezone.fromJS(parameters.timezone);
      return new TimeOffsetAction(value);
    }

    public duration: Duration;
    public timezone: Timezone;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.duration = parameters.duration;
      this.timezone = parameters.timezone;
      this._ensureAction("timeOffset");
      if (!Duration.isDuration(this.duration)) {
        throw new Error("`duration` must be a Duration");
      }
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.duration = this.duration;
      value.timezone = this.timezone;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.duration = this.duration.toJS();
      js.timezone = this.timezone.toJS();
      return js;
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'TIME');
      return 'TIME';
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [expressionString, this.duration.toString(), this.timezone.toString()];
    }

    public equals(other: TimeBucketAction): boolean {
      return super.equals(other) &&
        this.duration.equals(other.duration) &&
        this.timezone.equals(other.timezone);
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      var duration = this.duration;
      var timezone = this.timezone;
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return null;
        return duration.move(inV, timezone, 1); // ToDo: generalize direction
      }
    }

    protected _getJSHelper(inputJS: string): string {
      throw new Error("implement me");
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return dialect.offsetTimeExpression(inputSQL, this.duration);
    }
  }

  Action.register(TimeOffsetAction);
}
