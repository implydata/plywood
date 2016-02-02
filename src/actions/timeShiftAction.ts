module Plywood {
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

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'TIME');
      return 'TIME';
    }

    protected _toStringParameters(expressionString: string): string[] {
      var ret = [this.duration.toString()];
      if (this.timezone) ret.push(this.timezone.toString());
      return ret;
    }

    public equals(other: TimeShiftAction): boolean {
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
        return duration.move(inV, timezone, step);
      }
    }

    protected _getJSHelper(inputJS: string): string {
      throw new Error("implement me");
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return dialect.timeShiftExpression(inputSQL, this.duration, this.timezone);
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
  }

  Action.register(TimeShiftAction);
}
