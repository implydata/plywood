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

    public equals(other: TimeShiftAction): boolean {
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
      return 'TIME';
    }

    public _fillRefSubstitutions(): FullType {
      return {
        type: 'TIME',
      };
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      var duration = this.duration;
      var step = this.step;
      var timezone = this.getTimezone();
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return null;
        return duration.shift(inV, timezone, step);
      }
    }

    protected _getJSHelper(inputJS: string): string {
      throw new Error("implement me");
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return dialect.timeShiftExpression(inputSQL, this.duration, this.getTimezone());
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

    public needsEnvironment(): boolean {
      return !this.timezone;
    }

    public defineEnvironment(environment: Environment): Action {
      if (this.timezone || !environment.timezone) return this;
      var value = this.valueOf();
      value.timezone = environment.timezone;
      return new TimeShiftAction(value);
    }

    public getTimezone(): Timezone {
      return this.timezone || Timezone.UTC;
    }
  }

  Action.register(TimeShiftAction);
}
