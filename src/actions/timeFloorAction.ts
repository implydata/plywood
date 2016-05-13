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
      var duration = parameters.duration;
      this.duration = duration;
      this.timezone = parameters.timezone;
      this._ensureAction("timeFloor");
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
      var timezone = this.getTimezone();
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return null;
        return duration.floor(inV, timezone);
      }
    }

    protected _getJSHelper(inputJS: string): string {
      throw new Error("implement me");
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return dialect.timeFloorExpression(inputSQL, this.duration, this.getTimezone());
    }

    protected _foldWithPrevAction(prevAction: Action): Action {
      if (prevAction.equals(this)) {
        return this;
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
      return new TimeFloorAction(value);
    }

    public getTimezone(): Timezone {
      return this.timezone || Timezone.UTC;
    }

    public alignsWith(actions: Action[]): boolean {
      if (!actions.length) return false;
      const action = actions[0];

      const { timezone, duration } = this;
      if (!timezone) return false;

      if (action instanceof TimeFloorAction || action instanceof TimeBucketAction) {
        return timezone.equals(action.timezone) && action.duration.dividesBy(duration);
      }

      if (action instanceof InAction || action instanceof OverlapAction) {
        var literal = action.getLiteralValue();
        if (TimeRange.isTimeRange(literal)) {
          return literal.isAligned(duration, timezone);
        } else if (Set.isSet(literal)) {
          if (literal.setType !== 'TIME_RANGE') return false;
          return literal.elements.every((e: TimeRange) => {
            return e.isAligned(duration, timezone);
          });
        }
      }

      return false;
    }
  }

  Action.register(TimeFloorAction);
}
