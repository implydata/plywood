module Plywood {
  var timeBucketing: Lookup<string> = {
    "PT1S": "%Y-%m-%dT%H:%i:%SZ",
    "PT1M": "%Y-%m-%dT%H:%i:00Z",
    "PT1H": "%Y-%m-%dT%H:00:00Z",
    "P1D":  "%Y-%m-%dT00:00:00Z",
    "P1W":  "%Y-%m-%dT00:00:00Z",
    "P1M":  "%Y-%m-00T00:00:00Z",
    "P1Y":  "%Y-00-00T00:00:00Z"
  };

  export class TimeBucketAction extends Action {
    static fromJS(parameters: ActionJS): TimeBucketAction {
      var value = Action.jsToValue(parameters);
      value.duration = Duration.fromJS(parameters.duration);
      value.timezone = Timezone.fromJS(parameters.timezone);
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
      if (!Timezone.isTimezone(this.timezone)) {
        throw new Error("`timezone` must be a Timezone");
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
      return 'TIME_RANGE';
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [this.duration.toString(), this.timezone.toString()];
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
        return TimeRange.timeBucket(inV, duration, timezone);
      }
    }

    protected _getJSHelper(inputJS: string): string {
      throw new Error("implement me");
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      var bucketFormat = timeBucketing[this.duration.toString()];
      if (!bucketFormat) throw new Error("unsupported duration '" + this.duration + "'");

      var bucketTimezone = this.timezone.toString();
      var expression = inputSQL;
      if (bucketTimezone !== "Etc/UTC") {
        expression = `CONVERT_TZ(${expression}, '+0:00', '${bucketTimezone}')`;
      }

      return `DATE_FORMAT(${expression}, '${bucketFormat}')`;
    }
  }

  Action.register(TimeBucketAction);
}
