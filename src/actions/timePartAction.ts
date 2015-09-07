module Plywood {
  //var possiblePartings = 'SECOND_OF_DAY';

  export class TimePartAction extends Action {
    static fromJS(parameters: ActionJS): TimePartAction {
      var value = Action.jsToValue(parameters);
      value.part = parameters.part;
      value.timezone = Timezone.fromJS(parameters.timezone);
      return new TimePartAction(value);
    }

    public part: string;
    public timezone: Timezone;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.part = parameters.part;
      this.timezone = parameters.timezone;
      this._ensureAction("timePart");
      if (typeof this.part !== 'string') {
        throw new Error("`part` must be a string");
      }
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.part = this.part;
      value.timezone = this.timezone;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.part = this.part;
      js.timezone = this.timezone.toJS();
      return js;
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'TIME');
      return 'NUMBER';
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [expressionString, this.part.toString(), this.timezone.toString()];
    }

    public equals(other: TimePartAction): boolean {
      return super.equals(other) &&
        this.part === other.part &&
        this.timezone.equals(other.timezone);
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      var part = this.part;
      var timezone = this.timezone;
      return (d: Datum, c: Datum) => {
        // ToDo: make this work
      }
    }

    protected _getJSHelper(inputJS: string): string {
      throw new Error("implement me");
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      // ToDo: make this work
      throw new Error("Vad, srsly make this work")
    }

    public materializeWithinRange(extentRange: TimeRange, values: int[]): Set {
      var partUnits = this.part.toLowerCase().split('_of_');
      var unitSmall = partUnits[0];
      var unitBig = partUnits[1];
      var timezone = this.timezone;
      var smallTimeMover = <Chronoshift.TimeMover>(<any>Chronoshift)[unitSmall];
      var bigTimeMover = <Chronoshift.TimeMover>(<any>Chronoshift)[unitBig];

      var start = extentRange.start;
      var end = extentRange.end;

      var ranges: TimeRange[] = [];
      var iter = bigTimeMover.floor(start, timezone);
      while (iter <= end) {
        for (let value of values) {
          let subIter = smallTimeMover.move(iter, timezone, value);
          ranges.push(new TimeRange({
            start: subIter,
            end: smallTimeMover.move(subIter, timezone, 1)
          }));
        }
        iter = bigTimeMover.move(iter, timezone, 1);
      }

      return Set.fromJS({
        setType: 'TIME_RANGE',
        elements: ranges
      })
    }
  }

  Action.register(TimePartAction);
}
