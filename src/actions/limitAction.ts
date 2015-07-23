module Plywood {

  export class LimitAction extends Action {
    static fromJS(parameters: ActionJS): LimitAction {
      return new LimitAction({
        action: parameters.action,
        limit: parameters.limit
      });
    }

    public limit: int;

    constructor(parameters: ActionValue = {}) {
      super(parameters, dummyObject);
      this.limit = parameters.limit;
      this._ensureAction("limit");
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.limit = this.limit;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.limit = this.limit;
      return js;
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'DATASET');
      return 'DATASET';
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [String(this.limit)];
    }

    public equals(other: LimitAction): boolean {
      return super.equals(other) &&
        this.limit === other.limit;
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      var limit = this.limit;
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        return inV ? inV.limit(limit) : null;
      }
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `LIMIT ${this.limit}`;
    }
  }

  Action.register(LimitAction);
}
