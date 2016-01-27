module Plywood {
  export class FallbackAction extends Action {
    static fromJS(parameters: ActionJS): FallbackAction {
      return new FallbackAction({
        action: parameters.action,
        fallbackValue: parameters.fallbackValue
      });
    }

    public fallbackValue: string;
    public fallbackCondition: string;

    constructor(parameters: ActionValue = {}) {
      super(parameters, dummyObject);
      this.fallbackValue = parameters.fallbackValue;
      this._ensureAction("fallback");
    }

    public getOutputType(inputType: string): string {
      var expression = this.expression;
      this._checkInputType(inputType, 'DATASET');
      return 'DATASET';
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.fallbackValue = this.fallbackValue;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.fallbackValue = this.fallbackValue;
      return js;
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [String(this.fallbackValue)];
    }

    public equals(other: FallbackAction): boolean {
      return super.equals(other) &&
        this.fallbackValue === other.fallbackValue;
    }


    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      var fallbackValue = this.fallbackValue;
      var fallbackCondition = this.fallbackCondition ? this.fallbackCondition : null;

      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === fallbackCondition) return fallbackValue;
        return inV;
      }
    }

    protected _getJSHelper(inputJS: string): string {
      throw new Error('can not express as JS');
    }

    // this is just
    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `COALESCE(" + expressionSQL + "," + ${this.fallbackValue} + ")`;
    }
  }

  Action.register(FallbackAction);
}
