module Plywood {
  export class FallbackAction extends Action {
    static fromJS(parameters: ActionJS): FallbackAction {
      return new FallbackAction({
        action: parameters.action,
        expression: Expression.fromJS(parameters.expression),
        fallbackValue: parameters.fallbackValue
      });
    }

    public fallbackValue: string;

    constructor(parameters: ActionValue = {}) {
      super(parameters, dummyObject);
      this.fallbackValue = parameters.fallbackValue;
      if (!this.expression.isOp('ref')) {
        throw new Error("must be a reference expression (for now): " + this.toString());
      }

      this._ensureAction("fallback");
    }

    public getOutputType(inputType: string): string {
      this._checkInputTypes(inputType, 'DATASET');
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

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      var fallbackValue = this.fallbackValue;
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        var expression : any = this.expression;
        var name = expression['name'];
        return inV.fallback(fallbackValue, name);
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
