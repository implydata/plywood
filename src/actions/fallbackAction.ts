module Plywood {
  export class FallbackAction extends Action {
    static fromJS(parameters: ActionJS): FallbackAction {
      return new FallbackAction({
        action: parameters.action,
        fallbackValue: Expression.fromJS(parameters.fallbackValue)
      });
    }

    public fallbackValue: Expression;
    constructor(parameters: ActionValue = {}) {
      super(parameters, dummyObject);
      this._ensureAction("fallback");
      this.fallbackValue = parameters.fallbackValue;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.fallbackValue = this.fallbackValue.toJS();
      return js;
    }

    public getOutputType(inputType: string): string {
      var expressionType = this.expression.type;
      if (expressionType !== 'NULL') this._checkInputType(inputType, expressionType);
      return expressionType;
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var val = inputFn(d, c);
        if (val === null) return expressionFn(d, c);
        return val;
      }
    }

    protected _getJSHelper(inputJS: string): string {
      throw new Error('can not express as JS');
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `COALESCE(" + inputSQL + "," + ${expressionSQL} + ")`;
    }
  }

  Action.register(FallbackAction);
}
