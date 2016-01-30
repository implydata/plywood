module Plywood {
  export class FallbackAction extends Action {
    static fromJS(parameters: ActionJS): FallbackAction {
      return new FallbackAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue = {}) {
      super(parameters, dummyObject);
      this._ensureAction("fallback");
    }

    public getOutputType(inputType: string): string {
      var expressionType = this.expression.type;
      if (expressionType !== 'NULL') this._checkInputType(inputType, expressionType);
      return expressionType;
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var val = inputFn(d, c);
        if (val === null) {
          return expressionFn(d, c);
        }
        return val;
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      if (inputJS === null) return expressionJS;
      return inputJS;
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `COALESCE( ${inputSQL}, ${expressionSQL})`;

    }

    protected _removeAction(): boolean {
      return this.expression.equals(Expression.NULL);
    }

  }

  Action.register(FallbackAction);
}
