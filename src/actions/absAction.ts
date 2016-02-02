module Plywood {
  export class AbsAction extends Action {
    static fromJS(parameters: ActionJS): AbsAction {
      return new AbsAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("abs");
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'NUMBER');
      return 'NUMBER';
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var v = Math.abs(inputFn(d, c));
        return v;
      }
    }

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      if (literalExpression.equals(Expression.ZERO)) {
        return Expression.ZERO;
      }
    }

    protected _foldWithPrevAction(prevAction: Action): Action {
      if (prevAction.equals(this)) {
        return this;
      }
      return null;
    }

    protected _getJSHelper(inputJS: string): string {
      return 'Math.abs(' + inputJS + ')';
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return 'ABS(' + inputSQL + ')';
    }
  }

  Action.register(AbsAction);
}
