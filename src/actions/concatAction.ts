module Plywood {
  export class ConcatAction extends Action {
    static fromJS(parameters: ActionJS): ConcatAction {
      return new ConcatAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("concat");
      this._checkExpressionType('STRING');
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'STRING');
      return 'STRING';
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return null;
        var exV = expressionFn(d, c);
        if (exV === null) return null;
        return '' + inV + exV;
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return '(' + inputJS + '+' + expressionJS + ')';
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return 'CONCAT(' + inputSQL + ',' + expressionSQL + ')';
    }

    protected _removeAction(): boolean {
      return this.expression.equals(Expression.EMPTY_STRING);
    }

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      if (literalExpression.equals(Expression.EMPTY_STRING)) {
        return this.expression;
      }
    }

    protected _foldWithPrevAction(prevAction: Action): Action {
      if (prevAction instanceof ConcatAction) {
        var prevValue = prevAction.expression.getLiteralValue();
        var myValue = this.expression.getLiteralValue();
        if (typeof prevValue === 'string' && typeof myValue === 'string') {
          return new ConcatAction({
            expression: r(prevValue + myValue)
          });
        }
      }
      return null;
    }
  }

  Action.register(ConcatAction);
}
