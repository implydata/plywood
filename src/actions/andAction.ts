module Plywood {
  export class AndAction extends Action {
    static fromJS(parameters: ActionJS): AndAction {
      return new AndAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("and");
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'BOOLEAN');
      return 'BOOLEAN';
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => inputFn(d, c) && expressionFn(d, c);
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return '(' + inputJS + '&&' + expressionJS + ')';
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return '(' + inputSQL + ' AND ' + expressionSQL + ')';
    }

    protected _removeAction(): boolean {
      return this.expression.equals(Expression.TRUE);
    }

    protected _distributeAction(): Action[] {
      return this.expression.actionize(this.action);
    }

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      if (literalExpression.equals(Expression.TRUE)) {
        return this.expression;
      } else if (literalExpression.equals(Expression.FALSE)) {
        return Expression.FALSE;
      }
      return null;
    }

    protected _performOnRef(refExpression: RefExpression): Expression {
      if (this.expression.equals(Expression.FALSE)) {
        return Expression.FALSE;
      }
      return null;
    }

    protected _performOnChain(chainExpression: ChainExpression): Expression {
      if (this.expression.equals(Expression.FALSE)) {
        return Expression.FALSE;
      }
      return null;
    }
  }

  Action.register(AndAction);
}
