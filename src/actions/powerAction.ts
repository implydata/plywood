module Plywood {
  export class PowerAction extends Action {
    static fromJS(parameters: ActionJS): PowerAction {
      return new PowerAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("power");
      this._checkExpressionType('NUMBER');
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'NUMBER');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return {
        type: 'NUMBER',
        remote: typeContext.remote
      };
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        return Math.pow((inputFn(d, c) || 0), (expressionFn(d, c) || 0));
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return 'Math.pow(' + inputJS + ',' + expressionJS + ')';
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return 'POW(' + inputSQL + ',' + expressionSQL + ')';
    }

    protected _removeAction(): boolean {
      return this.expression.equals(Expression.ONE);
    }

    protected _distributeAction(): Action[] {
      return this.expression.actionize(this.action);
    }

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      if (literalExpression.equals(Expression.ZERO)) {
        return Expression.ZERO;
      }
      if (literalExpression.equals(Expression.ONE)) {
        return Expression.ONE;
      }

    }

  }

  Action.register(PowerAction);
}
