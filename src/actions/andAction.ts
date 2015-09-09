module Plywood {

  function mergeAnd(ex1: ChainExpression, ex2: ChainExpression): Expression {
    if (
      !ex1.expression.isOp('ref') ||
      !ex2.expression.isOp('ref') ||
      !arraysEqual(ex1.getFreeReferences(), ex2.getFreeReferences())
    ) return null;

    var ex1Actions = ex1.actions;
    var ex2Actions = ex2.actions;
    if (ex1Actions.length !== 1 || ex2Actions.length !== 1) return null;

    var firstActionExpression1 = ex1Actions[0].expression;
    var firstActionExpression2 = ex2Actions[0].expression;
    if (!firstActionExpression1.isOp('literal') || !firstActionExpression2.isOp('literal')) return null;

    var intersect = Set.generalIntersect(firstActionExpression1.getLiteralValue(), firstActionExpression2.getLiteralValue());
    if (intersect === null) return null;

    return Expression.inOrIs(ex1.expression, intersect);
  }


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
      var { expression } = this;
      if (expression.equals(Expression.FALSE)) {
        return Expression.FALSE;

      } else if (expression instanceof ChainExpression) {
        return mergeAnd(chainExpression, expression);

      }

      return null;
    }
  }

  Action.register(AndAction);
}
