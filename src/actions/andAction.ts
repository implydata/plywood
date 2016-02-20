module Plywood {

  function mergeAnd(ex1: Expression, ex2: Expression): Expression {
    if (
      !ex1.isOp('chain') ||
      !ex2.isOp('chain') ||
      !(<ChainExpression>ex1).expression.isOp('ref') ||
      !(<ChainExpression>ex2).expression.isOp('ref') ||
      !arraysEqual(ex1.getFreeReferences(), ex2.getFreeReferences())
    ) return null;

    var ex1Actions = (<ChainExpression>ex1).actions;
    var ex2Actions = (<ChainExpression>ex2).actions;
    if (ex1Actions.length !== 1 || ex2Actions.length !== 1) return null;

    var firstActionExpression1 = ex1Actions[0].expression;
    var firstActionExpression2 = ex2Actions[0].expression;

    if (!firstActionExpression1 || !firstActionExpression2 || !firstActionExpression1.isOp('literal') || !firstActionExpression2.isOp('literal')) return null;

    var intersect = Set.generalIntersect(firstActionExpression1.getLiteralValue(), firstActionExpression2.getLiteralValue());
    if (intersect === null) return null;

    return Expression.inOrIs((<ChainExpression>ex1).expression, intersect);
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
      this._checkInputTypes(inputType, 'BOOLEAN');
      return 'BOOLEAN';
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => inputFn(d, c) && expressionFn(d, c);
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return `(${inputJS}&&${expressionJS})`;
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `(${inputSQL} AND ${expressionSQL})`;
    }

    protected _removeAction(): boolean {
      return this.expression.equals(Expression.TRUE);
    }

    protected _nukeExpression(): Expression {
      if (this.expression.equals(Expression.FALSE)) return Expression.FALSE;
      return null;
    }

    protected _distributeAction(): Action[] {
      return this.expression.actionize(this.action);
    }

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      if (literalExpression.equals(Expression.TRUE)) {
        return this.expression;
      }
      if (literalExpression.equals(Expression.FALSE)) {
        return Expression.FALSE;
      }
      return null;
    }

    protected _performOnSimpleChain(chainExpression: ChainExpression): Expression {
      var { expression } = this;

      var andExpressions = chainExpression.getExpressionPattern('and');
      if (andExpressions) {
        for (var i = 0; i < andExpressions.length; i++) {
          var andExpression = andExpressions[i];
          var mergedExpression = mergeAnd(andExpression, expression);
          if (mergedExpression) {
            andExpressions[i] = mergedExpression;
            return Expression.and(andExpressions).simplify();
          }
        }
      } else {
        return mergeAnd(chainExpression, expression);
      }
    }
  }

  Action.register(AndAction);
}
