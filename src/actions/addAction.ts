module Plywood {
  export class AddAction extends Action {
    static fromJS(parameters: ActionJS): AddAction {
      return new AddAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("add");
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
        return (inputFn(d, c) || 0) + (expressionFn(d, c) || 0);
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return '(' + inputJS + '+' + expressionJS + ')';
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return '(' + inputSQL + '+' + expressionSQL + ')';
    }

    protected _specialSimplify(simpleExpression: Expression): ActionSimplification {
      if (simpleExpression.equals(Expression.ZERO)) {
        return {
          simplification: Simplification.Remove
        };
      } else if (simpleExpression instanceof ChainExpression) {
        var newActions = simpleExpression.actionize(this.action);
        if (!newActions) return null;
        return { simplification: Simplification.Replace, actions: newActions };
      }
      return null;
    }

    protected _specialFoldLiteral(literalInput: LiteralExpression): Expression {
      if (literalInput.equals(Expression.ZERO)) {
        return this.expression;
      }
      return null;
    }
  }

  Action.register(AddAction);
}
