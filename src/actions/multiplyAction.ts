module Plywood {
  export class MultiplyAction extends Action {
    static fromJS(parameters: ActionJS): MultiplyAction {
      return new MultiplyAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("multiply");
      this._checkExpressionTypes('NUMBER');
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'NUMBER');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return inputType;
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        return (inputFn(d, c) || 0) * (expressionFn(d, c) || 0);
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return `(${inputJS}*${expressionJS})`;
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `(${inputSQL}*${expressionSQL})`;
    }

    protected _removeAction(): boolean {
      return this.expression.equals(Expression.ONE);
    }

    protected _nukeExpression(): Expression {
      if (this.expression.equals(Expression.ZERO)) return Expression.ZERO;
      return null;
    }

    protected _distributeAction(): Action[] {
      return this.expression.actionize(this.action);
    }

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      if (literalExpression.equals(Expression.ONE)) {
        return this.expression;
      } else if (literalExpression.equals(Expression.ZERO)) {
        return Expression.ZERO;
      }
      return null;
    }
  }

  Action.register(MultiplyAction);
}
