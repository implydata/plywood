module Plywood {
  export class AddAction extends Action {
    static fromJS(parameters: ActionJS): AddAction {
      return new AddAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("add");
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
        return (inputFn(d, c) || 0) + (expressionFn(d, c) || 0);
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return `(${inputJS}+${expressionJS})`;
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `(${inputSQL}+${expressionSQL})`;
    }

    protected _removeAction(): boolean {
      return this.expression.equals(Expression.ZERO);
    }

    protected _distributeAction(): Action[] {
      return this.expression.actionize(this.action);
    }

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      if (literalExpression.equals(Expression.ZERO)) {
        return this.expression;
      }
      return null;
    }

    protected _foldWithPrevAction(prevAction: Action): Action {
      if (prevAction instanceof AddAction) {
        var prevValue = prevAction.expression.getLiteralValue();
        var myValue = this.expression.getLiteralValue();
        if (typeof prevValue === 'number' && typeof myValue === 'number') {
          return new AddAction({
            expression: r(prevValue + myValue)
          });
        }
      }
      return null;
    }
  }

  Action.register(AddAction);
}
