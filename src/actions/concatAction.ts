module Plywood {
  export class ConcatAction extends Action {
    static fromJS(parameters: ActionJS): ConcatAction {
      return new ConcatAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("concat");
      this._checkExpressionTypes('STRING');
    }

    public getOutputType(inputType: PlyType): PlyType {
      return this._stringTransformOutputType(inputType);
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return inputType;
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
      return Expression.jsNullSafety(inputJS, expressionJS, (a, b) => { return `${a}+${b}` }, inputJS[0] === '"', expressionJS[0] === '"');
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return dialect.concatExpression(inputSQL, expressionSQL);
    }

    protected _removeAction(): boolean {
      return this.expression.equals(Expression.EMPTY_STRING);
    }

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      if (literalExpression.equals(Expression.EMPTY_STRING)) {
        return this.expression;
      }
      return null;
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
