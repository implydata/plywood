module Plywood {
  export class SubtractAction extends Action {
    static fromJS(parameters: ActionJS): SubtractAction {
      return new SubtractAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("subtract");
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
        return (inputFn(d, c) || 0) - (expressionFn(d, c) || 0);
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return `(${inputJS}-${expressionJS})`;
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `(${inputSQL}-${expressionSQL})`;
    }

    protected _removeAction(): boolean {
      return this.expression.equals(Expression.ZERO);
    }
  }

  Action.register(SubtractAction);
}
