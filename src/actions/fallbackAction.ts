module Plywood {
  export class FallbackAction extends Action {
    static fromJS(parameters: ActionJS): FallbackAction {
      return new FallbackAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue = {}) {
      super(parameters, dummyObject);
      this._ensureAction("fallback");
    }

    public getOutputType(inputType: PlyType): PlyType {
      var expressionType = this.expression.type;
      if (expressionType && expressionType !== 'NULL') this._checkInputTypes(inputType, expressionType);
      return expressionType;
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return inputType;
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var val = inputFn(d, c);
        if (val === null) {
          return expressionFn(d, c);
        }
        return val;
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return `(_ = ${inputJS}, (_ === null ? ${expressionJS} : _))`
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `COALESCE(${inputSQL}, ${expressionSQL})`;
    }

    protected _removeAction(): boolean {
      return this.expression.equals(Expression.NULL);
    }

  }

  Action.register(FallbackAction);
}
