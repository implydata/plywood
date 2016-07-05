module Plywood {
  export class IndexOfAction extends Action {
    static fromJS(parameters: ActionJS): IndexOfAction {
      return new IndexOfAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("indexOf");
      this._checkExpressionTypes('STRING');
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'STRING');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return inputType;
    }

    protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return null;
        return inV.indexOf(expressionFn(d, c));
      }
    }

    protected _getJSHelper(inputType: PlyType, inputJS: string, expressionJS: string): string {
      return Expression.jsNullSafetyBinary(inputJS, expressionJS, (a, b) => { return `${a}.indexOf(${b})` }, inputJS[0] === '"', expressionJS[0] === '"');
    }

    protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return dialect.indexOfExpression(inputSQL, expressionSQL);
    }
  }

  Action.register(IndexOfAction);
}
