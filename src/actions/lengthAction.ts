module Plywood {
  export class LengthAction extends Action {
    static fromJS(parameters: ActionJS): LengthAction {
      return new LengthAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("length");
      this._checkNoExpression();
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'STRING');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType): FullType {
      return inputType;
    }

    protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return null;
        return inV.length;
      }
    }
    
    protected _getJSHelper(inputType: PlyType, inputJS: string): string {
      return Expression.jsNullSafetyUnary(inputJS, (input: string) => `${input}.length`);
    }

    protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return dialect.lengthExpression(inputSQL);
    }
  }

  Action.register(LengthAction);
}
