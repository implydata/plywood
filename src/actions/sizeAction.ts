module Plywood {
  export class SizeAction extends Action {
    static fromJS(parameters: ActionJS): SizeAction {
      return new SizeAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("size");
      this._checkNoExpression();
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'STRING', 'SET/STRING', 'SET/NUMBER', 'SET/TIME');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType): FullType {
      return inputType;
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return 0;
        if (typeof inV === 'string' || Array.isArray(inV)) return inV.length;
        return (inV as Set).elements.length;
      }
    }
    
    protected _getJSHelper(inputJS: string): string {
      return `${inputJS}.length`
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `LENGTH(${inputSQL})`
    }
  }

  Action.register(SizeAction);
}
