module Plywood {
  export class CardinalityAction extends Action {
    static fromJS(parameters: ActionJS): CardinalityAction {
      return new CardinalityAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("cardinality");
      this._checkNoExpression();
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'SET/STRING', 'SET/STRING_RANGE', 'SET/NUMBER', 'SET/NUMBER_RANGE', 'SET/TIME', 'SET/TIME_RANGE');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType): FullType {
      return inputType;
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return null;
        if (Array.isArray(inV)) return inV.length; // this is to allow passing an array into .compute()
        return inV.size();
      }
    }

    protected _getJSHelper(inputJS: string): string {
      return Expression.jsNullSafetyUnary(inputJS, (input: string) => `${input}.length`);
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `cardinality(${inputSQL})`
    }
  }

  Action.register(CardinalityAction);
}
