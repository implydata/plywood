module Plywood {
  export class CountAction extends Action {
    static fromJS(parameters: ActionJS): CountAction {
      return new CountAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("count");
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'DATASET');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      return {
        type: 'NUMBER',
        remote: typeContext.remote
      };
    }

    public getFn(inputFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        return inV ? inV.count() : 0;
      }
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return 'COUNT(*)';
    }
  }

  Action.register(CountAction);
}
