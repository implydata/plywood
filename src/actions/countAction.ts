module Plywood {
  export class CountAction extends Action {
    static fromJS(parameters: ActionJS): CountAction {
      return new CountAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("count");
      this._checkNoExpression();
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'DATASET');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(): FullType {
      return {
        type: 'NUMBER',
      };
    }

    public getFn(inputFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        return inV ? inV.count() : 0;
      }
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return inputSQL.indexOf(' WHERE ') === -1 ? `COUNT(*)` : `SUM(${dialect.aggregateFilterIfNeeded(inputSQL, '1')})`;
    }

    public isAggregate(): boolean {
      return true;
    }
  }

  Action.register(CountAction);
}
