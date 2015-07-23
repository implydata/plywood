module Plywood {
  export class CountDistinctAction extends Action {
    static fromJS(parameters: ActionJS): CountDistinctAction {
      return new CountDistinctAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("sum");
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'DATASET');
      return 'NUMBER';
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return 'COUNT(DISTINCT ' + expressionSQL + ')';
    }
  }

  Action.register(CountDistinctAction);
}
