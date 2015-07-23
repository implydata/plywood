module Plywood {
  export class AverageAction extends Action {
    static fromJS(parameters: ActionJS): AverageAction {
      return new AverageAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("average");
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'DATASET');
      return 'NUMBER';
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return 'AVG(' + expressionSQL + ')';
    }
  }

  Action.register(AverageAction);
}
