module Plywood {
  export class FilterAction extends Action {
    static fromJS(parameters: ActionJS): FilterAction {
      return new FilterAction({
        action: parameters.action,
        name: parameters.name,
        expression: Expression.fromJS(parameters.expression)
      });
    }

    constructor(parameters: ActionValue = {}) {
      super(parameters, dummyObject);
      this._ensureAction("filter");
      this._checkExpressionType('BOOLEAN');
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'DATASET');
      return 'DATASET';
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `${inputSQL} WHERE ${expressionSQL}`;
    }

    public isNester(): boolean {
      return true;
    }

    protected _foldWithPrevAction(prevAction: Action): Action {
      if (prevAction instanceof FilterAction) {
        return new FilterAction({
          expression: prevAction.expression.and(this.expression)
        });
      }
      return null;
    }
  }

  Action.register(FilterAction);
}
