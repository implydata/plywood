module Plywood {
  export class MaxAction extends Action {
    static fromJS(parameters: ActionJS): MaxAction {
      return new MaxAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("max");
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'DATASET');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return {
        type: 'NUMBER',
        remote: typeContext.remote
      };
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return 'MAX(' + expressionSQL + ')';
    }

    public isAggregate(): boolean {
      return true;
    }

    public isNester(): boolean {
      return true;
    }
  }

  Action.register(MaxAction);
}
