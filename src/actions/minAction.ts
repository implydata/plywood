module Plywood {
  export class MinAction extends Action {
    static fromJS(parameters: ActionJS): MinAction {
      return new MinAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("min");
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
      return 'MIN(' + expressionSQL + ')';
    }

    public contextDiff(): int {
      return -1;
    }
  }

  Action.register(MinAction);
}
