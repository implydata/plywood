module Plywood {
  export class MaxAction extends Action {
    static fromJS(parameters: ActionJS): MaxAction {
      return new MaxAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("max");
      this._checkExpressionTypes('NUMBER', 'TIME');
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'DATASET');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return {
        type: 'NUMBER',
      };
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `MAX(${dialect.aggregateFilterIfNeeded(inputSQL, expressionSQL)})`;
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
