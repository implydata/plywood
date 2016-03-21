module Plywood {
  export class CountDistinctAction extends Action {
    static fromJS(parameters: ActionJS): CountDistinctAction {
      return new CountDistinctAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("countDistinct");
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
      return `COUNT(DISTINCT ${dialect.aggregateFilterIfNeeded(inputSQL, expressionSQL, 'NULL')})`;
    }

    public isAggregate(): boolean {
      return true;
    }

    public isNester(): boolean {
      return true;
    }
  }

  Action.register(CountDistinctAction);
}
