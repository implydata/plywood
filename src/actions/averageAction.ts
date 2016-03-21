module Plywood {
  export class AverageAction extends Action {
    static fromJS(parameters: ActionJS): AverageAction {
      return new AverageAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("average");
      this._checkExpressionTypes('NUMBER');
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
      return `AVG(${dialect.aggregateFilterIfNeeded(inputSQL, expressionSQL)})`;
    }

    public isAggregate(): boolean {
      return true;
    }

    public isNester(): boolean {
      return true;
    }
  }

  Action.register(AverageAction);
}
