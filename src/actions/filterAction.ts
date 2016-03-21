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
      this._checkExpressionTypes('BOOLEAN');
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'DATASET');
      return 'DATASET';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return inputType;
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

    protected _putBeforeLastAction(lastAction: Action): Action {
      if (lastAction instanceof ApplyAction) {
        var freeReferences = this.getFreeReferences();
        return freeReferences.indexOf(lastAction.name) === -1 ? this : null;
      }

      if (lastAction instanceof SplitAction) {
        var splits = lastAction.splits;
        return new FilterAction({
          expression: this.expression.substitute((ex) => {
            if (ex instanceof RefExpression && splits[ex.name]) return splits[ex.name];
            return null;
          })
        });
      }

      if (lastAction instanceof SortAction) {
        return this;
      }

      return null;
    }
  }

  Action.register(FilterAction);
}
