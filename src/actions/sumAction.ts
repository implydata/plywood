module Plywood {
  export class SumAction extends Action {
    static fromJS(parameters: ActionJS): SumAction {
      return new SumAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("sum");
      this._checkExpressionTypes('NUMBER');
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'DATASET');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return {
        type: 'NUMBER'
      };
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `SUM(${dialect.aggregateFilterIfNeeded(inputSQL, expressionSQL)})`;
    }

    public isAggregate(): boolean {
      return true;
    }

    public isNester(): boolean {
      return true;
    }

    public canDistribute(): boolean {
      var expression = this.expression;
      return expression instanceof LiteralExpression ||
        Boolean(expression.getExpressionPattern('add') || expression.getExpressionPattern('subtract'));
    }

    public distribute(preEx: Expression): Expression {
      var expression = this.expression;
      if (expression instanceof LiteralExpression) {
        var value = expression.value;
        if (value === 0) return Expression.ZERO;
        return expression.multiply(preEx.count()).simplify();
      }

      var pattern: Expression[];
      if (pattern = expression.getExpressionPattern('add')) {
        return Expression.add(pattern.map(ex => preEx.sum(ex).distribute()));
      }
      if (pattern = expression.getExpressionPattern('subtract')) {
        return Expression.subtract(pattern.map(ex => preEx.sum(ex).distribute()));
      }
      return null;
    }
  }

  Action.register(SumAction);
}
