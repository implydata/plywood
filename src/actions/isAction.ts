module Plywood {
  export class IsAction extends Action {
    static fromJS(parameters: ActionJS): IsAction {
      return new IsAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("is");
    }

    public getOutputType(inputType: PlyType): PlyType {
      var expressionType = this.expression.type;
      if (expressionType && expressionType !== 'NULL') this._checkInputTypes(inputType, expressionType);
      return 'BOOLEAN';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return {
        type: 'BOOLEAN'
      };
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        return inputFn(d, c) === expressionFn(d, c);
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return `(${inputJS}===${expressionJS})`;
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `(${inputSQL}<=>${expressionSQL})`;
    }

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      var expression = this.expression;
      if (!expression.isOp('literal')) {
        return new IsAction({ expression: literalExpression }).performOnSimple(expression);
      }
      return null;
    }

    protected _performOnRef(refExpression: RefExpression): Expression {
      if (this.expression.equals(refExpression)) {
        return Expression.TRUE;
      }
      return null;
    }

    protected _performOnSimpleChain(chainExpression: ChainExpression): Expression {
      if (this.expression.equals(chainExpression)) {
        return Expression.TRUE;
      }

      var lastAction = chainExpression.lastAction();
      var literalValue = this.getLiteralValue();
      if (lastAction instanceof TimeBucketAction && literalValue instanceof TimeRange && lastAction.timezone) {
        var duration = lastAction.duration;
        var timezone = lastAction.timezone;
        var start = literalValue.start;
        var end = literalValue.end;

        if (duration.isFloorable()) {
          if (duration.floor(start, timezone).valueOf() === start.valueOf() &&
            duration.move(start, timezone, 1).valueOf() === end.valueOf()) {

            return new InAction({ expression: this.expression }).performOnSimple(chainExpression.popAction());
          } else {
            return Expression.FALSE;
          }
        }
      }
      return null;
    }
  }

  Action.register(IsAction);
}
