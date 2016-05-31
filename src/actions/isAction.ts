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
      return dialect.isNotDistinctFromExpression(inputSQL, expressionSQL);
    }

    protected _nukeExpression(precedingExpression: Expression): Expression {
      var prevAction = precedingExpression.lastAction();
      var literalValue = this.getLiteralValue();

      if (prevAction instanceof TimeBucketAction && literalValue instanceof TimeRange && prevAction.timezone) {
        if (literalValue.start !== null && TimeRange.timeBucket(literalValue.start, prevAction.duration, prevAction.timezone).equals(literalValue)) return null;
        return Expression.FALSE;
      }

      if (prevAction instanceof NumberBucketAction && literalValue instanceof NumberRange) {
        if (literalValue.start !== null && NumberRange.numberBucket(literalValue.start, prevAction.size, prevAction.offset).equals(literalValue)) return null;
        return Expression.FALSE;
      }

      return null;
    }

    protected _foldWithPrevAction(prevAction: Action): Action {
      var literalValue = this.getLiteralValue();

      if (prevAction instanceof TimeBucketAction && literalValue instanceof TimeRange && prevAction.timezone) {
        if (!(literalValue.start !== null && TimeRange.timeBucket(literalValue.start, prevAction.duration, prevAction.timezone).equals(literalValue))) return null;
        return new InAction({ expression: this.expression });
      }

      if (prevAction instanceof NumberBucketAction && literalValue instanceof NumberRange) {
        if (!(literalValue.start !== null && NumberRange.numberBucket(literalValue.start, prevAction.size, prevAction.offset).equals(literalValue))) return null;
        return new InAction({ expression: this.expression })
      }

      if (prevAction instanceof FallbackAction && prevAction.expression.isOp('literal') && this.expression.isOp('literal') && !prevAction.expression.equals(this.expression)) {
        return this;
      }

      return null;
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

      return null;
    }
  }

  Action.register(IsAction);
}
