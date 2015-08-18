module Plywood {
  export class IsAction extends Action {
    static fromJS(parameters: ActionJS): IsAction {
      return new IsAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("is");
    }

    public getOutputType(inputType: string): string {
      var expressionType = this.expression.type;
      if (expressionType !== 'NULL') this._checkInputType(inputType, expressionType);
      return 'BOOLEAN';
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
      return `(${inputSQL}=${expressionSQL})`;
    }

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      var expression = this.expression;
      if (expression instanceof RefExpression) {
        return expression.is(literalExpression);
      }
      return null;
    }

    protected _performOnRef(refExpression: RefExpression): Expression {
      if (this.expression.equals(refExpression)) {
        return Expression.TRUE;
      }
      return null;
    }

    protected _performOnChain(chainExpression: ChainExpression): Expression {
      if (this.expression.equals(chainExpression)) {
        return Expression.TRUE;
      }

      var actions = chainExpression.actions;
      var lastAction = actions[actions.length - 1];
      var literalValue = this.getLiteralValue();
      if (lastAction instanceof TimeBucketAction && literalValue instanceof TimeRange) {
        var duration = lastAction.duration;
        var timezone = lastAction.timezone;
        var start = literalValue.start;
        var end = literalValue.end;

        if (duration.isSimple()) {
          if (duration.floor(start, timezone).valueOf() === start.valueOf() &&
            duration.move(start, timezone, 1).valueOf() === end.valueOf()) {

            actions = actions.slice(0, -1);
            actions.push(new InAction({
              expression: this.expression
            }));

            var chainExpressionValue = chainExpression.valueOf();
            chainExpressionValue.actions = actions;
            return new ChainExpression(chainExpressionValue);
          } else {
            return Expression.FALSE;
          }
        }
      }
      return null;
    }

    /*
    public mergeAnd(ex: Expression): Expression {
      if (ex.isOp('literal')) return ex.mergeAnd(this);

      var references = this.getFreeReferences();

      if (!arraysEqual(references, ex.getFreeReferences())) return null;
      if (this.type !== ex.type) return null;

      if (ex instanceof IsExpression) {
        if (references.length === 2) return this;
        if (!(this.lhs instanceof RefExpression && ex.lhs instanceof RefExpression)) return null;

        if (
          (<LiteralExpression>this.rhs).value.valueOf &&
          (<LiteralExpression>ex.rhs).value.valueOf &&
          (<LiteralExpression>ex.rhs).value.valueOf() === (<LiteralExpression>this.rhs).value.valueOf()
        ) return this; // for higher objects
        if ((<LiteralExpression>this.rhs).value === (<LiteralExpression>ex.rhs).value) return this; // for simple values;
        return Expression.FALSE;

      } else if (ex instanceof InExpression) {
        return ex.mergeAnd(this);
      } else {
        return null;
      }
    }

    public mergeOr(ex: Expression): Expression {
      if (ex.isOp('literal')) return ex.mergeOr(this);

      var references = this.getFreeReferences();

      if (!arraysEqual(references, ex.getFreeReferences())) return null;
      if (this.type !== ex.type) return null;

      if (ex instanceof IsExpression) {
        if (references.length === 2) return this;
        if (!(this.lhs instanceof RefExpression && ex.lhs instanceof RefExpression)) return null;

        var thisValue = (<LiteralExpression>this.rhs).value;
        var expValue = (<LiteralExpression>(ex.rhs)).value;

        if (
          thisValue.valueOf &&
          expValue.valueOf &&
          expValue.valueOf() === thisValue.valueOf()
        ) return this; // for higher objects
        if (thisValue === expValue) return this; // for simple values;
        return new InExpression({
          op: 'in',
          lhs: this.lhs,
          rhs: new LiteralExpression({
            op: 'literal',
            value: Set.fromJS([thisValue, expValue])
          })
        });

      } else if (ex instanceof InExpression) {
        return ex.mergeOr(this);
      } else {
        return null;
      }
    }
    */
  }

  Action.register(IsAction);
}
