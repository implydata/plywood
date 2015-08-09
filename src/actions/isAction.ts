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
      this._checkInputType(inputType, this.expression.type);
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

    protected _performOnChain(chainExpression: ChainExpression): Expression {
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

    protected _specialSimplify(simpleLhs: Expression, simpleRhs: Expression): Expression {
      if (simpleLhs.equals(simpleRhs)) return Expression.TRUE;

      if (simpleLhs instanceof TimeBucketExpression && simpleRhs instanceof LiteralExpression) {
        var duration = simpleLhs.duration;
        var value: TimeRange = simpleRhs.value;
        var start = value.start;
        var end = value.end;

        if (duration.isSimple()) {
          if (duration.floor(start, simpleLhs.timezone).valueOf() === start.valueOf() &&
              duration.move(start, simpleLhs.timezone, 1).valueOf() === end.valueOf()) {
            return new InExpression({
              op: 'in',
              lhs: simpleLhs.operand,
              rhs: simpleRhs
            })
          } else {
            return Expression.FALSE;
          }
        }
      }

      return null;
    }
    */
  }

  Action.register(IsAction);
}
