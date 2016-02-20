module Plywood {
  export class GreaterThanOrEqualAction extends Action {
    static fromJS(parameters: ActionJS): GreaterThanOrEqualAction {
      return new GreaterThanOrEqualAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("greaterThanOrEqual");
      this._checkExpressionTypes('NUMBER', 'TIME');
    }

    public getOutputType(inputType: string): string {
      var expressionType = this.expression.type;
      if (expressionType) this._checkInputTypes(inputType, expressionType);
      return 'BOOLEAN';
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        return inputFn(d, c) >= expressionFn(d, c);
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return `(${inputJS}>=${expressionJS})`;
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `(${inputSQL}>=${expressionSQL})`;
    }

    protected _specialSimplify(simpleExpression: Expression): Action {
      var expression = this.expression;
      if (expression instanceof LiteralExpression) { // x >= 5
        return new InAction({
          expression: new LiteralExpression({
            value: Range.fromJS({ start: expression.value, end: null, bounds: '[)' })
          })
        });
      }
      return null;
    }

    protected _performOnSimpleLiteral(literalExpression: LiteralExpression): Expression {
      // 5 >= x
      return (new InAction({
        expression: new LiteralExpression({
          value: Range.fromJS({ start: null, end: literalExpression.value, bounds: '(]' })
        })
      })).performOnSimple(this.expression);
    }
  }

  Action.register(GreaterThanOrEqualAction);
}
