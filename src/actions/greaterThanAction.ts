module Plywood {
  export class GreaterThanAction extends Action {
    static fromJS(parameters: ActionJS): GreaterThanAction {
      return new GreaterThanAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("greaterThan");
      this._checkExpressionTypes('NUMBER', 'TIME');
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, this.expression.type);
      return 'BOOLEAN';
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        return inputFn(d, c) > expressionFn(d, c);
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return `(${inputJS}>${expressionJS})`;
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `(${inputSQL}>${expressionSQL})`;
    }

    protected _specialSimplify(simpleExpression: Expression): ActionSimplification {
      var expression = this.expression;
      if (expression instanceof LiteralExpression) {
        return {
          simplification: Simplification.Replace,
          action: new InAction({
            expression: new LiteralExpression({
              value: Range.fromJS({ start: expression.value, end: null, bounds: '()' })
            })
          })
        };
      }
      return null;
    }
  }

  Action.register(GreaterThanAction);
}
