module Plywood {
  export class ContainsAction extends Action {
    static fromJS(parameters: ActionJS): ContainsAction {
      return new ContainsAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("contains");
      this._checkExpressionType('STRING');
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'STRING');
      return 'STRING';
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        return String(inputFn(d, c)).indexOf(expressionFn(d, c)) > -1;
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return `String(${inputJS}).indexOf(${expressionJS}) > -1`;
    }

    public getSQL(inputSQL: string, dialect: SQLDialect): string {
      var expression = this.expression;
      if (expression instanceof LiteralExpression) {
        return `${inputSQL} LIKE "%${expression.value}%"`;
      } else {
        throw new Error(`can not express ${this.toString()} in SQL`);
      }
    }
  }

  Action.register(ContainsAction);
}
