module Plywood {
  export class PowerAction extends Action {
    static fromJS(parameters: ActionJS): PowerAction {
      return new PowerAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("power");
      this._checkExpressionTypes('NUMBER');
    }

    public getOutputType(inputType: string): string {
      this._checkInputTypes(inputType, 'NUMBER');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return {
        type: 'NUMBER',
        remote: typeContext.remote
      };
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        return Math.pow((inputFn(d, c) || 0), (expressionFn(d, c) || 0));
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return `Math.pow(${inputJS},${expressionJS})`;
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `POW(${inputSQL},${expressionSQL})`;
    }

    protected _removeAction(): boolean {
      return this.expression.equals(Expression.ONE);
    }

    protected _performOnRef(simpleExpression: RefExpression): Expression {
      if (this.expression.equals(Expression.ZERO)) return simpleExpression;
      return null;
    }

  }

  Action.register(PowerAction);
}
