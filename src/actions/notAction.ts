module Plywood {
  export class NotAction extends Action {
    static fromJS(parameters: ActionJS): NotAction {
      return new NotAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("not");
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'BOOLEAN');
      return 'BOOLEAN';
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        return !inputFn(d, c);
      }
    }

    protected _getJSHelper(inputJS: string): string {
      return "!(" + inputJS + ")"
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return 'NOT(' + inputSQL  + ')';
    }

    /*
    protected _specialSimplify(simpleOperand: Expression): Expression {
      if (simpleOperand instanceof NotExpression) {
        return simpleOperand.operand;
      }
      return null;
    }
    */
  }

  Action.register(NotAction);
}
