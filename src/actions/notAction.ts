module Plywood {
  export class NotAction extends Action {
    static fromJS(parameters: ActionJS): NotAction {
      return new NotAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("not");
      this._checkNoExpression();
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'BOOLEAN');
      return 'BOOLEAN';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType): FullType {
      return inputType;
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        return !inputFn(d, c);
      }
    }

    protected _getJSHelper(inputJS: string): string {
      return `!(${inputJS})`;
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `NOT(${inputSQL})`;
    }

    protected _foldWithPrevAction(prevAction: Action): Action {
      if (prevAction instanceof NotAction) {
        return new AndAction({ expression: Expression.TRUE }); // Boolean noop
      }
      return null;
    }
  }

  Action.register(NotAction);
}
