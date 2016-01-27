module Plywood {
  export class LookupAction extends Action {
    static fromJS(parameters: ActionJS): LookupAction {
      var value = Action.jsToValue(parameters);
      value.lookup = parameters.lookup;
      return new LookupAction(value);
    }

    public lookup: string;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.lookup = parameters.lookup;
      this._ensureAction("lookup");
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'STRING');
      return 'STRING';
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.lookup = this.lookup;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.lookup = this.lookup;
      return js;
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [String(this.lookup)];
    }

    public equals(other: LookupAction): boolean {
      return super.equals(other) &&
        this.lookup === other.lookup;
    }

    public fullyDefined(): boolean {
      return false;
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      throw new Error('can not express as JS');
    }

    protected _getJSHelper(inputJS: string): string {
      throw new Error('can not express as JS');
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      throw new Error('can not express as SQL');
    }
  }

  Action.register(LookupAction);
}
