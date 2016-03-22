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

    public equals(other: LookupAction): boolean {
      return super.equals(other) &&
        this.lookup === other.lookup;
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [String(this.lookup)];
    }

    public getOutputType(inputType: PlyType): PlyType {
      return this._stringTransformOutputType(inputType);
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType): FullType {
      return inputType;
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
