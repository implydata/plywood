module Plywood {
  export class ExtractAction extends Action {
    static fromJS(parameters: ActionJS): ExtractAction {
      var value = Action.jsToValue(parameters);
      value.regexp = parameters.regexp;
      return new ExtractAction(value);
    }

    public regexp: string;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.regexp = parameters.regexp;
      this._ensureAction("extract");
    }

    public getOutputType(inputType: PlyType): PlyType {
      return this._stringTransformOutputType(inputType);
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.regexp = this.regexp;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.regexp = this.regexp;
      return js;
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [this.regexp];
    }

    public equals(other: MatchAction): boolean {
      return super.equals(other) &&
        this.regexp === other.regexp;
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      var re = new RegExp(this.regexp);
      return (d: Datum, c: Datum) => {
        return (String(inputFn(d, c)).match(re) || [])[1] || null;
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return `((''+${inputJS}).match(/${this.regexp}/) || [])[1] || null`;
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      throw new Error('not implemented yet'); // https://github.com/mysqludf/lib_mysqludf_preg
    }
  }

  Action.register(ExtractAction);
}
