module Plywood {
  export class MatchAction extends Action {
    static fromJS(parameters: ActionJS): MatchAction {
      var value = Action.jsToValue(parameters);
      value.regexp = parameters.regexp;
      return new MatchAction(value);
    }

    public regexp: string;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.regexp = parameters.regexp;
      this._ensureAction("match");
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

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'STRING');
      return 'BOOLEAN';
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      return {
        type: 'BOOLEAN',
        remote: typeContext.remote
      };
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
        return re.test(inputFn(d, c));
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      return `/${this.regexp}/.test(${inputJS})`;
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `${inputSQL} REGEXP '${this.regexp}'`; // ToDo: escape this.regexp
    }
  }

  Action.register(MatchAction);
}
