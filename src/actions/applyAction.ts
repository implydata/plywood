module Plywood {
  export class ApplyAction extends Action {
    static fromJS(parameters: ActionJS): ApplyAction {
      var value = Action.jsToValue(parameters);
      value.name = parameters.name;
      return new ApplyAction(value);
    }

    public name: string;

    constructor(parameters: ActionValue = {}) {
      super(parameters, dummyObject);
      this.name = parameters.name;
      this._ensureAction("apply");
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.name = this.name;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.name = this.name;
      return js;
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'DATASET');
      return 'DATASET';
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [this.name, expressionString];
    }

    public equals(other: ApplyAction): boolean {
      return super.equals(other) &&
        this.name === other.name;
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      typeContext.datasetType[this.name] = this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return typeContext;
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      var name = this.name;
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        return inV ? inV.apply(name, expressionFn, foldContext(d, c)) : null;
      }
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `${expressionSQL} AS '${this.name}'`;
    }
  }

  Action.register(ApplyAction);
}
