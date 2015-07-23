module Plywood {
  export class SplitAction extends Action {
    static fromJS(parameters: ActionJS): SplitAction {
      var value = Action.jsToValue(parameters);
      value.name = parameters.name;
      value.dataName = parameters.dataName;
      return new SplitAction(value);
    }

    public name: string;
    public dataName: string;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.name = parameters.name;
      this.dataName = parameters.dataName;
      this._ensureAction("split");
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.name = this.name;
      value.dataName = this.dataName;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.name = this.name;
      js.dataName = this.dataName;
      return js;
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'DATASET');
      return 'DATASET';
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [expressionString, this.name, this.dataName];
    }

    public equals(other: SplitAction): boolean {
      return super.equals(other) &&
        this.name === other.name &&
        this.dataName === other.dataName;
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      var name = this.name;
      var dataName = this.dataName;
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        return inV ? inV.split(expressionFn, name, dataName) : null;
      }
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      var newDatasetType: Lookup<FullType> = {};
      var splitFullType = this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      newDatasetType[this.name] = splitFullType;
      newDatasetType[this.dataName] = typeContext;

      return {
        parent: typeContext.parent,
        type: 'DATASET',
        datasetType: newDatasetType,
        remote: splitFullType.remote
      };
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      throw `GROUP BY ${expressionSQL}`;
    }

    public contextDiff(): int {
      return 1;
    }
  }

  Action.register(SplitAction);
}
