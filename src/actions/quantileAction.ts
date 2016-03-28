module Plywood {
  export class QuantileAction extends Action {
    static fromJS(parameters: ActionJS): QuantileAction {
      var value = Action.jsToValue(parameters);
      value.quantile = parameters.quantile;
      return new QuantileAction(value);
    }

    public quantile: number;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.quantile = parameters.quantile;
      this._ensureAction("quantile");
      this._checkExpressionTypes('NUMBER');
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.quantile = this.quantile;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.quantile = this.quantile;
      return js;
    }

    public equals(other: QuantileAction): boolean {
      return super.equals(other) &&
        this.quantile === other.quantile;
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [expressionString, String(this.quantile)];
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'DATASET');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return {
        type: 'NUMBER'
      };
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      var quantile = this.quantile;
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        return inV ? inV.quantile(expressionFn, quantile, foldContext(d, c)) : null;
      }
    }

    public isAggregate(): boolean {
      return true;
    }

    public isNester(): boolean {
      return true;
    }
  }

  Action.register(QuantileAction);
}
