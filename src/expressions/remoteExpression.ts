module Plywood {
  export class RemoteExpression extends Expression {
    static fromJS(parameters: ExpressionJS): Expression {
      var value: ExpressionValue = {
        op: parameters.op
      };
      value.dataset = <RemoteDataset>RemoteDataset.fromJS(parameters.dataset);
      return new RemoteExpression(value);
    }

    public dataset: RemoteDataset;

    constructor(parameters: ExpressionValue) {
      super(parameters, dummyObject);
      this.dataset = parameters.value;
      this._ensureOp('remote');
      this.type = 'DATASET';
      this.simple = true;
    }

    public valueOf(): ExpressionValue {
      var value = super.valueOf();
      value.value = this.dataset;
      return value;
    }

    public toJS(): ExpressionJS {
      var js = super.toJS();
      js.dataset = this.dataset.toJS();
      return js;
    }

    public toString(): string {
      return `Remote:${this.dataset.toString()}`;
    }

    public getFn(): ComputeFn {
      var value = this.dataset;

      var hasSimulated = false;
      var simulatedValue: any;
      return (d: Datum, c: Datum) => {
        if (!hasSimulated) {
          simulatedQueries.push(value.getQueryAndPostProcess().query);
          simulatedValue = value.simulate();
          hasSimulated = true;
        }
        return simulatedValue;
      };
    }

    public equals(other: RemoteExpression): boolean {
      return super.equals(other) &&
        this.dataset.equals(other.dataset);
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      indexer.index++;
      var newTypeContext = (<Dataset>this.dataset).getFullType();
      newTypeContext.parent = typeContext;
      return newTypeContext;
    }

    public _computeResolved(): Q.Promise<any> {
      return this.dataset.queryValues();
    }

  }

  Expression.register(RemoteExpression);
}
