module Plywood {
  export class RemoteExpression extends Expression {
    static fromJS(parameters: ExpressionJS): Expression {
      var value: ExpressionValue = {
        op: parameters.op
      };
      value.external = External.fromJS(parameters.external);
      return new RemoteExpression(value);
    }

    public external: External;

    constructor(parameters: ExpressionValue) {
      super(parameters, dummyObject);
      this.external = parameters.external;
      this._ensureOp('remote');
      this.type = 'DATASET';
      this.simple = true;
    }

    public valueOf(): ExpressionValue {
      var value = super.valueOf();
      value.value = this.external;
      return value;
    }

    public toJS(): ExpressionJS {
      var js = super.toJS();
      js.external = this.external.toJS();
      return js;
    }

    public toString(): string {
      return `R:${this.external.toString()}`;
    }

    public getFn(): ComputeFn {
      var external = this.external;

      var hasSimulated = false;
      var simulatedValue: any;
      return (d: Datum, c: Datum) => {
        if (!hasSimulated) {
          simulatedQueries.push(external.getQueryAndPostProcess().query);
          simulatedValue = external.simulate();
          hasSimulated = true;
        }
        return simulatedValue;
      };
    }

    public equals(other: RemoteExpression): boolean {
      return super.equals(other) &&
        this.external.equals(other.external);
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      indexer.index++;
      var newTypeContext = this.external.getFullType();
      newTypeContext.parent = typeContext;
      return newTypeContext;
    }

    public _computeResolved(): Q.Promise<any> {
      return this.external.queryValues();
    }

  }

  Expression.register(RemoteExpression);
}
