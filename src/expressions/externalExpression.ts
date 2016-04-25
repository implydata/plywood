module Plywood {
  export class ExternalExpression extends Expression {
    static fromJS(parameters: ExpressionJS): Expression {
      var value: ExpressionValue = {
        op: parameters.op
      };
      value.external = External.fromJS(parameters.external);
      return new ExternalExpression(value);
    }

    public external: External;

    constructor(parameters: ExpressionValue) {
      super(parameters, dummyObject);
      var external = parameters.external;
      if (!external) throw new Error('must have an external');
      this.external = external;
      this._ensureOp('external');
      this.type = external.mode === 'value' ? 'NUMBER' : 'DATASET'; // ToDo: not always number
      this.simple = true;
    }

    public valueOf(): ExpressionValue {
      var value = super.valueOf();
      value.external = this.external;
      return value;
    }

    public toJS(): ExpressionJS {
      var js = super.toJS();
      js.external = this.external.toJS();
      return js;
    }

    public toString(): string {
      return `E:${this.external}`;
    }

    public getFn(): ComputeFn {
      throw new Error('should not call getFn on External');
    }

    public equals(other: ExternalExpression): boolean {
      return super.equals(other) &&
        this.external.equals(other.external);
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, indexer: Indexer, alterations: Alterations): FullType {
      indexer.index++;
      const { external } = this;
      if (external.mode === 'value') {
        return { type: 'NUMBER' };
      } else {
        var newTypeContext = this.external.getFullType();
        newTypeContext.parent = typeContext;
        return newTypeContext;
      }
    }

    public _computeResolvedSimulate(lastNode: boolean, simulatedQueries: any[]): PlywoodValue {
      var external = this.external;
      if (external.suppress) return external;
      return external.simulateValue(lastNode, simulatedQueries);
    }

    public _computeResolved(lastNode: boolean): Q.Promise<PlywoodValue> {
      var external = this.external;
      if (external.suppress) return Q(external);
      return external.queryValue(lastNode);
    }

    public unsuppress(): ExternalExpression {
      var value = this.valueOf();
      value.external = this.external.show();
      return new ExternalExpression(value);
    }

    public addAction(action: Action): ExternalExpression {
      var newExternal = this.external.addAction(action);
      if (!newExternal) return null;
      return new ExternalExpression({ external: newExternal });
    }

    public maxPossibleSplitValues(): number {
      return Infinity;
    }
  }

  Expression.register(ExternalExpression);
}
