module Plywood {
  export class AttachAction extends Action {
    static fromJS(parameters: ActionJS): AttachAction {
      var value = Action.jsToValue(parameters);
      value.selector = parameters.selector;
      value.prop = parameters.prop;
      return new AttachAction(value)
    }

    public selector: string;
    public prop: Lookup<any>;

    constructor(parameters: ActionValue = {}) {
      super(parameters, dummyObject);
      this.selector = parameters.selector;
      this.prop = parameters.prop;
      this._ensureAction("attach");
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.selector = this.selector;
      value.prop = this.prop;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.selector = this.selector;
      js.prop = this.prop;
      return js;
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'MARK');
      return 'MARK';
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      return {
        type: 'MARK',
        remote: typeContext.remote
      };
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [this.selector, '{}'];
    }

    public equals(other: AttachAction): boolean {
      return super.equals(other) &&
        this.selector === other.selector;
    }

    public getFn(inputFn: ComputeFn): ComputeFn {
      var selector = this.selector;
      var prop = this.prop;
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        return inV ? inV.attach(selector, prop) : null;
      }
    }
  }

  Action.register(AttachAction);
}
