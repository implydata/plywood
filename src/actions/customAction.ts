module Plywood {
  export class CustomAction extends Action {
    static fromJS(parameters: ActionJS): CustomAction {
      var value = Action.jsToValue(parameters);
      value.custom = parameters.custom;
      return new CustomAction(value);
    }

    public custom: string;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.custom = parameters.custom;
      this._ensureAction("custom");
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.custom = this.custom;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.custom = this.custom;
      return js;
    }

    public getOutputType(inputType: string): string {
      this._checkInputTypes(inputType, 'DATASET');
      return 'NUMBER';
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      return {
        type: 'NUMBER',
        remote: typeContext.remote
      };
    }

    public getFn(inputFn: ComputeFn): ComputeFn {
      throw new Error('can not getFn on custom action');
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      throw new Error('custom action not implemented');
    }

    public equals(other: CustomAction): boolean {
      return super.equals(other) &&
        this.custom === other.custom;
    }

    public isAggregate(): boolean {
      return true;
    }
  }

  Action.register(CustomAction);
}
