module Plywood {
  export type CaseType = 'upperCase' | 'lowerCase';
  export class TransformCaseAction extends Action {
    static UPPER_CASE = 'upperCase';
    static LOWER_CASE = 'lowerCase';

    static fromJS(parameters: ActionJS): TransformCaseAction {
      var value = Action.jsToValue(parameters);
      value.transformType = parameters.transformType;
      return new TransformCaseAction(value);
    }

    public transformType: CaseType;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      var transformType = parameters.transformType;
      if (transformType !== TransformCaseAction.UPPER_CASE && transformType !== TransformCaseAction.LOWER_CASE) {
        throw new Error(`Must supply transform type of '${TransformCaseAction.UPPER_CASE}' or '${TransformCaseAction.LOWER_CASE}'`);
      }
      this.transformType = transformType;
      this._ensureAction("transformCase");
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.transformType = this.transformType;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.transformType = this.transformType;
      return js;
    }

    public equals(other: TransformCaseAction): boolean {
      return super.equals(other) &&
        this.transformType === other.transformType;
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'STRING');
      return 'STRING';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      return inputType;
    }

    protected _foldWithPrevAction(prevAction: Action): Action {
      if (prevAction instanceof TransformCaseAction) {
        return this;
      }
      return null;
    }

    protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
      const { transformType } = this;
      return (d: Datum, c: Datum) => {
        return transformType === TransformCaseAction.UPPER_CASE ? inputFn(d, c).toLocaleUpperCase() : inputFn(d, c).toLocaleLowerCase();
      }
    }

    protected _getJSHelper(inputType: PlyType, inputJS: string, expressionJS: string): string {
      const { transformType } = this;
      return transformType === TransformCaseAction.UPPER_CASE ? `${inputJS}.toLocaleUpperCase()` : `${inputJS}.toLocaleLowerCase()`;
    }

    protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string): string {
      const { transformType } = this;
      return transformType === TransformCaseAction.UPPER_CASE ? `UPPER(${inputSQL})` : `LOWER(${inputSQL})`;
    }
  }

  Action.register(TransformCaseAction);
}
