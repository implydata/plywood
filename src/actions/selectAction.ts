module Plywood {
  export class SelectAction extends Action {
    static fromJS(parameters: ActionJS): SelectAction {
      return new SelectAction({
        action: parameters.action,
        attributes: parameters.attributes
      });
    }

    public attributes: string[];

    constructor(parameters: ActionValue = {}) {
      super(parameters, dummyObject);
      this.attributes = parameters.attributes;
      this._ensureAction("select");
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.attributes = this.attributes;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.attributes = this.attributes;
      return js;
    }

    public equals(other: SelectAction): boolean {
      return super.equals(other) &&
        String(this.attributes) === String(other.attributes);
    }

    protected _toStringParameters(expressionString: string): string[] {
      return this.attributes;
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'DATASET');
      return 'DATASET';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      const { attributes } = this;
      var { datasetType } = typeContext;
      var newDatasetType = Object.create(null);
      for (var attr of attributes) {
        var attrType = datasetType[attr];
        if (!attrType) throw new Error(`unknown attribute '${attr}' in select`);
        newDatasetType[attr] = attrType;
      }
      typeContext.datasetType = newDatasetType;
      return typeContext;
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      const { attributes } = this;
      return (d: Datum, c: Datum) => {
        var inV: Dataset = inputFn(d, c);
        return inV ? inV.select(attributes) : null;
      }
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      throw new Error('can not be expressed as SQL directly')
    }

    protected _foldWithPrevAction(prevAction: Action): Action {
      var { attributes } = this;
      if (prevAction instanceof SelectAction) {
        return new SelectAction({
          attributes: prevAction.attributes.filter(a => attributes.indexOf(a) !== -1)
        });
      } else if (prevAction instanceof ApplyAction) {
        if (attributes.indexOf(prevAction.name) === -1) {
          return this;
        }
      }

      return null;
    }

  }

  Action.register(SelectAction);
}
