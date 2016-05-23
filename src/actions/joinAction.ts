module Plywood {
  export class JoinAction extends Action {
    static fromJS(parameters: ActionJS): JoinAction {
      return new JoinAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("join");
      if(!this.expression.canHaveType('DATASET')) throw new TypeError('expression must be a DATASET');
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'DATASET');
      return 'DATASET';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      var typeContextParent = typeContext.parent;
      var expressionFullType = <DatasetFullType>this.expression._fillRefSubstitutions(typeContextParent, indexer, alterations);

      var inputDatasetType = typeContext.datasetType;
      var expressionDatasetType = expressionFullType.datasetType;
      var newDatasetType: Lookup<FullType> = Object.create(null);

      for (var k in inputDatasetType) {
        newDatasetType[k] = inputDatasetType[k];
      }
      for (var k in expressionDatasetType) {
        var ft = expressionDatasetType[k];
        if (hasOwnProperty(newDatasetType, k)) {
          if (newDatasetType[k].type !== ft.type) {
            throw new Error(`incompatible types of joins on ${k} between ${newDatasetType[k].type} and ${ft.type}`);
          }
        } else {
          newDatasetType[k] = ft;
        }
      }

      return {
        parent: typeContextParent,
        type: 'DATASET',
        datasetType: newDatasetType,
        remote: typeContext.remote || expressionFullType.remote
      };
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        return inV ? inV.join(expressionFn(d, c)) : inV;
      }
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      throw new Error('not possible');
    }

  }

  Action.register(JoinAction);
}
