module Plywood {

  export class ApplyAction extends Action {
    static fromJS(parameters: ActionJS): ApplyAction {
      var value = Action.jsToValue(parameters);
      value.name = parameters.name;
      return new ApplyAction(value);
    }

    public name: string;

    constructor(parameters: ActionValue = {}) {
      super(parameters, dummyObject);
      this.name = parameters.name;
      this._ensureAction("apply");
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.name = this.name;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.name = this.name;
      return js;
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'DATASET');
      return 'DATASET';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      typeContext.datasetType[this.name] = this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return typeContext;
    }

    protected _toStringParameters(expressionString: string): string[] {
      var name = this.name;
      if (!RefExpression.SIMPLE_NAME_REGEXP.test(name)) name = JSON.stringify(name);
      return [name, expressionString];
    }

    public equals(other: ApplyAction): boolean {
      return super.equals(other) &&
        this.name === other.name;
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      var name = this.name;
      var type = this.expression.type;
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        return inV ? inV.apply(name, expressionFn, type, foldContext(d, c)) : null;
      }
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `${expressionSQL} AS ${dialect.escapeName(this.name)}`;
    }

    public isSimpleAggregate(): boolean {
      const { expression } = this;
      if (expression instanceof ChainExpression) {
        var actions = expression.actions;
        return actions.length === 1 && actions[0].isAggregate();
      }
      return false;
    }

    public isNester(): boolean {
      return true;
    }

    protected _removeAction(): boolean {
      const { name, expression } = this;
      if (expression instanceof RefExpression) {
        return expression.name === name && expression.nest === 0;
      }
      return false;
    }

    protected _putBeforeLastAction(lastAction: Action): Action {
      if (
        this.isSimpleAggregate() &&
        lastAction instanceof ApplyAction &&
        !lastAction.isSimpleAggregate() &&
        this.expression.getFreeReferences().indexOf(lastAction.name) === -1
      ) {
        return this;
      }
      return null;
    }

  }

  Action.register(ApplyAction);
}
