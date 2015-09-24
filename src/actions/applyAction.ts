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

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'DATASET');
      return 'DATASET';
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [this.name, expressionString];
    }

    public equals(other: ApplyAction): boolean {
      return super.equals(other) &&
        this.name === other.name;
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      typeContext.datasetType[this.name] = this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return typeContext;
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      var name = this.name;
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        return inV ? inV.apply(name, expressionFn, foldContext(d, c)) : null;
      }
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `${expressionSQL} AS '${this.name}'`;
    }

    public isSimpleAggregate(): boolean {
      var expression = this.expression;
      if (expression instanceof ChainExpression) {
        var actions = expression.actions;
        return actions.length === 1 && actions[0].isAggregate();
      }
      return false;
    }

    public isNester(): boolean {
      return true;
    }

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      var dataset: Dataset = literalExpression.value;
      var myExpression = this.expression;
      if (dataset.basis()) {
        if (myExpression instanceof ExternalExpression) {
          var newTotalExpression = myExpression.makeTotal(this.name);
          if (newTotalExpression) return newTotalExpression;
        } else {
          var externals = myExpression.getExternals();
          if (externals.length === 1) {
            var newExternal = externals[0].makeTotal('main'); // ToDo: this 'main' is a hack
            if (!newExternal) return null;
            return this.performOnSimple(new ExternalExpression({
              external: newExternal
            }));
          } else if (externals.length > 1) {
            throw new Error('not done yet');
          }
        }
      }
      return null;
    }

    protected _performOnChain(chainExpression: ChainExpression): Expression {
      if (!this.isSimpleAggregate()) return null;
      var actions = chainExpression.actions;
      var i = actions.length;
      while (i > 0) {
        let action = actions[i - 1];
        if (action.action !== 'apply') break;
        if ((<ApplyAction>action).isSimpleAggregate()) break;
        i--;
      }
      actions = actions.slice();
      actions.splice(i, 0, this);
      return new ChainExpression({
        expression: chainExpression.expression,
        actions: actions,
        simple: true
      })
    }
  }

  Action.register(ApplyAction);
}
