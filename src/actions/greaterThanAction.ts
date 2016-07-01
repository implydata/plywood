module Plywood {
  export class GreaterThanAction extends Action {
    static fromJS(parameters: ActionJS): GreaterThanAction {
      return new GreaterThanAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("greaterThan");
      this._checkExpressionTypes('NUMBER', 'TIME');
    }

    public getOutputType(inputType: PlyType): PlyType {
      var expressionType = this.expression.type;
      if (expressionType) this._checkInputTypes(inputType, expressionType);
      return 'BOOLEAN';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return {
        type: 'BOOLEAN'
      };
    }

    protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        return inputFn(d, c) > expressionFn(d, c);
      }
    }

    protected _getJSHelper(inputType: PlyType, inputJS: string, expressionJS: string): string {
      return `(${inputJS}>${expressionJS})`;
    }

    protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return `(${inputSQL}>${expressionSQL})`;
    }

    protected _specialSimplify(simpleExpression: Expression): Action {
      if (simpleExpression instanceof LiteralExpression) { // x > 5
        return new InAction({
          expression: new LiteralExpression({
            value: Range.fromJS({ start: simpleExpression.value, end: null, bounds: '()' })
          })
        });
      }
      return null;
    }

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      // 5 > x
      return (new InAction({
        expression: new LiteralExpression({
          value: Range.fromJS({ start: null, end: literalExpression.value, bounds: '()' })
        })
      })).performOnSimple(this.expression);
    }
  }

  Action.register(GreaterThanAction);
}
