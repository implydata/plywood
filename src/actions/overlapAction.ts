module Plywood {
  export class OverlapAction extends Action {
    static fromJS(parameters: ActionJS): OverlapAction {
      return new OverlapAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("overlap");
      if (!this.expression.canHaveType('SET')) {
        throw new Error(`${this.action} must have an expression of type SET (is: ${this.expression.type})`);
      }
    }

    public getOutputType(inputType: PlyType): PlyType {
      var expressionType = this.expression.type;
      if (expressionType && expressionType !== 'NULL' && expressionType !== 'SET/NULL' && inputType && inputType !== 'NULL') {
        var setInputType = wrapSetType(inputType);
        var setExpressionType = wrapSetType(expressionType);
        if (setInputType !== setExpressionType) {
          throw new Error(`type mismatch in overlap action: ${inputType} is incompatible with ${expressionType}`);
        }
      }
      return 'BOOLEAN';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return {
        type: 'BOOLEAN'
      };
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        var exV = expressionFn(d, c);
        if (exV == null) return null;
        return Set.isSet(inV) ? inV.overlap(exV) : exV.contains(inV);
      }
    }

    //protected _getJSHelper(inputJS: string, expressionJS: string): string {
    //  return `(${inputJS}===${expressionJS})`;
    //}
    //
    //protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    //  return `(${inputSQL}=${expressionSQL})`;
    //}

    protected _nukeExpression(): Expression {
      if (this.expression.equals(Expression.EMPTY_SET)) return Expression.FALSE;
      return null;
    }

    private _performOnSimpleWhatever(ex: Expression): Expression {
      var expression = this.expression;
      if ('SET/' + ex.type === expression.type) {
        return new InAction({ expression }).performOnSimple(ex);
      }
      return null;
    }

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      var { expression } = this;
      if (!expression.isOp('literal')) return new OverlapAction({ expression: literalExpression }).performOnSimple(expression);

      return this._performOnSimpleWhatever(literalExpression);
    }

    protected _performOnRef(refExpression: RefExpression): Expression {
      return this._performOnSimpleWhatever(refExpression);
    }

    protected _performOnSimpleChain(chainExpression: ChainExpression): Expression {
      return this._performOnSimpleWhatever(chainExpression);
    }
  }

  Action.register(OverlapAction);
}
