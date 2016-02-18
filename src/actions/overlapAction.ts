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

    public getOutputType(inputType: string): string {
      var expressionType = this.expression.type;
      if (expressionType && expressionType !== 'NULL') {
        this._checkInputTypes(inputType, expressionType);
      } else if (inputType && inputType !== 'NULL' && inputType.indexOf('SET/') !== 0) {
        throw new Error(`${this.action} must have input of type 'SET/*' (is ${inputType})`);
      }
      return 'BOOLEAN';
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        var exV = expressionFn(d, c);
        if (!inV || !exV) return null;
        return inV.overlap(exV);
      }
    }

    //protected _getJSHelper(inputJS: string, expressionJS: string): string {
    //  return `(${inputJS}===${expressionJS})`;
    //}
    //
    //protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    //  return `(${inputSQL}=${expressionSQL})`;
    //}

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      if (literalExpression.equals(Expression.EMPTY_SET)) return Expression.FALSE;

      var { expression } = this;
      if (!expression.isOp('literal')) return expression.overlap(literalExpression);

      return null;
    }

    protected _performOnRef(refExpression: RefExpression): Expression {
      if (this.expression.equals(Expression.EMPTY_SET)) return Expression.FALSE;
      return null;
    }

    protected _performOnChain(chainExpression: ChainExpression): Expression {
      if (this.expression.equals(Expression.EMPTY_SET)) return Expression.FALSE;
      return null;
    }
  }

  Action.register(OverlapAction);
}
