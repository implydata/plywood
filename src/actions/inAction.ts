module Plywood {

  export class InAction extends Action {
    static fromJS(parameters: ActionJS): InAction {
      return new InAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("in");
    }

    public getOutputType(inputType: string): string {
      var expression = this.expression;
      if (inputType) {
        if (!(
          (inputType.indexOf('SET/') === -1 && expression.canHaveType('SET')) ||
          (inputType === 'NUMBER' && expression.canHaveType('NUMBER_RANGE')) ||
          (inputType === 'TIME' && expression.canHaveType('TIME_RANGE'))
        )) {
          throw new TypeError(`in action has a bad type combination ${inputType} IN ${expression.type || '*'}`);
        }
      } else {
        if (!(expression.canHaveType('NUMBER_RANGE') || expression.canHaveType('TIME_RANGE') || expression.canHaveType('SET'))) {
          throw new TypeError(`in action has invalid expression type ${expression.type}`);
        }
      }
      return 'BOOLEAN';
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        var exV = expressionFn(d, c);
        if (!exV) return null;
        return (<any>exV).contains(inV);
      }
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      var expression = this.expression;
      var expressionType = expression.type;
      switch (expressionType) {
        case 'NUMBER_RANGE':
          if (expression instanceof LiteralExpression) {
            var numberRange: NumberRange = expression.value;
            return dialect.inExpression(inputSQL, dialect.numberToSQL(numberRange.start), dialect.numberToSQL(numberRange.end), numberRange.bounds);
          }
          throw new Error('not implemented yet');

        case 'TIME_RANGE':
          if (expression instanceof LiteralExpression) {
            var timeRange: TimeRange = expression.value;
            return dialect.inExpression(inputSQL, dialect.timeToSQL(timeRange.start), dialect.timeToSQL(timeRange.end), timeRange.bounds);
          }
          throw new Error('not implemented yet');

        case 'SET/STRING':
          return `${inputSQL} IN ${expressionSQL}`;

        default:
          throw new Error('not implemented yet');
      }
    }

    protected _nukeExpression(): Expression {
      var expression = this.expression;
      if (
        expression instanceof LiteralExpression &&
        expression.type.indexOf('SET/') === 0 &&
        expression.value.empty()
      ) return Expression.FALSE;
      return null;
    }

    private _performOnSimpleWhatever(ex: Expression): Expression {
      var expression = this.expression;
      var setValue: Set = expression.getLiteralValue();
      if (setValue && 'SET/' + ex.type === expression.type && setValue.size() === 1) {
        return new IsAction({ expression: r(setValue.elements[0]) }).performOnSimple(ex);
      }
      return null;
    }

    protected _performOnSimpleLiteral(literalExpression: LiteralExpression): Expression {
      return this._performOnSimpleWhatever(literalExpression);
    }

    protected _performOnSimpleRef(refExpression: RefExpression): Expression {
      return this._performOnSimpleWhatever(refExpression);
    }

    protected _performOnSimpleChain(chainExpression: ChainExpression): Expression {
      return this._performOnSimpleWhatever(chainExpression);
    }
  }

  Action.register(InAction);
}
