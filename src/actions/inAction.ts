module Plywood {
  /*
  function makeInOrIs(lhs: Expression, value: any): Expression {
    var literal = new LiteralExpression({
      op: 'literal',
      value: value
    });

    var literalType = literal.type;
    var returnExpression: Expression = null;
    if (literalType === 'NUMBER_RANGE' || literalType === 'TIME_RANGE' || literalType.indexOf('SET/') === 0) {
      returnExpression = new InExpression({ op: 'in', lhs: lhs, rhs: literal });
    } else {
      returnExpression = new IsExpression({ op: 'is', lhs: lhs, rhs: literal });
    }
    return returnExpression.simplify();
  }
  */

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
        if (!(expression.canHaveType('SET')
          || (inputType === 'NUMBER' && expression.canHaveType('NUMBER_RANGE'))
          || (inputType === 'TIME' && expression.canHaveType('TIME_RANGE')))) {
          throw new TypeError(`in action has a bad type combination ${inputType} in ${expression.type}`);
        }
      } else {
        if (!(expression.canHaveType('NUMBER_RANGE') || expression.canHaveType('TIME_RANGE') || expression.canHaveType('SET'))) {
          throw new TypeError(`in action has invalid expression  type ${expression.type}`);
        }
      }
      return 'BOOLEAN';
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      var expressionType = this.expression.type;
      if (expressionType === 'SET/NUMBER_RANGE' || expressionType === 'SET/TIME_RANGE') {
        return (d: Datum, c: Datum) => {
          var inV = inputFn(d, c);
          var exV = expressionFn(d, c);
          if (inV instanceof NumberRange || inV instanceof TimeRange) {
            return (<Set>exV).contains(inV);
          } else {
            return (<Set>exV).containsWithin(inV);
          }
        }
      } else {
        // Time range and set also have contains
        return (d: Datum, c: Datum) => {
          var inV = inputFn(d, c);
          var exV = expressionFn(d, c);
          return (<NumberRange>exV).contains(inV);
        }
      }
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      var expression = this.expression;
      var expressionType = expression.type;
      switch (expressionType) {
        case 'NUMBER_RANGE':
          if (expression instanceof LiteralExpression) {
            var numberRange: NumberRange = expression.value;
            return dialect.inExpression(inputSQL, numberToSQL(numberRange.start), numberToSQL(numberRange.end), numberRange.bounds);
          }
          throw new Error('not implemented yet');

        case 'TIME_RANGE':
          if (expression instanceof LiteralExpression) {
            var timeRange: TimeRange = expression.value;
            return dialect.inExpression(inputSQL, timeToSQL(timeRange.start), timeToSQL(timeRange.end), timeRange.bounds);
          }
          throw new Error('not implemented yet');

        case 'SET/STRING':
          return `${inputSQL} IN ${expressionSQL}`;

        default:
          throw new Error('not implemented yet');
      }
    }

    protected _specialSimplify(simpleExpression: Expression): ActionSimplification {
      if (
        simpleExpression instanceof LiteralExpression &&
        simpleExpression.type.indexOf('SET/') === 0 &&
        simpleExpression.value.empty()
      ) {
        return {
          simplification: Simplification.Wipe,
          expression: Expression.FALSE
        };
      }
      return null;
    }

    /*
    public mergeAnd(ex: Expression): Expression {
      if (ex.isOp('literal')) return ex.mergeAnd(this);

      if (!this.checkLefthandedness()) return null;
      if (!arraysEqual(this.getFreeReferences(), ex.getFreeReferences())) return null;

      if (ex instanceof IsExpression || ex instanceof InExpression) {
        if (!ex.checkLefthandedness()) return null;

        var intersect = Set.generalIntersect((<LiteralExpression>this.expression).value, (<LiteralExpression>ex.rhs).value);
        if (intersect === null) return null;

        return makeInOrIs(this.lhs, intersect);
      }
      return null;
    }

    public mergeOr(ex: Expression): Expression {
      if (ex.isOp('literal')) return ex.mergeOr(this);

      if (!this.checkLefthandedness()) return null;
      if (!arraysEqual(this.getFreeReferences(), ex.getFreeReferences())) return null;

      if (ex instanceof IsExpression || ex instanceof InExpression) {
        if (!ex.checkLefthandedness()) return null;

        var intersect = Set.generalUnion((<LiteralExpression>this.rhs).value, (<LiteralExpression>ex.rhs).value);
        if (intersect === null) return null;

        return makeInOrIs(this.lhs, intersect);
      }
      return null;
    }
    */
  }

  Action.register(InAction);
}
