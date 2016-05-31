module Plywood {

  export class InAction extends Action {
    static fromJS(parameters: ActionJS): InAction {
      return new InAction(Action.jsToValue(parameters));
    }

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this._ensureAction("in");
    }

    public getOutputType(inputType: PlyType): PlyType {
      var expression = this.expression;
      if (inputType) {
        if (!(
          (!isSetType(inputType) && expression.canHaveType('SET')) ||
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
        if (!exV) return null;
        return (<any>exV).contains(inV);
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      const { expression } = this;
      if (expression instanceof LiteralExpression) {
        switch (expression.type) {
          case 'NUMBER_RANGE':
          case 'TIME_RANGE':
            var range: PlywoodRange = expression.value;
            var r0 = range.start;
            var r1 = range.end;
            var bounds = range.bounds;

            var cmpStrings: string[] = [];
            if (r0 != null) {
              cmpStrings.push(`${JSON.stringify(r0)} ${bounds[0] === '(' ? '<' : '<='} _`);
            }
            if (r1 != null) {
              cmpStrings.push(`_ ${bounds[1] === ')' ? '<' : '<='} ${JSON.stringify(r1)}`);
            }

            return `(_=${inputJS}, ${cmpStrings.join(' && ')})`;

          case 'SET/STRING':
            var valueSet: Set = expression.value;
            return `${JSON.stringify(valueSet.elements)}.indexOf(${inputJS})>-1`;

          default:
            throw new Error(`can not convert ${this} to JS function, unsupported type ${expression.type}`);
        }
      }

      throw new Error(`can not convert ${this} to JS function`);
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      var expression = this.expression;
      var expressionType = expression.type;
      switch (expressionType) {
        case 'NUMBER_RANGE':
        case 'TIME_RANGE':
          if (expression instanceof LiteralExpression) {
            var range: PlywoodRange = expression.value;
            return dialect.inExpression(inputSQL, dialect.numberOrTimeToSQL(range.start), dialect.numberOrTimeToSQL(range.end), range.bounds);
          }
          throw new Error(`can not convert action to SQL ${this}`);

        case 'SET/STRING':
        case 'SET/NUMBER':
          return `${inputSQL} IN ${expressionSQL}`;

        case 'SET/NUMBER_RANGE':
        case 'SET/TIME_RANGE':
          if (expression instanceof LiteralExpression) {
            var setOfRange: Set = expression.value;
            return setOfRange.elements.map((range: PlywoodRange) => {
              return dialect.inExpression(inputSQL, dialect.numberOrTimeToSQL(range.start), dialect.numberOrTimeToSQL(range.end), range.bounds);
            }).join(' OR ');
          }
          throw new Error(`can not convert action to SQL ${this}`);

        default:
          throw new Error(`can not convert action to SQL ${this}`);
      }
    }

    protected _nukeExpression(): Expression {
      var expression = this.expression;
      if (
        expression instanceof LiteralExpression &&
        isSetType(expression.type) &&
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

    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      return this._performOnSimpleWhatever(literalExpression);
    }

    protected _performOnRef(refExpression: RefExpression): Expression {
      return this._performOnSimpleWhatever(refExpression);
    }

    protected _performOnSimpleChain(chainExpression: ChainExpression): Expression {
      return this._performOnSimpleWhatever(chainExpression);
    }
  }

  Action.register(InAction);
}
