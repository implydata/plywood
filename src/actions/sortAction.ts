module Plywood {
  export class SortAction extends Action {
    static fromJS(parameters: ActionJS): SortAction {
      return new SortAction({
        action: parameters.action,
        expression: Expression.fromJS(parameters.expression),
        direction: parameters.direction
      });
    }

    public direction: string;

    constructor(parameters: ActionValue = {}) {
      super(parameters, dummyObject);
      this.direction = parameters.direction;
      this._ensureAction("sort");
      if (this.direction !== 'descending' && this.direction !== 'ascending') {
        throw new Error("direction must be 'descending' or 'ascending'");
      }
      if (!this.expression.isOp('ref')) {
        throw new Error("must be a reference expression (for now): " + this.toString());
      }
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.direction = this.direction;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.direction = this.direction;
      return js;
    }

    public getOutputType(inputType: string): string {
      this._checkInputType(inputType, 'DATASET');
      return 'DATASET';
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [expressionString, this.direction];
    }

    public equals(other: SortAction): boolean {
      return super.equals(other) &&
        this.direction === other.direction;
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      var direction = this.direction;
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        return inV ? inV.sort(expressionFn, direction) : null;
      }
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      var dir = this.direction === 'descending' ? 'DESC' : 'ASC';
      return `ORDER BY ${expressionSQL} ${dir}`;
    }

    public refName(): string {
      var expression = this.expression;
      return (expression instanceof RefExpression) ? expression.name : null;
    }
  }

  Action.register(SortAction);
}
