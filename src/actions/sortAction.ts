module Plywood {
  export class SortAction extends Action {
    static DESCENDING = 'descending';
    static ASCENDING = 'ascending';

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
      var { direction } = parameters;
      if (direction !== SortAction.DESCENDING && direction !== SortAction.ASCENDING) {
        throw new Error(`direction must be '${SortAction.DESCENDING}' or '${SortAction.ASCENDING}'`);
      }
      this.direction = direction;
      if (!this.expression.isOp('ref')) {
        throw new Error("must be a reference expression (for now): " + this.toString());
      }
      this._ensureAction("sort");
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
      var dir = this.direction === SortAction.DESCENDING ? 'DESC' : 'ASC';
      return `ORDER BY ${expressionSQL} ${dir}`;
    }

    public refName(): string {
      var expression = this.expression;
      return (expression instanceof RefExpression) ? expression.name : null;
    }

    public isNester(): boolean {
      return true;
    }

    protected _foldWithPrevAction(prevAction: Action): Action {
      if (prevAction instanceof SortAction && this.expression.equals(prevAction.expression)) {
        return this;
      }
      return null;
    }

    public toggleDirection(): SortAction {
      return new SortAction({
        expression: this.expression,
        direction: this.direction === SortAction.ASCENDING ? SortAction.DESCENDING : SortAction.ASCENDING
      });
    }
  }

  Action.register(SortAction);
}
