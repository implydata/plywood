module Plywood {
  export class ContainsAction extends Action {
    static NORMAL = 'normal';
    static IGNORE_CASE = 'ignoreCase';

    static fromJS(parameters: ActionJS): ContainsAction {
      var value = Action.jsToValue(parameters);
      value.compare = parameters.compare;
      return new ContainsAction(value);
    }

    public compare: string;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      var { compare } = parameters;
      if (!compare) {
        compare = ContainsAction.NORMAL;
      } else if (compare !== ContainsAction.NORMAL && compare !== ContainsAction.IGNORE_CASE) {
        throw new Error(`compare must be '${ContainsAction.NORMAL}' or '${ContainsAction.IGNORE_CASE}'`);
      }
      this.compare = compare;
      this._ensureAction("contains");
      this._checkExpressionTypes('STRING');
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.compare = this.compare;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.compare = this.compare;
      return js;
    }

    public equals(other: ContainsAction): boolean {
      return super.equals(other) &&
        this.compare === other.compare;
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [expressionString, this.compare];
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'STRING', 'SET/STRING');
      return 'BOOLEAN';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      this.expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return inputType;
    }

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      if (this.compare === ContainsAction.NORMAL) {
        return (d: Datum, c: Datum) => {
          return String(inputFn(d, c)).indexOf(expressionFn(d, c)) > -1;
        }
      } else {
        return (d: Datum, c: Datum) => {
          return String(inputFn(d, c)).toLowerCase().indexOf(String(expressionFn(d, c)).toLowerCase()) > -1;
        }
      }
    }

    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      var combine: (lhs: string, rhs: string) => string;
      if (this.compare === ContainsAction.NORMAL) {
        combine = (lhs, rhs) => `(''+${lhs}).indexOf(${rhs})>-1`;
      } else {
        combine = (lhs, rhs) => `(''+${lhs}).toLowerCase().indexOf((''+${rhs}).toLowerCase())>-1`;
      }
      return Expression.jsNullSafety(inputJS, expressionJS, combine, inputJS[0] === '"', expressionJS[0] === '"');
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      if (this.compare === ContainsAction.IGNORE_CASE) {
        expressionSQL = `LOWER(${expressionSQL})`;
        inputSQL = `LOWER(${inputSQL})`;
      }
      return dialect.containsExpression(expressionSQL, inputSQL);
    }
  }

  Action.register(ContainsAction);
}
