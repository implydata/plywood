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

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'BOOLEAN', 'STRING');
      return 'BOOLEAN';
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [expressionString, this.compare];
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
      if (this.compare === ContainsAction.NORMAL) {
        return `(''+${inputJS}).indexOf(${expressionJS})>-1`;
      } else {
        return `(''+${inputJS}).toLowerCase().indexOf(String(${expressionJS}).toLowerCase())>-1`;
      }
    }

    public getSQL(inputSQL: string, dialect: SQLDialect): string {
      var expression = this.expression;
      if (expression instanceof LiteralExpression) {
        return `${inputSQL} LIKE "%${expression.value}%"`;
      } else {
        throw new Error(`can not express ${this} in SQL`);
      }
    }
  }

  Action.register(ContainsAction);
}
