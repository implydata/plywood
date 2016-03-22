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
      if (this.compare === ContainsAction.NORMAL) {
        return `(''+${inputJS}).indexOf(${expressionJS})>-1`;
      } else {
        if (this.expression.isOp('literal')) {
          return `(''+${inputJS}).toLowerCase().indexOf(${expressionJS.toLowerCase()})>-1`;
        } else {
          return `(''+${inputJS}).toLowerCase().indexOf((''+${expressionJS}).toLowerCase())>-1`;
        }
      }
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      if (this.compare === ContainsAction.IGNORE_CASE) {
        expressionSQL = `LOWER(${expressionSQL})`;
        inputSQL = `LOWER(${inputSQL})`;
      }
      return `LOCATE(${expressionSQL},${inputSQL})>0`;
    }
  }

  Action.register(ContainsAction);
}
