module Plywood {

  interface Caster {
    (d: number): Date;
  }

  const CAST_TYPE_TO_FN: Lookup<Caster> = {
    TIME: d => new Date(d*1000)
  };

  const CAST_TYPE_TO_JS: Lookup<(inputJS: string)=> string> = {
    TIME: (inputJS) => `new Date(${inputJS}*1000)`
  };

  export class CastAction extends Action {
    static fromJS(parameters: ActionJS): CastAction {
      var value = Action.jsToValue(parameters);
      value.castType = parameters.castType;
      return new CastAction(value);
    }

    public castType: string;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.castType = parameters.castType;
      this._ensureAction("cast");
      if (typeof this.castType !== 'string') {
        throw new Error("`castType` must be a string");
      }
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.castType = this.castType;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.castType = this.castType;
      return js;
    }

    public equals(other: CastAction): boolean {
      return super.equals(other) &&
        this.castType === other.castType;
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [this.castType];
    }


    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'NUMBER');
      return 'TIME';
    }

    public _fillRefSubstitutions(): FullType {
      return {
        type: 'TIME',
      };
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      const { castType } = this;
      var caster = CAST_TYPE_TO_FN[castType];
      if (!caster) throw new Error(`unsupported cast type '${castType}'`);
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (!inV) return null;
        return caster(inV);
      }
    }

    protected _getJSHelper(inputJS: string): string {
      const { castType } = this;
      var castJS = CAST_TYPE_TO_JS[castType];
      if (!castJS) throw new Error(`unsupported cast type '${castType}'`);
      return castJS(inputJS);
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return dialect.castExpression(inputSQL, this.castType);
    }
  }

  Action.register(CastAction);
}
