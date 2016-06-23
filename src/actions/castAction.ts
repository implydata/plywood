module Plywood {

  interface Caster {
    TIME: (n: number) => Date;
    NUMBER: (d: Date) => number;
    [propName: string]: (v: number | Date) => number | Date;
  }

  const CAST_TYPE_TO_FN: Caster = {
    TIME: n => new Date(n*1000),
    NUMBER: (d) => Date.parse(d.toString()) / 1000
  };

  const CAST_TYPE_TO_JS: Lookup<(inputJS: string)=> string> = {
    TIME: (inputJS) => `new Date(${inputJS}*1000)`,
    NUMBER: (inputJS) => `${inputJS} / 1000` // we get the time in ms as argument in extractionFn
  };

  export class CastAction extends Action {
    static fromJS(parameters: ActionJS): CastAction {
      var value = Action.jsToValue(parameters);
      value.castType = parameters.castType;
      return new CastAction(value);
    }

    public castType: PlyType;

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
      var castType = this.castType;
      if (inputType) {
        if (!(
          (inputType === 'NUMBER' && castType === 'TIME') ||
          (inputType === 'TIME' && castType === 'NUMBER')
        )) {
          throw new TypeError(`cast action has a bad type combination ${inputType} CAST ${castType}`);
        }
      }
      return castType as PlyType;
    }

    public _fillRefSubstitutions(): FullType {
      var castType = this.castType;

      if (castType === 'TIME') {
        return {
          type: 'TIME',
        };
      } else if (castType === 'NUMBER') {
        return {
          type: 'NUMBER',
        };
      }

      throw new Error(`unrecognized cast type ${castType}`);
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
