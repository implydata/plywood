module Plywood {

  interface Caster {
    TIME: {
      NUMBER: (n: number) => Date
    };
    NUMBER: {
      TIME: (d: Date) => number;
      STRING: (s: string) => number;
    };
    STRING: {
      NUMBER: (n: number) => String
    }
    [propName: string]: any;
  }

  const CAST_TYPE_TO_FN: Caster = {
    TIME: {
      NUMBER: n => new Date(n*1000)
    },
    NUMBER: {
      TIME: (n) => Date.parse(n.toString()) / 1000,
      STRING: (s) => Number(s)
    },
    STRING: {
      NUMBER: (n: number) => ``+n
    }
  };

  const CAST_TYPE_TO_JS: Lookup<Lookup<(inputJS: string)=> string>> = {
    TIME: {
      NUMBER: (inputJS) => `new Date(${inputJS}*1000)`
    },
    NUMBER: {
      TIME: (inputJS) => `${inputJS} / 1000`, // we get the time in ms as argument in extractionFn
      STRING: (s) => `Number(${s})`
    },
    STRING: {
      NUMBER: (inputJS) => `${inputJS}`
    }
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
      if (inputType && (!CAST_TYPE_TO_FN[castType][inputType])) {
        throw new Error(`unsupported cast from ${inputType} to ${castType}`);
      }

      return castType as PlyType;
    }

    public _fillRefSubstitutions(): FullType {
      const { castType } = this;

      if (castType === 'TIME') {
        return {
          type: 'TIME',
        };
      } else if (castType === 'NUMBER') {
        return {
          type: 'NUMBER',
        };
      } else if (castType === 'STRING') {
        return {
          type: 'STRING',
        };
      }


      throw new Error(`unrecognized cast type ${castType}`);
    }

    protected _foldWithPrevAction(prevAction: Action): Action {
      if (prevAction.equals(this)) {
        return this;
      }
      return null;
    }


    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      const { castType } = this;
      var caster = (CAST_TYPE_TO_FN as any)[castType];
      if (!caster) throw new Error(`unsupported cast type '${castType}'`);
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (!inV) return null;
        if (isDate(inV)) return caster['TIME'](inV);
        if (typeof inV === 'string') return caster['STRING'](inV);
        if (typeof inV === 'number') return caster['NUMBER'](inV);

        throw new Error(`unsupported input type ${inV}`)
      }
    }

    protected _getJSHelper(inputType: PlyType, inputJS: string): string {
      const { castType } = this;
      var castJS = CAST_TYPE_TO_JS[castType];
      if (!castJS) throw new Error(`unsupported cast type in getJS '${castType}'`);
      return castJS[inputType](inputJS);
    }

    protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return dialect.castExpression(inputType, inputSQL, this.castType);
    }
  }

  Action.register(CastAction);
}
