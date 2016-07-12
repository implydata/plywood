/*
 * Copyright 2015-2016 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

module Plywood {

  interface Caster {
    TIME: {
      NUMBER: (n: number) => Date
    };
    NUMBER: {
      TIME: (d: Date) => number;
      UNIVERSAL: (v: any) => number;
    };
    STRING: {
      UNIVERSAL: (v: any) => String;
    }
    [castTo: string]: {[inputType: string]: any};
  }

  const CAST_TYPE_TO_FN: Caster = {
    TIME: {
      NUMBER: n => new Date(n)
    },
    NUMBER: {
      TIME: (n: Date) => Date.parse(n.toString()),
      UNIVERSAL: (s: any) => Number(s)
    },
    STRING: {
      UNIVERSAL: (v: any) => '' + v
    }
  };

  const CAST_TYPE_TO_JS: Lookup<Lookup<(inputJS: string)=> string>> = {
    TIME: {
      NUMBER: (inputJS) => `new Date(${inputJS})`
    },
    NUMBER: {
      UNIVERSAL: (s) => `+(${s})`
    },
    STRING: {
      UNIVERSAL: (inputJS) => `('' + ${inputJS})`
    }
  };

  export class CastAction extends Action {
    static fromJS(parameters: ActionJS): CastAction {
      var value = Action.jsToValue(parameters);
      value.castType = parameters.castType;
      return new CastAction(value);
    }

    public castType: PlyTypeSimple;

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
      if (inputType === castType) return castType;
      if (inputType && (!CAST_TYPE_TO_FN[castType][inputType]) && (!CAST_TYPE_TO_FN[castType]['UNIVERSAL'])) {
        throw new Error(`unsupported cast from ${inputType} to ${castType}`);
      }

      return castType;
    }

    public _fillRefSubstitutions(): FullType {
      const { castType } = this;
      return {
        type: castType
      };
    }

    protected _removeAction(inputType: PlyType): boolean {
      return this.castType === inputType;
    }

    protected _foldWithPrevAction(prevAction: Action): Action {
      if (prevAction.equals(this)) {
        return this;
      }
      return null;
    }


    protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
      const { castType } = this;
      var caster = (CAST_TYPE_TO_FN as any)[castType];
      var castFn = caster[inputType] || caster['UNIVERSAL'];
      if (!castFn) throw new Error(`unsupported cast from ${inputType} to '${castType}'`);
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (!inV) return null;
        return castFn(inV);
      }
    }

    protected _getJSHelper(inputType: PlyType, inputJS: string): string {
      const { castType } = this;
      var castJS = CAST_TYPE_TO_JS[castType];
      if (!castJS) throw new Error(`unsupported cast type in getJS '${castType}'`);
      var js = castJS[inputType] || castJS['UNIVERSAL'];
      if (!js) throw new Error(`unsupported combo in getJS of cast action: ${inputType} to ${castType}`);
      return js(inputJS);
    }

    protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return dialect.castExpression(inputType, inputSQL, this.castType);
    }
  }

  Action.register(CastAction);
}
