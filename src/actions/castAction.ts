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

import { Action, ActionJS, ActionValue } from "./baseAction";
import { PlyType, DatasetFullType, PlyTypeSimple, FullType } from "../types";
import { SQLDialect } from "../dialect/baseDialect";
import { Datum, ComputeFn } from "../datatypes/dataset";
import { hasOwnProperty } from "../helper/utils";

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
    var outputType = parameters.outputType || (parameters as any).castType;

    // Back compat
    if (!outputType && hasOwnProperty(parameters, 'castType')) {
      outputType = (parameters as any).castType;
    }
    value.outputType = outputType;

    return new CastAction(value);
  }

  public outputType: PlyTypeSimple;

  constructor(parameters: ActionValue) {
    super(parameters, dummyObject);
    this.outputType = parameters.outputType;
    this._ensureAction("cast");
    if (typeof this.outputType !== 'string') {
      throw new Error("`outputType` must be a string");
    }
  }

  public valueOf(): ActionValue {
    var value = super.valueOf();
    value.outputType = this.outputType;
    return value;
  }

  public toJS(): ActionJS {
    var js = super.toJS();
    js.outputType = this.outputType;
    return js;
  }

  public equals(other: CastAction): boolean {
    return super.equals(other) &&
      this.outputType === other.outputType;
  }

  protected _toStringParameters(expressionString: string): string[] {
    return [this.outputType];
  }

  public getNecessaryInputTypes(): PlyTypeSimple[] {
    var castType = this.outputType;
    return Object.keys(CAST_TYPE_TO_FN[castType]) as PlyTypeSimple[];
  }

  public getOutputType(inputType: PlyType): PlyType {
    return this.outputType;
  }

  public _fillRefSubstitutions(): FullType {
    const { outputType } = this;
    return {
      type: outputType
    };
  }

  protected _removeAction(inputType: PlyType): boolean {
    return this.outputType === inputType;
  }

  protected _foldWithPrevAction(prevAction: Action): Action {
    if (prevAction.equals(this)) {
      return this;
    }
    return null;
  }


  protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
    const { outputType } = this;
    var caster = (CAST_TYPE_TO_FN as any)[outputType];
    var castFn = caster[inputType] || caster['UNIVERSAL'];
    if (!castFn) throw new Error(`unsupported cast from ${inputType} to '${outputType}'`);
    return (d: Datum, c: Datum) => {
      var inV = inputFn(d, c);
      if (!inV) return null;
      return castFn(inV);
    }
  }

  protected _getJSHelper(inputType: PlyType, inputJS: string): string {
    const { outputType } = this;
    var castJS = CAST_TYPE_TO_JS[outputType];
    if (!castJS) throw new Error(`unsupported cast type in getJS '${outputType}'`);
    var js = castJS[inputType] || castJS['UNIVERSAL'];
    if (!js) throw new Error(`unsupported combo in getJS of cast action: ${inputType} to ${outputType}`);
    return js(inputJS);
  }

  protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
    return dialect.castExpression(inputType, inputSQL, this.outputType);
  }
}

Action.register(CastAction);
