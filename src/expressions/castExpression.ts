/*
 * Copyright 2016-2020 Imply Data, Inc.
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

import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { PlyTypeSimple } from '../types';

import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';

interface Caster {
  TIME: {
    NUMBER: (n: number) => Date;
  };
  NUMBER: {
    TIME: (d: Date) => number;
    _: (v: any) => number;
  };
  STRING: {
    _: (v: any) => string;
  };
  [castTo: string]: { [inputType: string]: any };
}

const CAST_TYPE_TO_FN: Caster = {
  TIME: {
    NUMBER: n => new Date(n),
  },
  NUMBER: {
    TIME: (n: Date) => Date.parse(n.toString()),
    _: (s: any) => Number(s),
  },
  STRING: {
    _: (v: any) => '' + v,
  },
};

export class CastExpression extends ChainableExpression {
  static op = 'Cast';
  static fromJS(parameters: ExpressionJS): CastExpression {
    const value = ChainableExpression.jsToValue(parameters);
    value.outputType = parameters.outputType || (parameters as any).castType; // Back compat
    return new CastExpression(value);
  }

  public outputType: PlyTypeSimple;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this.outputType = parameters.outputType;
    this._ensureOp('cast');
    if (typeof this.outputType !== 'string') {
      throw new Error('`outputType` must be a string');
    }
    this.type = this.outputType;
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.outputType = this.outputType;
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    js.outputType = this.outputType;
    return js;
  }

  public equals(other: CastExpression | undefined): boolean {
    return super.equals(other) && this.outputType === other.outputType;
  }

  protected _toStringParameters(_indent?: int): string[] {
    return [this.outputType];
  }

  protected _calcChainableHelper(operandValue: any): PlywoodValue {
    const { outputType } = this;
    const inputType = this.operand.type;
    if (outputType === inputType) return operandValue;

    const caster = (CAST_TYPE_TO_FN as any)[outputType];
    if (!caster) throw new Error(`unsupported cast type in calc '${outputType}'`);

    const castFn = caster[inputType] || caster['_'];
    if (!castFn) throw new Error(`unsupported cast from ${inputType} to '${outputType}'`);
    return operandValue ? castFn(operandValue) : null;
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    return dialect.castExpression(this.operand.type, operandSQL, this.outputType);
  }

  protected specialSimplify(): Expression {
    const { operand, outputType } = this;
    if (operand.type === outputType) return operand;
    return this;
  }
}

Expression.register(CastExpression);
