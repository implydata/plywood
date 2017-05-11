/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2017 Imply Data, Inc.
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

import * as hasOwnProp from 'has-own-prop';
import { SimpleArray } from 'immutable-class';
import { ComputeFn, Datum, PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/index';
import { repeat } from '../helper/index';
import { DatasetFullType, PlyType } from '../types';
import { Expression, ExpressionJS, ExpressionValue } from './baseExpression';

export const POSSIBLE_TYPES: Record<string, number> = {
  'NULL': 1,
  'BOOLEAN': 1,
  'NUMBER': 1,
  'TIME': 1,
  'STRING': 1,
  'NUMBER_RANGE': 1,
  'TIME_RANGE': 1,
  'SET': 1,
  'SET/NULL': 1,
  'SET/BOOLEAN': 1,
  'SET/NUMBER': 1,
  'SET/TIME': 1,
  'SET/STRING': 1,
  'SET/NUMBER_RANGE': 1,
  'SET/TIME_RANGE': 1,
  'DATASET': 1
};

const GENERATIONS_REGEXP = /^\^+/;
const TYPE_REGEXP = /:([A-Z\/_]+)$/;

export class RefExpression extends Expression {
  static SIMPLE_NAME_REGEXP = /^([a-z_]\w*)$/i;

  static op = "Ref";
  static fromJS(parameters: ExpressionJS): RefExpression {
    let value: ExpressionValue;
    if (hasOwnProp(parameters, 'nest')) {
      value = <any>parameters;
    } else {
      value = {
        op: 'ref',
        nest: 0,
        name: parameters.name,
        type: parameters.type,
        ignoreCase: parameters.ignoreCase
      };
    }
    return new RefExpression(value);
  }

  static parse(str: string): RefExpression {
    let refValue: ExpressionValue = { op: 'ref' };
    let match: RegExpMatchArray;

    match = str.match(GENERATIONS_REGEXP);
    if (match) {
      let nest = match[0].length;
      refValue.nest = nest;
      str = str.substr(nest);
    } else {
      refValue.nest = 0;
    }

    match = str.match(TYPE_REGEXP);
    if (match) {
      refValue.type = <PlyType>match[1];
      str = str.substr(0, str.length - match[0].length);
    }

    if (str[0] === '{' && str[str.length - 1] === '}') {
      str = str.substr(1, str.length - 2);
    }

    refValue.name = str;
    return new RefExpression(refValue);
  }

  static validType(typeName: string): boolean {
    return hasOwnProp(POSSIBLE_TYPES, typeName);
  }

  static toJavaScriptSafeName(variableName: string): string {
    if (!RefExpression.SIMPLE_NAME_REGEXP.test(variableName)) {
      variableName = variableName.replace(/\W/g, (c) => `$${c.charCodeAt(0)}`);
    }
    return '_' + variableName;
  }

  static findProperty(obj: any, key: string): any {
    return hasOwnProp(obj, key) ? key : null;
  }

  static findPropertyCI(obj: any, key: string): any {
    let lowerKey = key.toLowerCase();
    if (obj == null) return null;
    return SimpleArray.find(Object.keys(obj), (v) => v.toLowerCase() === lowerKey);
  }


  public nest: int;
  public name: string;
  public ignoreCase: boolean;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("ref");

    let name = parameters.name;
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError("must have a nonempty `name`");
    }
    this.name = name;

    let nest = parameters.nest;
    if (typeof nest !== 'number') {
      throw new TypeError("must have nest");
    }
    if (nest < 0) {
      throw new Error("nest must be non-negative");
    }
    this.nest = nest;

    let myType = parameters.type;
    if (myType) {
      if (!RefExpression.validType(myType)) {
        throw new TypeError(`unsupported type '${myType}'`);
      }
      this.type = myType;
    }

    this.simple = true;
    this.ignoreCase = parameters.ignoreCase;
  }

  public valueOf(): ExpressionValue {
    let value = super.valueOf();
    value.name = this.name;
    value.nest = this.nest;
    if (this.type) value.type = this.type;
    if (this.ignoreCase) value.ignoreCase = true;
    return value;
  }

  public toJS(): ExpressionJS {
    let js = super.toJS();
    js.name = this.name;
    if (this.nest) js.nest = this.nest;
    if (this.type) js.type = this.type;
    if (this.ignoreCase) js.ignoreCase = true;
    return js;
  }

  public toString(): string {
    const { name, nest, type, ignoreCase } = this;
    let str = name;

    if (!RefExpression.SIMPLE_NAME_REGEXP.test(name)) {
      str = '{' + str + '}';
    }
    if (nest) {
      str = repeat('^', nest) + str;
    }
    if (type) {
      str += ':' + type;
    }
    return (ignoreCase ? 'i$' : '$') + str;
  }

  public getFn(): ComputeFn {
    const { name, nest, ignoreCase } = this;
    if (nest) throw new Error('can not getFn on a nested function');

    return (d: Datum) => {
      let property = ignoreCase ? RefExpression.findPropertyCI(d, name) : name;
      return property != null ? d[property] : null;
    };
  }

  public calc(datum: Datum): PlywoodValue {
    const { name, nest, ignoreCase } = this;
    if (nest) throw new Error('can not calc on a nested expression');

    let property = ignoreCase ? RefExpression.findPropertyCI(datum, name) : name;
    return property != null ? (datum[property] as any) : null;
  }

  public getJS(datumVar: string): string {
    const { name, nest, ignoreCase } = this;
    if (nest) throw new Error("can not call getJS on unresolved expression");
    if (ignoreCase) throw new Error("can not express ignore case as js expression");

    let expr: string;
    if (datumVar) {
      expr = datumVar.replace('[]', "[" + JSON.stringify(name) + "]");
    } else {
      expr = RefExpression.toJavaScriptSafeName(name);
    }

    switch (this.type) {
      case 'NUMBER':
        return `parseFloat(${expr})`;

      default:
        return expr;
    }
  }

  public getSQL(dialect: SQLDialect, minimal = false): string {
    if (this.nest) throw new Error(`can not call getSQL on unresolved expression: ${this}`);
    return dialect.maybeNamespacedName(this.name);
  }

  public equals(other: RefExpression): boolean {
    return super.equals(other) &&
      this.name === other.name &&
      this.nest === other.nest &&
      this.ignoreCase === other.ignoreCase;
  }

  public changeInTypeContext(typeContext: DatasetFullType): RefExpression {
    const { nest, ignoreCase, name } = this;
    // Step the parentContext back; once for each generation
    let myTypeContext = typeContext;
    for (let i = nest; i > 0; i--) {
      myTypeContext = myTypeContext.parent;
      if (!myTypeContext) throw new Error(`went too deep on ${this}`);
    }

    let myName = ignoreCase ? RefExpression.findPropertyCI(myTypeContext.datasetType, name) : name;
    if (myName == null) throw new Error(`could not resolve ${this}`);
    // Look for the reference in the parent chain
    let nestDiff = 0;
    while (myTypeContext && !hasOwnProp(myTypeContext.datasetType, myName)) {
      myTypeContext = myTypeContext.parent;
      nestDiff++;
    }
    if (!myTypeContext) {
      throw new Error(`could not resolve ${this}`);
    }

    let myFullType = myTypeContext.datasetType[myName];
    let myType = myFullType.type;

    if (this.type && this.type !== myType) {
      throw new TypeError(`type mismatch in ${this} (has: ${this.type} needs: ${myType})`);
    }

    // Check if it needs to be replaced
    if (!this.type || nestDiff > 0 || ignoreCase) {
      return new RefExpression({
        name: myName,
        nest: nest + nestDiff,
        type: myType
      });
    } else {
      return this;
    }
  }

  public updateTypeContext(typeContext: DatasetFullType): DatasetFullType {
    if (this.type !== 'DATASET') return typeContext;

    const { nest, name } = this;
    let myTypeContext = typeContext;
    for (let i = nest; i > 0; i--) {
      myTypeContext = myTypeContext.parent;
      if (!myTypeContext) throw new Error('went too deep on ' + this.toString());
    }

    let myFullType = myTypeContext.datasetType[name];

    return {
      parent: typeContext,
      type: 'DATASET',
      datasetType: (myFullType as DatasetFullType).datasetType
    };
  }

  public incrementNesting(by: int = 1): RefExpression {
    let value = this.valueOf();
    value.nest += by;
    return new RefExpression(value);
  }

  public upgradeToType(targetType: PlyType): Expression {
    const { type } = this;
    if (targetType === 'TIME' && (!type || type === 'STRING')) {
      return this.changeType(targetType);
    }
    return this;
  }

  public toCaseInsensitive(): Expression {
    let value = this.valueOf();
    value.ignoreCase = true;
    return new RefExpression(value);
  }

  private changeType(newType: PlyType) {
    let value = this.valueOf();
    value.type = newType;
    return new RefExpression(value);
  }
}

Expression._ = new RefExpression({ name: '_', nest: 0 });

Expression.register(RefExpression);
