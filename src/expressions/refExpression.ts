/*
 * Copyright 2012-2015 Metamarkets Group Inc.
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

import * as Q from 'q';
import { SimpleArray } from 'immutable-class';
import { Expression, ExpressionValue, ExpressionJS, Alterations, Indexer } from "./baseExpression";
import { PlyType, DatasetFullType, PlyTypeSingleValue, FullType } from "../types";
import { SQLDialect } from "../dialect/baseDialect";
import { hasOwnProperty, repeat } from "../helper/utils";
import { PlywoodValue } from "../datatypes/index";
import { Datum, ComputeFn } from "../datatypes/dataset";

export const POSSIBLE_TYPES: Lookup<number> = {
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

  static fromJS(parameters: ExpressionJS): RefExpression {
    var value: ExpressionValue;
    if (hasOwnProperty(parameters, 'nest')) {
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
    var refValue: ExpressionValue = { op: 'ref' };
    var match: RegExpMatchArray;

    match = str.match(GENERATIONS_REGEXP);
    if (match) {
      var nest = match[0].length;
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
    return hasOwnProperty(POSSIBLE_TYPES, typeName);
  }

  static toJavaScriptSafeName(variableName: string): string {
    if (!RefExpression.SIMPLE_NAME_REGEXP.test(variableName)) {
      variableName = variableName.replace(/\W/g, (c) => `$${c.charCodeAt(0)}`);
    }
    return '_' + variableName;
  }

  static findProperty(obj: any, key: string): any {
    return hasOwnProperty(obj, key) ? key : null;
  }

  static findPropertyCI(obj: any, key: string): any {
    var lowerKey = key.toLowerCase();
    if (obj == null) return null;
    return SimpleArray.find(Object.keys(obj), (v) => v.toLowerCase() === lowerKey);
  }


  public nest: int;
  public name: string;
  public remote: boolean;
  public ignoreCase: boolean;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp("ref");

    var name = parameters.name;
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError("must have a nonempty `name`");
    }
    this.name = name;

    var nest = parameters.nest;
    if (typeof nest !== 'number') {
      throw new TypeError("must have nest");
    }
    if (nest < 0) {
      throw new Error("nest must be non-negative");
    }
    this.nest = nest;

    var myType = parameters.type;
    if (myType) {
      if (!RefExpression.validType(myType)) {
        throw new TypeError(`unsupported type '${myType}'`);
      }
      this.type = myType;
    }

    this.remote = Boolean(parameters.remote);
    this.simple = true;
    this.ignoreCase = parameters.ignoreCase;
  }

  public valueOf(): ExpressionValue {
    var value = super.valueOf();
    value.name = this.name;
    value.nest = this.nest;
    if (this.type) value.type = this.type;
    if (this.remote) value.remote = true;
    if (this.ignoreCase) value.ignoreCase = true;
    return value;
  }

  public toJS(): ExpressionJS {
    var js = super.toJS();
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
    let property: string = null;

    return (d: Datum, c: Datum) => {
      if (nest) {
        property = ignoreCase ? RefExpression.findPropertyCI(c, name) : name;
        return c[property];
      } else {
        property = ignoreCase ? RefExpression.findPropertyCI(d, name) : RefExpression.findProperty(d, name);
        return property != null ? d[property] : null;
      }
    };
  }

  public getJS(datumVar: string): string {
    const { name, nest, ignoreCase } = this;
    if (nest) throw new Error("can not call getJS on unresolved expression");
    if (ignoreCase) throw new Error("can not express ignore case as js expression");

    var expr: string;
    if (datumVar) {
      expr = datumVar.replace('[]', "[" + JSON.stringify(name) + "]");
    } else {
      expr = RefExpression.toJavaScriptSafeName(name);
    }

    if (this.type === 'NUMBER') expr = `(+${expr})`;
    return expr;
  }

  public getSQL(dialect: SQLDialect, minimal = false): string {
    if (this.nest) throw new Error(`can not call getSQL on unresolved expression: ${this}`);
    return dialect.escapeName(this.name);
  }

  public equals(other: RefExpression): boolean {
    return super.equals(other) &&
      this.name === other.name &&
      this.nest === other.nest &&
      this.remote === other.remote &&
      this.ignoreCase === other.ignoreCase;
  }

  public isRemote(): boolean {
    return this.remote;
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, indexer: Indexer, alterations: Alterations): FullType {
    var myIndex = indexer.index;
    indexer.index++;
    var { nest, ignoreCase, name } = this;
    // Step the parentContext back; once for each generation
    var myTypeContext = typeContext;
    while (nest--) {
      myTypeContext = myTypeContext.parent;
      if (!myTypeContext) throw new Error('went too deep on ' + this.toString());
    }

    var myName = ignoreCase ? RefExpression.findPropertyCI(myTypeContext.datasetType, name) : name;
    if (myName == null) throw new Error('could not resolve ' + this.toString());
    // Look for the reference in the parent chain
    var nestDiff = 0;
    while (myTypeContext && !hasOwnProperty(myTypeContext.datasetType, myName)) {
      myTypeContext = myTypeContext.parent;
      nestDiff++;
    }
    if (!myTypeContext) {
      throw new Error('could not resolve ' + this.toString());
    }

    var myFullType = myTypeContext.datasetType[myName];
    var myType = myFullType.type;
    var myRemote = Boolean((myFullType as DatasetFullType).remote);

    if (this.type && this.type !== myType) {
      throw new TypeError(`type mismatch in ${this} (has: ${this.type} needs: ${myType})`);
    }

    // Check if it needs to be replaced
    if (!this.type || nestDiff > 0 || this.remote !== myRemote || ignoreCase) {
      alterations[myIndex] = new RefExpression({
        name: myName,
        nest: this.nest + nestDiff,
        type: myType,
        remote: myRemote
      });
    }

    if (myType === 'DATASET') {
      return {
        parent: typeContext,
        type: 'DATASET',
        datasetType: (myFullType as DatasetFullType).datasetType,
        remote: (myFullType as DatasetFullType).remote
      };
    }

    return myFullType;
  }

  public incrementNesting(by: int = 1): RefExpression {
    var value = this.valueOf();
    value.nest = by + value.nest;
    return new RefExpression(value);
  }

  public maxPossibleSplitValues(): number {
    return this.type === 'BOOLEAN' ? 3 : Infinity;
  }

  public upgradeToType(targetType: PlyType): Expression {
    const { type } = this;
    if (targetType === 'TIME' && (!type || type === 'STRING')) {
      return this.changeType(targetType);
    }
    return this;
  }

  public toCaseInsensitive(): Expression {
    var value = this.valueOf();
    value.ignoreCase = true;
    return new RefExpression(value);
  }

  private changeType(newType: PlyType) {
    var value = this.valueOf();
    value.type = newType;
    return new RefExpression(value);
  }
}

Expression.register(RefExpression);
