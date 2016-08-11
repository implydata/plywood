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

import { Class, Instance, isInstanceOf } from "immutable-class";
import { PlyType, FullType } from "../types";
import { hasOwnProperty, overridesByName } from "../helper/utils";
import { ActionJS, Action } from "../actions/baseAction";
import { RefExpression } from "../expressions/refExpression";

function isInteger(n: any): boolean {
  return !isNaN(n) && n % 1 === 0;
}

function isPositiveInteger(n: any): boolean {
  return isInteger(n) && 0 < n;
}

export type Attributes = AttributeInfo[];
export type AttributeJSs = AttributeInfoJS[];

export interface AttributeInfoValue {
  special?: string;
  name: string;
  type?: PlyType;
  datasetType?: Lookup<FullType>;
  unsplitable?: boolean;
  makerAction?: Action;

  // range
  separator?: string;
  rangeSize?: number;
  digitsBeforeDecimal?: int;
  digitsAfterDecimal?: int;
}

export interface AttributeInfoJS {
  special?: string;
  name: string;
  type?: PlyType;
  datasetType?: Lookup<FullType>;
  unsplitable?: boolean;
  makerAction?: ActionJS;

  // range
  separator?: string;
  rangeSize?: number;
  digitsBeforeDecimal?: int;
  digitsAfterDecimal?: int;
}

var check: Class<AttributeInfoValue, AttributeInfoJS>;
export class AttributeInfo implements Instance<AttributeInfoValue, AttributeInfoJS> {
  static isAttributeInfo(candidate: any): candidate is AttributeInfo {
    return isInstanceOf(candidate, AttributeInfo);
  }

  static jsToValue(parameters: AttributeInfoJS): AttributeInfoValue {
    var value: AttributeInfoValue = {
      special: parameters.special,
      name: parameters.name
    };
    if (parameters.type) value.type = parameters.type;
    if (parameters.datasetType) value.datasetType = parameters.datasetType;
    if (parameters.unsplitable) value.unsplitable = true;
    if (parameters.makerAction) value.makerAction = Action.fromJS(parameters.makerAction);
    return value;
  }

  static classMap: Lookup<typeof AttributeInfo> = {};
  static register(ex: typeof AttributeInfo): void {
    var op = (<any>ex).name.replace('AttributeInfo', '').replace(/^\w/, (s: string) => s.toLowerCase());
    AttributeInfo.classMap[op] = ex;
  }

  static fromJS(parameters: AttributeInfoJS): AttributeInfo {
    if (typeof parameters !== "object") {
      throw new Error("unrecognizable attributeMeta");
    }
    if (!hasOwnProperty(parameters, 'special')) {
      return new AttributeInfo(AttributeInfo.jsToValue(parameters));
    }
    if (parameters.special === 'range') {
      throw new Error("'range' attribute info is no longer supported, you should apply the .extract('^\\d+') function instead");
    }
    var Class = AttributeInfo.classMap[parameters.special];
    if (!Class) {
      throw new Error(`unsupported special attributeInfo '${parameters.special}'`);
    }
    return Class.fromJS(parameters);
  }

  static fromJSs(attributeJSs: AttributeJSs): Attributes {
    if (!Array.isArray(attributeJSs)) {
      if (attributeJSs && typeof attributeJSs === 'object') {
        var newAttributeJSs: any[] = [];
        for (var attributeName in attributeJSs) {
          if (!hasOwnProperty(attributeJSs, attributeName)) continue;
          var attributeJS = attributeJSs[attributeName];
          attributeJS['name'] = attributeName;
          newAttributeJSs.push(attributeJS);
        }
        console.warn('attributes now needs to be passed as an array like so: ' + JSON.stringify(newAttributeJSs, null, 2));
        attributeJSs = newAttributeJSs;
      } else {
        throw new TypeError("invalid attributeJSs");
      }
    }
    return attributeJSs.map(attributeJS => AttributeInfo.fromJS(attributeJS));
  }

  static toJSs(attributes: Attributes): AttributeJSs {
    return attributes.map(attribute => attribute.toJS());
  }

  static override(attributes: Attributes, attributeOverrides: Attributes): Attributes {
    return overridesByName(attributes, attributeOverrides);
  }


  public special: string;
  public name: string;
  public type: PlyType;
  public datasetType: Lookup<FullType>;
  public unsplitable: boolean;
  public makerAction: Action;

  constructor(parameters: AttributeInfoValue) {
    if (parameters.special) this.special = parameters.special;

    if (typeof parameters.name !== "string") {
      throw new Error("name must be a string");
    }
    this.name = parameters.name;

    if (hasOwnProperty(parameters, 'type') && !RefExpression.validType(parameters.type)) {
      throw new Error(`invalid type: ${parameters.type}`);
    }
    this.type = parameters.type;

    this.datasetType = parameters.datasetType;
    this.unsplitable = Boolean(parameters.unsplitable);
    this.makerAction = parameters.makerAction;
  }

  public _ensureSpecial(special: string) {
    if (!this.special) {
      this.special = special;
      return;
    }
    if (this.special !== special) {
      throw new TypeError(`incorrect attributeInfo special '${this.special}' (needs to be: '${special}')`);
    }
  }

  public _ensureType(myType: PlyType) {
    if (!this.type) {
      this.type = myType;
      return;
    }
    if (this.type !== myType) {
      throw new TypeError(`incorrect attributeInfo type '${this.type}' (needs to be: '${myType}')`);
    }
  }

  public toString(): string {
    var special = this.special ? `[${this.special}]` : '';
    return `${this.name}::${this.type}${special}`;
  }

  public valueOf(): AttributeInfoValue {
    var value: AttributeInfoValue = {
      name: this.name,
      type: this.type,
      unsplitable: this.unsplitable
    };
    if (this.special) value.special = this.special;
    if (this.datasetType) value.datasetType = this.datasetType;
    if (this.makerAction) value.makerAction = this.makerAction;
    return value;
  }

  public toJS(): AttributeInfoJS {
    var js: AttributeInfoJS = {
      name: this.name,
      type: this.type
    };
    if (this.unsplitable) js.unsplitable = true;
    if (this.special) js.special = this.special;
    if (this.datasetType) js.datasetType = this.datasetType;
    if (this.makerAction) js.makerAction = this.makerAction.toJS();
    return js;
  }

  public toJSON(): AttributeInfoJS {
    return this.toJS();
  }

  public equals(other: AttributeInfo): boolean {
    return AttributeInfo.isAttributeInfo(other) &&
      this.special === other.special &&
      this.name === other.name &&
      this.type === other.type &&
      Boolean(this.makerAction) === Boolean(other.makerAction) &&
      (!this.makerAction || this.makerAction.equals(other.makerAction));
  }

  public serialize(value: any): any {
    return value;
  }
}
check = AttributeInfo;


export class UniqueAttributeInfo extends AttributeInfo {
  static fromJS(parameters: AttributeInfoJS): UniqueAttributeInfo {
    return new UniqueAttributeInfo(AttributeInfo.jsToValue(parameters));
  }

  constructor(parameters: AttributeInfoValue) {
    super(parameters);
    this._ensureSpecial("unique");
    this._ensureType('STRING');
  }

  public serialize(value: any): string {
    throw new Error("can not serialize an approximate unique value");
  }
}
AttributeInfo.register(UniqueAttributeInfo);

export class ThetaAttributeInfo extends AttributeInfo {
  static fromJS(parameters: AttributeInfoJS): ThetaAttributeInfo {
    return new ThetaAttributeInfo(AttributeInfo.jsToValue(parameters));
  }

  constructor(parameters: AttributeInfoValue) {
    super(parameters);
    this._ensureSpecial("theta");
    this._ensureType('STRING');
  }

  public serialize(value: any): string {
    throw new Error("can not serialize a theta value");
  }
}
AttributeInfo.register(ThetaAttributeInfo);

export class HistogramAttributeInfo extends AttributeInfo {
  static fromJS(parameters: AttributeInfoJS): HistogramAttributeInfo {
    return new HistogramAttributeInfo(AttributeInfo.jsToValue(parameters));
  }

  constructor(parameters: AttributeInfoValue) {
    super(parameters);
    this._ensureSpecial("histogram");
    this._ensureType('NUMBER');
  }

  public serialize(value: any): string {
    throw new Error("can not serialize a histogram value");
  }
}
AttributeInfo.register(HistogramAttributeInfo);
