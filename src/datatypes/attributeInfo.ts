/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2020 Imply Data, Inc.
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
import { Class, immutableEqual, Instance, NamedArray } from 'immutable-class';
import { Expression, ExpressionJS, RefExpression } from '../expressions';
import { FullType, PlyType } from '../types';
import { Range, PlywoodRange, PlywoodRangeJS } from './range';

export type Attributes = AttributeInfo[];
export type AttributeJSs = AttributeInfoJS[];

export interface AttributeInfoValue {
  name: string;
  type?: PlyType;
  nativeType?: string;
  unsplitable?: boolean;
  maker?: Expression;
  cardinality?: number;
  range?: PlywoodRange;
  termsDelegate?: string;
}

export interface AttributeInfoJS {
  name: string;
  type?: PlyType;
  nativeType?: string;
  unsplitable?: boolean;
  maker?: ExpressionJS;
  cardinality?: number;
  range?: PlywoodRangeJS;
  termsDelegate?: string;
}

let check: Class<AttributeInfoValue, AttributeInfoJS>;
export class AttributeInfo implements Instance<AttributeInfoValue, AttributeInfoJS> {
  static isAttributeInfo(candidate: any): candidate is AttributeInfo {
    return candidate instanceof AttributeInfo;
  }

  static NATIVE_TYPE_FROM_SPECIAL: Record<string, string> = {
    unique: 'hyperUnique',
    theta: 'thetaSketch',
    histogram: 'approximateHistogram'
  };

  static fromJS(parameters: AttributeInfoJS): AttributeInfo {
    if (typeof parameters !== "object") {
      throw new Error("unrecognizable attributeMeta");
    }

    let value: AttributeInfoValue = {
      name: parameters.name
    };
    if (parameters.type) value.type = parameters.type;

    let nativeType = parameters.nativeType;
    if (!nativeType && hasOwnProp(parameters, 'special')) {
      nativeType = AttributeInfo.NATIVE_TYPE_FROM_SPECIAL[(parameters as any).special];
      value.type = 'NULL';
    }
    value.nativeType = nativeType;
    if (parameters.unsplitable) value.unsplitable = true;

    let maker = parameters.maker || (parameters as any).makerAction;
    if (maker) value.maker = Expression.fromJS(maker);
    if (parameters.cardinality) value.cardinality = parameters.cardinality;
    if (parameters.range) value.range = Range.fromJS(parameters.range);
    if (parameters.termsDelegate) value.termsDelegate = parameters.termsDelegate;

    return new AttributeInfo(value);
  }

  static fromJSs(attributeJSs: AttributeJSs): Attributes {
    if (!Array.isArray(attributeJSs)) throw new TypeError("invalid attributeJSs");
    return attributeJSs.map(attributeJS => AttributeInfo.fromJS(attributeJS));
  }

  static toJSs(attributes: Attributes): AttributeJSs {
    return attributes.map(attribute => attribute.toJS());
  }

  static override(attributes: Attributes, attributeOverrides: Attributes): Attributes {
    return NamedArray.overridesByName(attributes, attributeOverrides);
  }


  public name: string;
  public nativeType: string;
  public type: PlyType;
  public datasetType?: Record<string, FullType>;
  public unsplitable: boolean;
  public maker?: Expression;
  public cardinality?: number;
  public range?: PlywoodRange;
  public termsDelegate?: string;

  constructor(parameters: AttributeInfoValue) {
    if (typeof parameters.name !== "string") {
      throw new Error("name must be a string");
    }
    this.name = parameters.name;
    this.type = parameters.type || 'NULL';
    if (!RefExpression.validType(this.type)) throw new Error(`invalid type: ${this.type}`);

    this.unsplitable = Boolean(parameters.unsplitable);
    this.maker = parameters.maker;
    if (parameters.nativeType) this.nativeType = parameters.nativeType;
    if (parameters.cardinality) this.cardinality = parameters.cardinality;
    if (parameters.range) this.range = parameters.range;
    if (parameters.termsDelegate) this.termsDelegate = parameters.termsDelegate;
  }

  public toString(): string {
    let nativeType = this.nativeType ? `[${this.nativeType}]` : '';
    return `${this.name}::${this.type}${nativeType}`;
  }

  public valueOf(): AttributeInfoValue {
    return {
      name: this.name,
      type: this.type,
      unsplitable: this.unsplitable,
      nativeType: this.nativeType,
      maker: this.maker,
      cardinality: this.cardinality,
      range: this.range,
      termsDelegate: this.termsDelegate
    };
  }

  public toJS(): AttributeInfoJS {
    let js: AttributeInfoJS = {
      name: this.name,
      type: this.type
    };
    if (this.nativeType) js.nativeType = this.nativeType;
    if (this.unsplitable) js.unsplitable = true;
    if (this.maker) js.maker = this.maker.toJS();
    if (this.cardinality) js.cardinality = this.cardinality;
    if (this.range) js.range = this.range.toJS();
    if (this.termsDelegate) js.termsDelegate = this.termsDelegate;
    return js;
  }

  public toJSON(): AttributeInfoJS {
    return this.toJS();
  }

  public equals(other: AttributeInfo | undefined): boolean {
    return other instanceof AttributeInfo &&
      this.name === other.name &&
      this.type === other.type &&
      this.nativeType === other.nativeType &&
      this.unsplitable === other.unsplitable &&
      immutableEqual(this.maker, other.maker) &&
      this.cardinality === other.cardinality &&
      immutableEqual(this.range, other.range) &&
      this.termsDelegate === other.termsDelegate;
  }

  public dropOriginInfo(): AttributeInfo {
    let value = this.valueOf();
    delete value.maker;
    delete value.nativeType;
    value.unsplitable = false;
    delete value.cardinality;
    delete value.range;
    return new AttributeInfo(value);
  }

  public get(propertyName: string): any {
    return (this as any)[propertyName];
  }

  public deepGet(propertyName: string): any {
    return this.get(propertyName);
  }

  public change(propertyName: string, newValue: any): AttributeInfo {
    let v = this.valueOf();

    if (!hasOwnProp(v, propertyName)) {
      throw new Error(`Unknown property: ${propertyName}`);
    }

    (v as any)[propertyName] = newValue;
    return new AttributeInfo(v);
  }

  public deepChange(propertyName: string, newValue: any): AttributeInfo {
    return this.change(propertyName, newValue);
  }

  public changeType(type: PlyType): AttributeInfo {
    let value = this.valueOf();
    value.type = type;
    return new AttributeInfo(value);
  }

  public getUnsplitable(): boolean {
    return this.unsplitable;
  }

  public changeUnsplitable(unsplitable: boolean): AttributeInfo {
    let value = this.valueOf();
    value.unsplitable = unsplitable;
    return new AttributeInfo(value);
  }

  public changeRange(range: PlywoodRange): AttributeInfo {
    let value = this.valueOf();
    value.range = range;
    return new AttributeInfo(value);
  }
}
check = AttributeInfo;
