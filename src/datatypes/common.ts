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

import * as Promise from 'any-promise';
import { isDate } from 'chronoshift';
import * as hasOwnProp from 'has-own-prop';
import { Expression } from '../expressions/baseExpression';
import { Dataset, Datum } from './dataset';
import { Set } from './set';
import { TimeRange } from './timeRange';
import { StringRange } from './stringRange';
import { NumberRange } from './numberRange';
import { External } from '../external/baseExternal';
import { PlyType, DatasetFullType, PlyTypeSimple, FullType } from '../types';

export function getValueType(value: any): PlyType {
  let typeofValue = typeof value;
  if (typeofValue === 'object') {
    if (value === null) {
      return 'NULL';
    } else if (isDate(value)) {
      return 'TIME';
    } else if (hasOwnProp(value, 'start') && hasOwnProp(value, 'end')) {
      if (isDate(value.start) || isDate(value.end)) return 'TIME_RANGE';
      if (typeof value.start === 'number' || typeof value.end === 'number') return 'NUMBER_RANGE';
      if (typeof value.start === 'string' || typeof value.end === 'string') return 'STRING_RANGE';
      throw new Error("unrecognizable range");
    } else {
      let ctrType = value.constructor.type;
      if (!ctrType) {
        if (value instanceof Expression) {
          throw new Error(`expression used as datum value ${value}`);
        } else {
          throw new Error(`can not have an object without a type: ${JSON.stringify(value)}`);
        }
      }
      if (ctrType === 'SET') ctrType += '/' + value.setType;
      return <PlyType>ctrType;
    }
  } else {
    if (typeofValue !== 'boolean' && typeofValue !== 'number' && typeofValue !== 'string') {
      throw new TypeError('unsupported JS type ' + typeofValue);
    }
    return <PlyType>typeofValue.toUpperCase();
  }
}

export function getFullType(value: any): FullType {
  let myType = getValueType(value);
  return myType === 'DATASET' ? (<Dataset>value).getFullType() : { type: <PlyTypeSimple>myType };
}

export function getFullTypeFromDatum(datum: Datum): DatasetFullType {
  let datasetType: Lookup<FullType> = {};
  for (let k in datum) {
    if (!hasOwnProp(datum, k)) continue;
    datasetType[k] = getFullType(datum[k]);
  }

  return {
    type: 'DATASET',
    datasetType: datasetType
  };
}

export function valueFromJS(v: any, typeOverride: string | null = null): any {
  if (v == null) {
    return null;
  } else if (Array.isArray(v)) {
    if (v.length && typeof v[0] !== 'object') {
      return Set.fromJS(v);
    } else {
      return Dataset.fromJS(v);
    }
  } else if (typeof v === 'object') {
    switch (typeOverride || v.type) {
      case 'NUMBER':
        let n = Number(v.value);
        if (isNaN(n)) throw new Error(`bad number value '${v.value}'`);
        return n;

      case 'NUMBER_RANGE':
        return NumberRange.fromJS(v);

      case 'STRING_RANGE':
        return StringRange.fromJS(v);

      case 'TIME':
        return typeOverride ? v : new Date(v.value);

      case 'TIME_RANGE':
        return TimeRange.fromJS(v);

      case 'SET':
        return Set.fromJS(v);

      default:
        if (v.toISOString) {
          return v; // Allow native date
        } else {
          throw new Error('can not have an object without a `type` as a datum value');
        }
    }
  } else if (typeof v === 'string' && typeOverride === 'TIME') {
    return new Date(v);
  }
  return v;
}

export function valueToJS(v: any): any {
  if (v == null) {
    return null;
  } else {
    let typeofV = typeof v;
    if (typeofV === 'object') {
      if (v.toISOString) {
        return v;
      } else {
        return v.toJS();
      }
    } else if (typeofV === 'number' && !isFinite(v)) {
      return String(v);
    }
  }
  return v;
}

export function valueToJSInlineType(v: any): any {
  if (v == null) {
    return null;
  } else {
    let typeofV = typeof v;
    if (typeofV === 'object') {
      if (v.toISOString) {
        return { type: 'TIME', value: v };
      } else {
        if (typeof v.toJS !== 'function') return '???'; // Technically we should never get here
        let js = v.toJS();
        if (!Array.isArray(js)) {
          js.type = v.constructor.type || 'EXPRESSION';
        }
        return js;
      }
    } else if (typeofV === 'number' && !isFinite(v)) {
      return { type: 'NUMBER', value: String(v) };
    }
  }
  return v;
}

// External functionality

export function datumHasExternal(datum: Datum): boolean {
  for (let name in datum) {
    let value = datum[name];
    if (value instanceof External) return true;
    if (value instanceof Dataset && value.hasExternal()) return true;
  }
  return false;
}

export function introspectDatum(datum: Datum): Promise<Datum> {
  let promises: Promise<void>[] = [];
  let newDatum: Datum = Object.create(null);
  Object.keys(datum)
    .forEach(name => {
      let v = datum[name];
      if (v instanceof External && v.needsIntrospect()) {
        promises.push(
          v.introspect().then((introspectedExternal: External) => {
            newDatum[name] = introspectedExternal;
          })
        );
      } else {
        newDatum[name] = v;
      }
    });

  return Promise.all(promises).then(() => newDatum);
}

export function failIfIntrospectNeededInDatum(datum: Datum): void {
  Object.keys(datum)
    .forEach(name => {
      let v = datum[name];
      if (v instanceof External && v.needsIntrospect()) {
        throw new Error('Can not have un-introspected external');
      }
    });
}



export function isSetType(type: PlyType): boolean {
  return type && type.indexOf('SET/') === 0;
}

export function wrapSetType(type: PlyType): PlyType {
  if (!type) return null;
  return isSetType(type) ? type : <PlyType>('SET/' + type);
}

export function unwrapSetType(type: PlyType): PlyType {
  if (!type) return null;
  return isSetType(type) ? <PlyType>type.substr(4) : type;
}


