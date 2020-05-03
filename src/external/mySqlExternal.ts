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


import { PlywoodRequester } from 'plywood-base-api';
import * as toArray from 'stream-to-array';
import { AttributeInfo, Attributes, PseudoDatum } from '../datatypes/index';
import { MySQLDialect } from '../dialect/mySqlDialect';
import { PlyType } from '../types';
import { External, ExternalJS, ExternalValue } from './baseExternal';
import { SQLExternal } from './sqlExternal';

export interface MySQLDescribeRow {
  Field: string;
  Type: string;
}

export class MySQLExternal extends SQLExternal {
  static engine = 'mysql';
  static type = 'DATASET';

  static fromJS(parameters: ExternalJS, requester: PlywoodRequester<any>): MySQLExternal {
    let value: ExternalValue = External.jsToValue(parameters, requester);
    return new MySQLExternal(value);
  }

  static postProcessIntrospect(columns: MySQLDescribeRow[]): Attributes {
    return columns.map((column: MySQLDescribeRow) => {
      let name = column.Field;
      let type: PlyType;
      let nativeType = column.Type.toLowerCase();
      if (nativeType === "datetime" || nativeType === "timestamp") {
        type = 'TIME';
      } else if (nativeType.indexOf("varchar(") === 0 || nativeType.indexOf("blob") === 0) {
        type = 'STRING';
      } else if (
        nativeType.indexOf("int(") === 0 ||
        nativeType.indexOf("bigint(") === 0 ||
        nativeType.indexOf("decimal(") === 0 ||
        nativeType.indexOf("float") === 0 ||
        nativeType.indexOf("double") === 0
      ) {
        type = 'NUMBER';
      } else if (nativeType.indexOf("tinyint(1)") === 0) {
        type = 'BOOLEAN';
      } else {
        return null;
      }
      return new AttributeInfo({
        name,
        type,
        nativeType
      });
    }).filter(Boolean);
  }

  static getSourceList(requester: PlywoodRequester<any>): Promise<string[]> {
    return toArray(requester({ query: "SHOW TABLES" }))
      .then((sources) => {
        if (!Array.isArray(sources)) throw new Error('invalid sources response');
        if (!sources.length) return sources;
        let key = Object.keys(sources[0])[0];
        if (!key) throw new Error('invalid sources response (no key)');
        return sources.map((s: PseudoDatum) => s[key]).sort();
      });
  }

  static getVersion(requester: PlywoodRequester<any>): Promise<string> {
    return toArray(requester({ query: 'SELECT @@version' }))
      .then((res) => {
        if (!Array.isArray(res) || res.length !== 1) throw new Error('invalid version response');
        let key = Object.keys(res[0])[0];
        if (!key) throw new Error('invalid version response (no key)');
        return res[0][key];
      });
  }

  constructor(parameters: ExternalValue) {
    super(parameters, new MySQLDialect());
    this._ensureEngine("mysql");
  }

  protected getIntrospectAttributes(): Promise<Attributes> {
    return toArray(this.requester({ query: `DESCRIBE ${this.dialect.escapeName(this.source as string)}` }))
      .then(MySQLExternal.postProcessIntrospect);
  }
}

External.register(MySQLExternal);
