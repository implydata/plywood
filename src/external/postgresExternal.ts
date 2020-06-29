/*
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
import { AttributeInfo, Attributes } from '../datatypes/attributeInfo';
import { PseudoDatum } from '../datatypes/dataset';
import { PostgresDialect } from '../dialect/postgresDialect';
import { PlyType } from '../types';
import { External, ExternalJS, ExternalValue } from './baseExternal';
import { SQLExternal } from './sqlExternal';

export interface PostgresSQLDescribeRow {
  name: string;
  sqlType: string;
  arrayType?: string;
}

export class PostgresExternal extends SQLExternal {
  static engine = 'postgres';
  static type = 'DATASET';

  static fromJS(parameters: ExternalJS, requester: PlywoodRequester<any>): PostgresExternal {
    let value: ExternalValue = External.jsToValue(parameters, requester);
    return new PostgresExternal(value);
  }

  static postProcessIntrospect(columns: PostgresSQLDescribeRow[]): Attributes {
    return columns
      .map((column: PostgresSQLDescribeRow) => {
        let name = column.name;
        let type: PlyType;
        let nativeType = column.sqlType.toLowerCase();
        if (nativeType.indexOf('timestamp') !== -1) {
          type = 'TIME';
        } else if (nativeType === 'character varying') {
          type = 'STRING';
        } else if (nativeType === 'integer' || nativeType === 'bigint') {
          // ToDo: make something special for integers
          type = 'NUMBER';
        } else if (nativeType === 'double precision' || nativeType === 'float') {
          type = 'NUMBER';
        } else if (nativeType === 'boolean') {
          type = 'BOOLEAN';
        } else if (nativeType === 'array') {
          nativeType = column.arrayType.toLowerCase();
          if (nativeType === 'character') {
            type = 'SET/STRING';
          } else if (nativeType === 'timestamp') {
            type = 'SET/TIME';
          } else if (
            nativeType === 'integer' ||
            nativeType === 'bigint' ||
            nativeType === 'double precision' ||
            nativeType === 'float'
          ) {
            type = 'SET/NUMBER';
          } else if (nativeType === 'boolean') {
            type = 'SET/BOOLEAN';
          } else {
            return null;
          }
        } else {
          return null;
        }

        return new AttributeInfo({
          name,
          type,
          nativeType,
        });
      })
      .filter(Boolean);
  }

  static getSourceList(requester: PlywoodRequester<any>): Promise<string[]> {
    return toArray(
      requester({
        query: `SELECT table_name AS "tab" FROM INFORMATION_SCHEMA.TABLES WHERE table_type = 'BASE TABLE' AND table_schema = 'public'`,
      }),
    ).then(sources => {
      if (!sources.length) return sources;
      return sources.map((s: PseudoDatum) => s['tab']).sort();
    });
  }

  static getVersion(requester: PlywoodRequester<any>): Promise<string> {
    return toArray(requester({ query: 'SELECT version()' })).then(res => {
      if (!Array.isArray(res) || res.length !== 1) throw new Error('invalid version response');
      let key = Object.keys(res[0])[0];
      if (!key) throw new Error('invalid version response (no key)');
      let versionString = res[0][key];
      let match: string[];
      if ((match = versionString.match(/^PostgreSQL (\S+) on/))) versionString = match[1];
      return versionString;
    });
  }

  constructor(parameters: ExternalValue) {
    super(parameters, new PostgresDialect());
    this._ensureEngine('postgres');
  }

  protected getIntrospectAttributes(): Promise<Attributes> {
    // from https://www.postgresql.org/docs/9.1/static/infoschema-element-types.html
    return toArray(
      this.requester({
        query: `SELECT c.column_name as "name", c.data_type as "sqlType", e.data_type AS "arrayType"
       FROM information_schema.columns c LEFT JOIN information_schema.element_types e
       ON ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier)
       = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))
       WHERE table_name = ${this.dialect.escapeLiteral(this.source as string)}`,
      }),
    ).then(PostgresExternal.postProcessIntrospect);
  }
}

External.register(PostgresExternal);
