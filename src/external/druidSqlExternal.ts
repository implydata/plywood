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
import { AttributeInfo, Attributes, PseudoDatum } from '../datatypes';
import { DruidDialect } from '../dialect';
import { PlyType } from '../types';
import { External, ExternalJS, ExternalValue } from './baseExternal';
import { SQLExternal } from './sqlExternal';

export interface DruidSQLDescribeRow {
  COLUMN_NAME: string;
  DATA_TYPE: string;
}

export class DruidSQLExternal extends SQLExternal {
  static engine = 'druidsql';
  static type = 'DATASET';

  static fromJS(parameters: ExternalJS, requester: PlywoodRequester<any>): DruidSQLExternal {
    let value: ExternalValue = SQLExternal.jsToValue(parameters, requester);
    return new DruidSQLExternal(value);
  }

  static postProcessIntrospect(columns: DruidSQLDescribeRow[]): Attributes {
    // only support BIGINT, FLOAT, VARCHAR, TIMESTAMP, DATE, OTHER
    return columns
      .map((column: DruidSQLDescribeRow) => {
        let name = column.COLUMN_NAME;
        let type: PlyType;
        let nativeType = column.DATA_TYPE;
        switch (nativeType) {
          case 'TIMESTAMP':
          case 'DATE':
            type = 'TIME';
            break;

          case 'VARCHAR':
            type = 'STRING';
            break;

          case 'DOUBLE':
          case 'FLOAT':
          case 'BIGINT':
            type = 'NUMBER';
            break;

          default:
            // OTHER
            type = 'NULL';
            break;
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
        query: {
          query: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'druid' AND TABLE_TYPE = 'TABLE'`,
        },
      }),
    ).then(sources => {
      if (!sources.length) return sources;
      return sources.map((s: PseudoDatum) => s['TABLE_NAME']).sort();
    });
  }

  static getVersion(requester: PlywoodRequester<any>): Promise<string> {
    return toArray(
      requester({
        query: {
          queryType: 'status',
        },
      }),
    ).then(res => {
      return res[0].version;
    });
  }

  constructor(parameters: ExternalValue) {
    super(parameters, new DruidDialect());
    this._ensureEngine('druidsql');
  }

  protected getIntrospectAttributes(): Promise<Attributes> {
    // from http://druid.io/docs/0.10.0-rc1/querying/sql.html
    return toArray(
      this.requester({
        query: {
          query: `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = ${this.dialect.escapeLiteral(
            this.source as string,
          )}`,
        },
      }),
    ).then(DruidSQLExternal.postProcessIntrospect);
  }

  protected sqlToQuery(sql: string): any {
    return {
      query: sql,
      // context: {
      //   useCache: false,
      //   populateCache: false
      // }
    };
  }

  protected capability(cap: string): boolean {
    if (cap === 'filter-on-attribute') return false;
    return super.capability(cap);
  }
}

External.register(DruidSQLExternal);
