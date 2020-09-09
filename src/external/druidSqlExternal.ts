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

import { Introspect, QueryResult, SqlQuery, ColumnInfo } from 'druid-query-toolkit';
import { PlywoodRequester } from 'plywood-base-api';
import * as toArray from 'stream-to-array';
import { AttributeInfo, Attributes, PseudoDatum } from '../datatypes';
import { DruidDialect } from '../dialect';
import { dictEqual } from '../helper';
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
    value.context = parameters.context;
    return new DruidSQLExternal(value);
  }

  static postProcessIntrospect(columns: ColumnInfo[]): Attributes {
    // only support BIGINT, FLOAT, VARCHAR, TIMESTAMP, DATE, OTHER
    return columns
      .map((column: ColumnInfo) => {
        let name = column.name;
        let type: PlyType;
        let nativeType = column.type;
        switch (nativeType) {
          case 'TIMESTAMP':
          case 'DATE':
            type = 'TIME';
            break;

          case 'VARCHAR':
          case 'STRING':
            type = 'STRING';
            break;

          case 'DOUBLE':
          case 'FLOAT':
          case 'BIGINT':
          case 'LONG':
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

  static async getSourceList(requester: PlywoodRequester<any>): Promise<string[]> {
    const sources = await toArray(
      requester({
        query: {
          query: Introspect.getTableIntrospectionQuery(),
        },
      }),
    );

    return Introspect.decodeTableIntrospectionResult(QueryResult.fromRawResult(sources)).map(s => s.name).sort();
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

  public context: Record<string, any>;

  constructor(parameters: ExternalValue) {
    super(parameters, new DruidDialect());
    this._ensureEngine('druidsql');
    this.context = parameters.context;
  }

  public valueOf(): ExternalValue {
    let value: ExternalValue = super.valueOf();
    value.context = this.context;
    return value;
  }

  public toJS(): ExternalJS {
    let js: ExternalJS = super.toJS();
    if (this.context) js.context = this.context;
    return js;
  }

  public equals(other: DruidSQLExternal | undefined): boolean {
    return (
      super.equals(other) &&
      dictEqual(this.context, other.context)
    );
  }

  // -----------------

  protected async getIntrospectAttributes(): Promise<Attributes> {
    const { source, withQuery } = this;

    let sampleResult: QueryResult | undefined;
    let introspectQuery: SqlQuery;
    if (withQuery) {
      let withQueryParsed: SqlQuery;
      try {
        withQueryParsed = SqlQuery.parse(withQuery);
      } catch (e) {
        throw new Error(`could not parse withQuery: ${e.message}`);
      }
      introspectQuery = Introspect.getQueryColumnIntrospectionQuery(withQueryParsed);

      if (withQueryParsed.hasStarInSelect()) {
        // Query for sample also
        const sampleRawResult = await toArray(
          this.requester({ query: this.sqlToQuery(String(Introspect.getQueryColumnSampleQuery(withQueryParsed))) }),
        );

        sampleResult = QueryResult.fromRawResult(sampleRawResult);
      }
    } else {
      let table: string;
      if (Array.isArray(source)) {
        table = source[0];
      } else {
        table = source;
      }
      introspectQuery = Introspect.getTableColumnIntrospectionQuery(table);
    }

    const query = this.sqlToQuery(String(introspectQuery));

    const result = await toArray(
      this.requester({ query }),
    );

    const queryResult = QueryResult.fromRawResult(result).attachQuery(query, introspectQuery);

    return DruidSQLExternal.postProcessIntrospect(Introspect.decodeColumnIntrospectionResult(queryResult, sampleResult));
  }

  protected sqlToQuery(sql: string): any {
    const payload: any = {
      query: sql,
    };

    if (this.context) {
      payload.context = this.context;
    }

    return payload;
  }

  protected capability(cap: string): boolean {
    if (cap === 'filter-on-attribute') return false;
    return super.capability(cap);
  }
}

External.register(DruidSQLExternal);
