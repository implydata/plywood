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

import { Transform } from 'readable-stream';
import { Attributes } from '../datatypes/attributeInfo';
import { SQLDialect } from '../dialect/baseDialect';
import {
  ApplyExpression,
  Expression,
  FilterExpression,
  LimitExpression,
  NumberBucketExpression,
  SortExpression,
  SplitExpression,
  TimeBucketExpression,
} from '../expressions/index';
import { External, ExternalValue, Inflater, QueryAndPostTransform } from './baseExternal';

function getSplitInflaters(split: SplitExpression): Inflater[] {
  return split.mapSplits((label, splitExpression) => {
    let simpleInflater = External.getInteligentInflater(splitExpression, label);
    if (simpleInflater) return simpleInflater;
    return undefined;
  });
}

export abstract class SQLExternal extends External {
  static type = 'DATASET';

  public dialect: SQLDialect;

  constructor(parameters: ExternalValue, dialect: SQLDialect) {
    super(parameters, dummyObject);
    this.dialect = dialect;
  }

  // -----------------

  public canHandleFilter(filter: FilterExpression): boolean {
    return true;
  }

  public canHandleSort(sort: SortExpression): boolean {
    return true;
  }

  protected capability(cap: string): boolean {
    if (cap === 'filter-on-attribute' || cap === 'shortcut-group-by') return true;
    return super.capability(cap);
  }

  // -----------------

  protected sqlToQuery(sql: string): any {
    return sql;
  }

  protected getFrom(): string {
    const { source, dialect } = this;
    const m = String(source).match(/^(\w+)\.(.+)$/);
    if (m) {
      return `FROM ${m[1]}.${dialect.escapeName(m[2])} AS t`;
    } else {
      return `FROM ${dialect.escapeName(source as string)} AS t`;
    }
  }

  public getQueryAndPostTransform(): QueryAndPostTransform<string> {
    const { mode, applies, sort, limit, derivedAttributes, dialect } = this;

    let query = ['SELECT'];
    let postTransform: Transform = null;
    let inflaters: Inflater[] = [];
    let keys: string[] = null;
    let zeroTotalApplies: ApplyExpression[] = null;

    //dialect.setTable(null);
    let from = this.getFrom();
    //dialect.setTable(source as string);

    let filter = this.getQueryFilter();
    if (!filter.equals(Expression.TRUE)) {
      from += '\nWHERE ' + filter.getSQL(dialect);
    }

    let selectedAttributes = this.getSelectedAttributes();
    switch (mode) {
      case 'raw':
        selectedAttributes = selectedAttributes.map(a => a.dropOriginInfo());

        inflaters = selectedAttributes
          .map(attribute => {
            let { name, type } = attribute;
            switch (type) {
              case 'BOOLEAN':
                return External.booleanInflaterFactory(name);

              case 'TIME':
                return External.timeInflaterFactory(name);

              case 'SET/STRING':
                return External.setStringInflaterFactory(name);

              default:
                return null;
            }
          })
          .filter(Boolean);

        query.push(
          selectedAttributes
            .map(a => {
              let name = a.name;
              if (derivedAttributes[name]) {
                return Expression._.apply(name, derivedAttributes[name]).getSQL(dialect);
              } else {
                return dialect.escapeName(name);
              }
            })
            .join(', '),
          from,
        );
        if (sort) {
          query.push(sort.getSQL(dialect));
        }
        if (limit) {
          query.push(limit.getSQL(dialect));
        }
        break;

      case 'value':
        query.push(this.toValueApply().getSQL(dialect), from, dialect.constantGroupBy());
        postTransform = External.valuePostTransformFactory();
        break;

      case 'total':
        zeroTotalApplies = applies;
        inflaters = applies
          .map(apply => {
            let { name, expression } = apply;
            return External.getSimpleInflater(expression.type, name);
          })
          .filter(Boolean);

        keys = [];
        query.push(
          applies.map(apply => apply.getSQL(dialect)).join(',\n'),
          from,
          dialect.constantGroupBy(),
        );
        break;

      case 'split':
        let split = this.getQuerySplit();
        keys = split.mapSplits(name => name);
        query.push(
          split
            .getSelectSQL(dialect)
            .concat(applies.map(apply => apply.getSQL(dialect)))
            .join(',\n'),
          from,
          'GROUP BY ' +
            (this.capability('shortcut-group-by')
              ? split.getShortGroupBySQL()
              : split.getGroupBySQL(dialect)
            ).join(','),
        );
        if (!this.havingFilter.equals(Expression.TRUE)) {
          query.push('HAVING ' + this.havingFilter.getSQL(dialect));
        }
        if (sort) {
          query.push(sort.getSQL(dialect));
        }
        if (limit) {
          query.push(limit.getSQL(dialect));
        }
        inflaters = getSplitInflaters(split);
        break;

      default:
        throw new Error(`can not get query for mode: ${mode}`);
    }

    return {
      query: this.sqlToQuery(query.join('\n')),
      postTransform:
        postTransform ||
        External.postTransformFactory(inflaters, selectedAttributes, keys, zeroTotalApplies),
    };
  }

  protected abstract getIntrospectAttributes(): Promise<Attributes>;
}
