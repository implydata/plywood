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
import { PlywoodValue, Dataset } from '../datatypes/index';
import {
  Expression,
  ApplyExpression,
  LimitExpression,
  NumberBucketExpression,
  SortExpression,
  SplitExpression,
  FilterExpression,
  TimeBucketExpression
} from '../expressions/index';
import { Attributes } from '../datatypes/attributeInfo';
import { External, ExternalValue, Inflater, QueryAndPostProcess, PostProcess, TotalContainer } from './baseExternal';
import { SQLDialect } from '../dialect/baseDialect';

function correctResult(result: any[]): boolean {
  return Array.isArray(result) && (result.length === 0 || typeof result[0] === 'object');
}

function getSplitInflaters(split: SplitExpression): Inflater[] {
  return split.mapSplits((label, splitExpression) => {
    let simpleInflater = External.getSimpleInflater(splitExpression, label);
    if (simpleInflater) return simpleInflater;

    if (splitExpression instanceof TimeBucketExpression) {
      return External.timeRangeInflaterFactory(label, splitExpression.duration, splitExpression.getTimezone());
    }

    if (splitExpression instanceof NumberBucketExpression) {
      return External.numberRangeInflaterFactory(label, splitExpression.size);
    }

    return undefined;
  });
}

function valuePostProcess(data: any[]): PlywoodValue {
  if (!correctResult(data)) {
    let err = new Error("unexpected result (value)");
    (<any>err).result = data; // ToDo: special error type
    throw err;
  }

  return data.length ? data[0][External.VALUE_NAME] : 0;
}

function totalPostProcessFactory(inflaters: Inflater[], zeroTotalApplies: ApplyExpression[]): PostProcess {
  return (data: any[]): TotalContainer => {
    if (!correctResult(data)) {
      let err = new Error("unexpected total result");
      (<any>err).result = data; // ToDo: special error type
      throw err;
    }

    let datum: any;
    if (data.length === 1) {
      datum = data[0];
      for (let inflater of inflaters) {
        inflater(datum);
      }
    } else if (zeroTotalApplies) {
      datum = External.makeZeroDatum(zeroTotalApplies);
    }

    return new TotalContainer(datum);
  };
}

function postProcessFactory(inflaters: Inflater[], zeroTotalApplies: ApplyExpression[]): PostProcess {
  return (data: any[]): Dataset => {
    if (!correctResult(data)) {
      let err = new Error("unexpected result");
      (<any>err).result = data; // ToDo: special error type
      throw err;
    }

    let n = data.length;
    for (let inflater of inflaters) {
      for (let i = 0; i < n; i++) {
        inflater(data[i]);
      }
    }

    if (n === 0 && zeroTotalApplies) {
      data = [External.makeZeroDatum(zeroTotalApplies)];
    }

    return new Dataset({ data });
  };
}

export abstract class SQLExternal extends External {
  static type = 'DATASET';

  public dialect: SQLDialect;

  constructor(parameters: ExternalValue, dialect: SQLDialect) {
    super(parameters, dummyObject);
    this.dialect = dialect;
  }

  // -----------------

  public canHandleTotal(): boolean {
    return true;
  }

  public canHandleFilter(filter: FilterExpression): boolean {
    return true;
  }

  public canHandleSplit(split: SplitExpression): boolean {
    return true;
  }

  public canHandleSplitExpression(ex: Expression): boolean {
    return true;
  }

  public canHandleApply(apply: ApplyExpression): boolean {
    return true;
  }

  public canHandleSort(sort: SortExpression): boolean {
    return true;
  }

  public canHandleLimit(limit: LimitExpression): boolean {
    return true;
  }

  public canHandleHavingFilter(havingFilter: FilterExpression): boolean {
    return true;
  }

  // -----------------

  public getQueryAndPostProcess(): QueryAndPostProcess<string> {
    const { source, mode, applies, sort, limit, derivedAttributes, dialect } = this;

    let query = ['SELECT'];
    let postProcess: PostProcess = null;
    let inflaters: Inflater[] = [];
    let zeroTotalApplies: ApplyExpression[] = null;

    let from = "FROM " + this.dialect.escapeName(source as string);
    let filter = this.getQueryFilter();
    if (!filter.equals(Expression.TRUE)) {
      from += '\nWHERE ' + filter.getSQL(dialect);
    }

    switch (mode) {
      case 'raw':
        let selectedAttributes = this.getSelectedAttributes();

        selectedAttributes.forEach(attribute => {
          let { name, type } = attribute;
          switch (type) {
            case 'BOOLEAN':
              inflaters.push(External.booleanInflaterFactory(name));
              break;

            case 'SET/STRING':
              inflaters.push(External.setStringInflaterFactory(name));
              break;
          }
        });

        query.push(
          selectedAttributes.map(a => {
            let name = a.name;
            if (derivedAttributes[name]) {
              return Expression._.apply(name, derivedAttributes[name]).getSQL(dialect);
            } else {
              return dialect.escapeName(name);
            }
          }).join(', '),
          from
        );
        if (sort) {
          query.push(sort.getSQL(dialect));
        }
        if (limit) {
          query.push(limit.getSQL(dialect));
        }
        break;

      case 'value':
        query.push(
          this.toValueApply().getSQL(dialect),
          from,
          dialect.constantGroupBy()
        );
        postProcess = valuePostProcess;
        break;

      case 'total':
        zeroTotalApplies = applies;
        query.push(
          applies.map(apply => apply.getSQL(dialect)).join(',\n'),
          from,
          dialect.constantGroupBy()
        );
        postProcess = totalPostProcessFactory(inflaters, zeroTotalApplies);
        break;

      case 'split':
        let split = this.getQuerySplit();
        query.push(
          split.getSelectSQL(dialect)
            .concat(applies.map(apply => apply.getSQL(dialect)))
            .join(',\n'),
          from,
          split.getShortGroupBySQL()
        );
        if (!(this.havingFilter.equals(Expression.TRUE))) {
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
      query: query.join('\n'),
      postProcess: postProcess || postProcessFactory(inflaters, zeroTotalApplies)
    };
  }

  protected abstract getIntrospectAttributes(): Promise<Attributes>
}
