/*
 * Copyright 2016-2017 Imply Data, Inc.
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

import { AttributeInfo, NumberRange } from '../../datatypes/index';
import {
  AndExpression,
  Expression,
  IsExpression,
  LiteralExpression,
  NotExpression,
  OrExpression,
  OverlapExpression,
  RefExpression
} from '../../expressions/index';
import { External } from '../baseExternal';
import { DruidFilterBuilder } from './druidFilterBuilder';
import { CustomDruidTransforms } from './druidTypes';


export interface DruidHavingFilterBuilderOptions {
  version: string;
  attributes: AttributeInfo[];
  customTransforms: CustomDruidTransforms;
}

export class DruidHavingFilterBuilder {

  public version: string;
  public attributes: AttributeInfo[];
  public customTransforms: CustomDruidTransforms;

  constructor(options: DruidHavingFilterBuilderOptions) {
    this.version = options.version;
    this.attributes = options.attributes;
    this.customTransforms = options.customTransforms;
  }

  public filterToHavingFilter(filter: Expression): Druid.Having {
    if (this.versionBefore('0.10.0')) {
      return this.filterToLegacyHavingFilter(filter);
    } else {
      return {
        type: 'filter',
        filter: new DruidFilterBuilder({
          version: this.version,
          rawAttributes: this.attributes,
          timeAttribute: '***',
          allowEternity: true,
          customTransforms: this.customTransforms
        }).timelessFilterToFilter(filter, false)
      };
    }
  }

  public filterToLegacyHavingFilter(filter: Expression): Druid.Having {
    if (filter instanceof LiteralExpression) {
      if (filter.value === true) {
        return null;
      } else {
        throw new Error("should never get here");
      }

    } else if (filter instanceof NotExpression) {
      return {
        type: 'not',
        havingSpec: this.filterToHavingFilter(filter.operand)
      };

    } else if (filter instanceof AndExpression) {
      return {
        type: 'and',
        havingSpecs: filter.getExpressionList().map(this.filterToHavingFilter, this)
      };

    } else if (filter instanceof OrExpression) {
      return {
        type: 'or',
        havingSpecs: filter.getExpressionList().map(this.filterToHavingFilter, this)
      };

    } else if (filter instanceof IsExpression) {
      const { operand: lhs, expression: rhs } = filter;
      if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
        let rhsType = rhs.type;
        if (rhsType === 'STRING' || rhsType === 'NUMBER') {
          return {
            type: "equalTo",
            aggregation: lhs.name,
            value: rhs.value
          };

        } else if (rhsType === 'SET/STRING' || rhsType === 'SET/NUMBER') {
          return {
            type: "or",
            havingSpecs: rhs.value.elements.map((value: string) => {
              return {
                type: "equalTo",
                aggregation: (lhs as RefExpression).name,
                value: value
              };
            })
          };

        }

      } else {
        throw new Error(`can not convert ${filter} to Druid having filter`);
      }

    } else if (filter instanceof OverlapExpression) {
      const { operand: lhs, expression: rhs } = filter;
      if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
        let rhsType = rhs.type;
        if (rhsType === 'SET/NUMBER_RANGE') {
          return {
            type: "or",
            havingSpecs: rhs.value.elements.map((value: NumberRange) => {
              return this.inToHavingFilter((lhs as RefExpression).name, value);
            }, this)
          };

        } else if (rhsType === 'NUMBER_RANGE') {
          return this.inToHavingFilter(lhs.name, rhs.value);

        } else if (rhsType === 'TIME_RANGE') {
          throw new Error("can not compute having filter on time");

        } else {
          throw new Error(`not supported ${rhsType}`);
        }
      } else {
        throw new Error(`can not convert ${filter} to Druid having filter`);
      }

    }

    throw new Error(`could not convert filter ${filter} to Druid having filter`);
  }

  private makeHavingComparison(agg: string, op: string, value: number): Druid.Having {
    // Druid does not support <= and >= filters so... improvise.
    switch (op) {
      case '<':
        return { type: "lessThan", aggregation: agg, value: value };
      case '>':
        return { type: "greaterThan", aggregation: agg, value: value };
      case '<=':
        return { type: 'not', havingSpec: { type: "greaterThan", aggregation: agg, value: value } };
      case '>=':
        return { type: 'not', havingSpec: { type: "lessThan", aggregation: agg, value: value } };
      default:
        throw new Error(`unknown op: ${op}`);
    }
  }

  private inToHavingFilter(agg: string, range: NumberRange): Druid.Having {
    let havingSpecs: Druid.Having[] = [];
    if (range.start !== null) {
      havingSpecs.push(this.makeHavingComparison(agg, (range.bounds[0] === '[' ? '>=' : '>'), range.start));
    }
    if (range.end !== null) {
      havingSpecs.push(this.makeHavingComparison(agg, (range.bounds[1] === ']' ? '<=' : '<'), range.end));
    }
    return havingSpecs.length === 1 ? havingSpecs[0] : { type: 'and', havingSpecs };
  }

  private versionBefore(neededVersion: string): boolean {
    const { version } = this;
    return version && External.versionLessThan(version, neededVersion);
  }
}
