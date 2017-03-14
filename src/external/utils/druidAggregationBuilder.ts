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

import * as hasOwnProp from 'has-own-prop';
import { NamedArray } from 'immutable-class';

import {
  $,
  Expression,
  LiteralExpression,
  RefExpression,
  ChainableExpression,
  ChainableUnaryExpression,

  AbsoluteExpression,
  AddExpression,
  AndExpression,
  ApplyExpression,
  AverageExpression,
  CardinalityExpression,
  CastExpression,
  CollectExpression,
  ConcatExpression,
  ContainsExpression,
  CountExpression,
  CountDistinctExpression,
  CustomAggregateExpression,
  CustomTransformExpression,
  DivideExpression,
  ExtractExpression,
  FallbackExpression,
  FilterExpression,
  GreaterThanExpression,
  GreaterThanOrEqualExpression,
  IsExpression,
  JoinExpression,
  LengthExpression,
  IndexOfExpression,
  LookupExpression,
  LimitExpression,
  MatchExpression,
  MaxExpression,
  MinExpression,
  MultiplyExpression,
  NotExpression,
  NumberBucketExpression,
  OrExpression,
  PowerExpression,
  QuantileExpression,
  SplitExpression,
  SubstrExpression,
  SubtractExpression,
  SumExpression,
  TimeBucketExpression,
  TimeFloorExpression,
  TimePartExpression,
  TimeRangeExpression,
  TimeShiftExpression,
  TransformCaseExpression
} from '../../expressions/index';

import { AttributeInfo } from '../../datatypes/index';

import { External } from '../baseExternal';
import { CustomDruidAggregations, CustomDruidTransforms } from './druidTypes';
import { DruidFilterBuilder } from './druidFilterBuilder';


export interface AggregationsAndPostAggregations {
  aggregations: Druid.Aggregation[];
  postAggregations: Druid.PostAggregation[];
}

export interface DruidAggregationBuilderOptions {
  version: string;
  rawAttributes: AttributeInfo[];
  timeAttribute: string;
  derivedAttributes: Lookup<Expression>;
  customAggregations: CustomDruidAggregations;
  customTransforms: CustomDruidTransforms;
  rollup: boolean;
  exactResultsOnly: boolean;
  allowEternity: boolean;
}

export class DruidAggregationBuilder {
  static AGGREGATE_TO_FUNCTION: Lookup<Function> = {
    sum: (a: string, b: string) => `${a}+${b}`,
    min: (a: string, b: string) => `Math.min(${a},${b})`,
    max: (a: string, b: string) => `Math.max(${a},${b})`
  };

  static AGGREGATE_TO_ZERO: Lookup<string> = {
    sum: "0",
    min: "Infinity",
    max: "-Infinity"
  };


  public version: string;
  public rawAttributes: AttributeInfo[];
  public timeAttribute: string;
  public derivedAttributes: Lookup<Expression>;
  public customAggregations: CustomDruidAggregations;
  public customTransforms: CustomDruidTransforms;
  public rollup: boolean;
  public exactResultsOnly: boolean;
  public allowEternity: boolean;

  constructor(options: DruidAggregationBuilderOptions) {
    this.version = options.version;
    this.rawAttributes = options.rawAttributes;
    this.timeAttribute = options.timeAttribute;
    this.derivedAttributes = options.derivedAttributes;
    this.customAggregations = options.customAggregations;
    this.customTransforms = options.customTransforms;
    this.rollup = options.rollup;
    this.exactResultsOnly = options.exactResultsOnly;
    this.allowEternity = options.allowEternity;
  }

  public makeAggregationsAndPostAggregations(applies: ApplyExpression[]): AggregationsAndPostAggregations {
    let { aggregateApplies, postAggregateApplies } = External.segregationAggregateApplies(
      applies.map(apply => {
        let expression = apply.expression;
        expression = this.switchToRollupCount(this.inlineDerivedAttributesInAggregate(expression).decomposeAverage()).distribute();
        return apply.changeExpression(expression);
      })
    );

    let aggregations: Druid.Aggregation[] = [];
    let postAggregations: Druid.PostAggregation[] = [];

    for (let aggregateApply of aggregateApplies) {
      this.applyToAggregation(aggregateApply, aggregations, postAggregations);
    }

    for (let postAggregateApply of postAggregateApplies) {
      this.applyToPostAggregation(postAggregateApply, aggregations, postAggregations);
    }

    return {
      aggregations,
      postAggregations
    };
  }


  private applyToAggregation(action: ApplyExpression, aggregations: Druid.Aggregation[], postAggregations: Druid.PostAggregation[]): void {
    const { name, expression } = action;
    aggregations.push(this.expressionToAggregation(name, expression, postAggregations));
  }

  private applyToPostAggregation(apply: ApplyExpression, aggregations: Druid.Aggregation[], postAggregations: Druid.PostAggregation[]): void {
    let postAgg = this.expressionToPostAggregation(apply.expression, aggregations, postAggregations);
    postAgg.name = apply.name;
    postAggregations.push(postAgg);
  }

  // -----------------------------------

  private filterAggregateIfNeeded(datasetExpression: Expression, aggregator: Druid.Aggregation): Druid.Aggregation {
    if (datasetExpression instanceof FilterExpression) {
      return {
        type: "filtered",
        name: aggregator.name,
        filter: new DruidFilterBuilder(this).timelessFilterToFilter(datasetExpression.expression, true),
        aggregator
      };

    } else if (datasetExpression instanceof RefExpression) {
      return aggregator;

    } else {
      throw new Error(`could not construct aggregate on ${datasetExpression}`);

    }
  }

  private expressionToAggregation(name: string, expression: Expression, postAggregations: Druid.PostAggregation[]) {
    if (expression instanceof CountExpression) {
      return this.countToAggregation(name, expression);

    } else if (expression instanceof SumExpression || expression instanceof MinExpression || expression instanceof MaxExpression) {
      return this.sumMinMaxToAggregation(name, expression);

    } else if (expression instanceof CountDistinctExpression) {
      return this.countDistinctToAggregation(name, expression, postAggregations);

    } else if (expression instanceof QuantileExpression) {
      return this.quantileToAggregation(name, expression, postAggregations);

    } else if (expression instanceof CustomAggregateExpression) {
      return this.customAggregateToAggregation(name, expression);

    } else {
      throw new Error(`unsupported aggregate action ${expression} (as ${name})`);

    }
  }

  private countToAggregation(name: string, expression: CountExpression): Druid.Aggregation {
    return this.filterAggregateIfNeeded(expression.operand, {
      name,
      type: 'count'
    });
  }

  private sumMinMaxToAggregation(name: string, expression: SumExpression | MinExpression | MaxExpression): Druid.Aggregation {
    let aggregation: Druid.Aggregation;

    let aggregateExpression = expression.expression;
    if (aggregateExpression instanceof RefExpression) {
      let refName = aggregateExpression.name;
      let attributeInfo = this.getAttributesInfo(refName);
      if (attributeInfo.nativeType === 'STRING') {
        aggregation = this.makeJavaScriptAggregation(name, expression);
      } else {
        let op = expression.op;
        aggregation = {
          name,
          type: (attributeInfo.nativeType === 'LONG' ? 'long' : 'double') + op[0].toUpperCase() + op.substr(1),
          fieldName: refName
        };
      }
    } else {
      aggregation = this.makeJavaScriptAggregation(name, expression);
    }

    return this.filterAggregateIfNeeded(expression.operand, aggregation);
  }

  private isCardinalityCrossProductExpression(expression: Expression) {
    return expression.every(ex => {
      if (ex instanceof RefExpression || ex instanceof LiteralExpression) return true;
      if (ex instanceof ConcatExpression || ex instanceof CastExpression) return null; // search within
      return false;
    });
  }

  private countDistinctToAggregation(name: string, expression: CountDistinctExpression, postAggregations: Druid.PostAggregation[]): Druid.Aggregation {
    if (this.exactResultsOnly) {
      throw new Error("approximate query not allowed");
    }

    let aggregation: Druid.Aggregation;
    let attribute = expression.expression;
    if (attribute instanceof RefExpression) {
      let attributeName = attribute.name;

      let attributeInfo = this.getAttributesInfo(attributeName);
      if (attributeInfo.nativeType === 'hyperUnique') {
        aggregation = {
          name: name,
          type: "hyperUnique",
          fieldName: attributeName
        };

      } else if (attributeInfo.nativeType === 'thetaSketch') {
        let tempName = '!Theta_' + name;
        postAggregations.push({
          type: "thetaSketchEstimate",
          name: name,
          field: { type: 'fieldAccess', fieldName: tempName }
        });

        aggregation = {
          name: tempName,
          type: "thetaSketch",
          fieldName: attributeName
        };

      } else {
        aggregation = {
          name: name,
          type: "cardinality",
          fieldNames: [attributeName]
        };

      }
    } else if (attribute.type === 'STRING' || attribute.type === 'NUMBER') {
      if (this.isCardinalityCrossProductExpression(attribute)) {
        aggregation = {
          name: name,
          type: "cardinality",
          fieldNames: attribute.getFreeReferences(),
          byRow: true
        };
      } else {
        throw new Error(`can not compute countDistinct on ${attribute}`);

      }

    } else {
      throw new Error(`can not compute countDistinct on ${attribute}`);

    }

    return this.filterAggregateIfNeeded(expression.operand, aggregation);
  }

  private customAggregateToAggregation(name: string, expression: CustomAggregateExpression): Druid.Aggregation {
    let customAggregationName = expression.custom;
    let customAggregation = this.customAggregations[customAggregationName];
    if (!customAggregation) throw new Error(`could not find '${customAggregationName}'`);
    let aggregationObj = customAggregation.aggregation;
    if (typeof aggregationObj.type !== 'string') throw new Error(`must have type in custom aggregation '${customAggregationName}'`);
    try {
      aggregationObj = JSON.parse(JSON.stringify(aggregationObj));
    } catch (e) {
      throw new Error(`must have JSON custom aggregation '${customAggregationName}'`);
    }
    aggregationObj.name = name;
    return aggregationObj;
  }

  private quantileToAggregation(name: string, expression: QuantileExpression, postAggregations: Druid.PostAggregation[]): Druid.Aggregation {
    if (this.exactResultsOnly) {
      throw new Error("approximate query not allowed");
    }

    let attribute = expression.expression;
    let attributeName: string;
    if (attribute instanceof RefExpression) {
      attributeName = attribute.name;
    } else {
      throw new Error(`can not compute quantile on derived attribute: ${attribute}`);
    }

    let histogramAggregationName = "!H_" + name;
    let aggregation: Druid.Aggregation = {
      name: histogramAggregationName,
      type: "approxHistogramFold",
      fieldName: attributeName
    };

    postAggregations.push({
      name,
      type: "quantile",
      fieldName: histogramAggregationName,
      probability: expression.value
    });

    return aggregation;
  }

  private makeJavaScriptAggregation(name: string, aggregate: Expression): Druid.Aggregation {
    if (aggregate instanceof ChainableUnaryExpression) {
      let aggregateType = aggregate.op;
      let aggregateExpression = aggregate.expression;

      let aggregateFunction = DruidAggregationBuilder.AGGREGATE_TO_FUNCTION[aggregateType];
      if (!aggregateFunction) throw new Error(`Can not convert ${aggregateType} to JS`);
      let zero = DruidAggregationBuilder.AGGREGATE_TO_ZERO[aggregateType];
      let fieldNames = aggregateExpression.getFreeReferences();
      let simpleFieldNames = fieldNames.map(RefExpression.toJavaScriptSafeName);
      return {
        name,
        type: "javascript",
        fieldNames: fieldNames,
        fnAggregate: `function($$,${simpleFieldNames.join(',')}) { return ${aggregateFunction('$$', aggregateExpression.getJS(null))}; }`,
        fnCombine: `function(a,b) { return ${aggregateFunction('a', 'b')}; }`,
        fnReset: `function() { return ${zero}; }`
      };
    } else {
      throw new Error(`Can not convert ${aggregate} to JS aggregate`);
    }
  }

  // ------------------------------

  private getAccessTypeForAggregation(aggregationType: string): string {
    if (aggregationType === 'hyperUnique' || aggregationType === 'cardinality') return 'hyperUniqueCardinality';

    let customAggregations = this.customAggregations;
    for (let customName in customAggregations) {
      if (!hasOwnProp(customAggregations, customName)) continue;
      let customAggregation = customAggregations[customName];
      if (customAggregation.aggregation.type === aggregationType) {
        return customAggregation.accessType || 'fieldAccess';
      }
    }
    return 'fieldAccess';
  }

  private getAccessType(aggregations: Druid.Aggregation[], aggregationName: string): string {
    for (let aggregation of aggregations) {
      if (aggregation.name === aggregationName) {
        let aggregationType = aggregation.type;
        if (aggregationType === 'filtered') aggregationType = aggregation.aggregator.type;
        return this.getAccessTypeForAggregation(aggregationType);
      }
    }
    return 'fieldAccess'; // If not found it must be a post-agg
  }

  private expressionToPostAggregation(ex: Expression, aggregations: Druid.Aggregation[], postAggregations: Druid.PostAggregation[]): Druid.PostAggregation {
    if (ex instanceof RefExpression) {
      let refName = ex.name;
      return {
        type: this.getAccessType(aggregations, refName),
        fieldName: refName
      };

    } else if (ex instanceof LiteralExpression) {
      if (ex.type !== 'NUMBER') throw new Error("must be a NUMBER type");
      return {
        type: 'constant',
        value: ex.value
      };

    } else if (
      ex instanceof AbsoluteExpression ||
      ex instanceof PowerExpression ||
      ex instanceof FallbackExpression ||
      ex instanceof CastExpression ||
      ex instanceof IndexOfExpression ||
      ex instanceof TransformCaseExpression
    ) {
      let fieldNameRefs = ex.getFreeReferences();
      let fieldNames = fieldNameRefs.map(fieldNameRef => {
        let accessType = this.getAccessType(aggregations, fieldNameRef);
        if (accessType === 'fieldAccess') return fieldNameRef;
        let fieldNameRefTemp = '!F_' + fieldNameRef;
        postAggregations.push({
          name: fieldNameRefTemp,
          type: accessType,
          fieldName: fieldNameRef
        });
        return fieldNameRefTemp;
      });

      return {
        type: 'javascript',
        fieldNames: fieldNames,
        'function': `function(${fieldNameRefs.map(RefExpression.toJavaScriptSafeName)}) { return ${ex.getJS(null)}; }`
      };

    } else if (ex instanceof AddExpression) {
      return {
        type: 'arithmetic',
        fn: '+',
        fields: ex.getExpressionList().map(e => this.expressionToPostAggregation(e, aggregations, postAggregations))
      };

    } else if (ex instanceof SubtractExpression) {
      return {
        type: 'arithmetic',
        fn: '-',
        fields: ex.getExpressionList().map(e => this.expressionToPostAggregation(e, aggregations, postAggregations))
      };

    } else if (ex instanceof MultiplyExpression) {
      return {
        type: 'arithmetic',
        fn: '*',
        fields: ex.getExpressionList().map(e => this.expressionToPostAggregation(e, aggregations, postAggregations))
      };

    } else if (ex instanceof DivideExpression) {
      return {
        type: 'arithmetic',
        fn: '/',
        fields: ex.getExpressionList().map(e => this.expressionToPostAggregation(e, aggregations, postAggregations))
      };

    } else {
      throw new Error(`can not convert expression to post agg: ${ex}`);
    }
  }

  private switchToRollupCount(expression: Expression): Expression {
    if (!this.rollup) return expression;

    let countRef: RefExpression = null;
    return expression.substitute((ex) => {
      if (ex instanceof CountExpression) {
        if (!countRef) countRef = $(this.getRollupCountName(), 'NUMBER');
        return ex.operand.sum(countRef);
      }
      return null;
    });
  }

  private getRollupCountName(): string {
    const { rawAttributes } = this;
    for (let attribute of rawAttributes) {
      let maker = attribute.maker;
      if (maker && maker.op === 'count') return attribute.name;
    }
    throw new Error(`could not find rollup count`);
  }

  private inlineDerivedAttributes(expression: Expression): Expression {
    const { derivedAttributes } = this;
    return expression.substitute(refEx => {
      if (refEx instanceof RefExpression) {
        return derivedAttributes[refEx.name] || null;
      } else {
        return null;
      }
    });
  }

  private inlineDerivedAttributesInAggregate(expression: Expression): Expression {
    return expression.substitute((ex) => {
      if (ex.isAggregate()) {
        return this.inlineDerivedAttributes(ex);
      }
      return null;
    });
  }

  public getAttributesInfo(attributeName: string) {
    return NamedArray.get(this.rawAttributes, attributeName);
  }
}
