/*
 * Copyright 2016-2018 Imply Data, Inc.
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

import { AttributeInfo } from '../../datatypes/index';

import {
  $,
  AbsoluteExpression,
  AddExpression,
  ApplyExpression,
  CastExpression,
  ChainableUnaryExpression,
  ConcatExpression,
  CountDistinctExpression,
  CountExpression,
  CustomAggregateExpression,
  DivideExpression,
  Expression,
  FallbackExpression,
  FilterExpression,
  IndexOfExpression,
  LiteralExpression,
  MaxExpression,
  MinExpression,
  MultiplyExpression,
  PowerExpression,
  QuantileExpression,
  RefExpression,
  SubtractExpression,
  SumExpression,
  TransformCaseExpression
} from '../../expressions';

import { External } from '../baseExternal';
import { DruidExpressionBuilder } from './druidExpressionBuilder';
import { DruidExtractionFnBuilder } from './druidExtractionFnBuilder';
import { DruidFilterBuilder } from './druidFilterBuilder';
import { CustomDruidAggregations, CustomDruidTransforms } from './druidTypes';

const APPROX_HISTOGRAM_TUNINGS = [
  "resolution",
  "numBuckets",
  "lowerLimit",
  "upperLimit"
];

export interface AggregationsAndPostAggregations {
  aggregations: Druid.Aggregation[];
  postAggregations: Druid.PostAggregation[];
}

export interface DruidAggregationBuilderOptions {
  version: string;
  rawAttributes: AttributeInfo[];
  timeAttribute: string;
  derivedAttributes: Record<string, Expression>;
  customAggregations: CustomDruidAggregations;
  customTransforms: CustomDruidTransforms;
  rollup: boolean;
  exactResultsOnly: boolean;
  allowEternity: boolean;
  forceFinalize: boolean;
}

export class DruidAggregationBuilder {
  static AGGREGATE_TO_FUNCTION: Record<string, Function> = {
    sum: (a: string, b: string) => `${a}+${b}`,
    min: (a: string, b: string) => `Math.min(${a},${b})`,
    max: (a: string, b: string) => `Math.max(${a},${b})`
  };

  static AGGREGATE_TO_ZERO: Record<string, string> = {
    sum: "0",
    min: "Infinity",
    max: "-Infinity"
  };


  public version: string;
  public rawAttributes: AttributeInfo[];
  public timeAttribute: string;
  public derivedAttributes: Record<string, Expression>;
  public customAggregations: CustomDruidAggregations;
  public customTransforms: CustomDruidTransforms;
  public rollup: boolean;
  public exactResultsOnly: boolean;
  public allowEternity: boolean;
  public forceFinalize: boolean;

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
    this.forceFinalize = options.forceFinalize;
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
    this.expressionToAggregation(name, expression, aggregations, postAggregations);
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
        filter: new DruidFilterBuilder(this).timelessFilterToFilter(datasetExpression.expression),
        aggregator
      };

    } else if (datasetExpression instanceof RefExpression) {
      return aggregator;

    } else {
      throw new Error(`could not construct aggregate on ${datasetExpression}`);

    }
  }

  private expressionToAggregation(name: string, expression: Expression, aggregations: Druid.Aggregation[], postAggregations: Druid.PostAggregation[]): void {
    if (expression instanceof CountExpression) {
      aggregations.push(this.countToAggregation(name, expression));

    } else if (expression instanceof SumExpression || expression instanceof MinExpression || expression instanceof MaxExpression) {
      aggregations.push(this.sumMinMaxToAggregation(name, expression));

    } else if (expression instanceof CountDistinctExpression) {
      aggregations.push(this.countDistinctToAggregation(name, expression, postAggregations));

    } else if (expression instanceof QuantileExpression) {
      aggregations.push(this.quantileToAggregation(name, expression, postAggregations));

    } else if (expression instanceof CustomAggregateExpression) {
      this.customAggregateToAggregation(name, expression, aggregations, postAggregations);

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

  private getCardinalityExpressions(expression: Expression): Expression[] {
    if (expression instanceof LiteralExpression) {
      return [];

    } else if (expression instanceof CastExpression) {
      return [expression.operand];

    } else if (expression instanceof ConcatExpression) {
      const subEx = expression.getExpressionList().map(ex => this.getCardinalityExpressions(ex));
      return [].concat(...subEx);

    } else if (expression.getFreeReferences().length === 1) {
      return [expression];

    } else {
      throw new Error(`can not convert ${expression} to cardinality expressions`);

    }
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
        let tempName = '!Hyper_' + name;
        aggregation = {
          name: this.forceFinalize ? tempName : name,
          type: "hyperUnique",
          fieldName: attributeName
        };
        if (!this.versionBefore('0.10.1')) aggregation.round = true;
        if (this.forceFinalize) {
          postAggregations.push({
            type: 'finalizingFieldAccess',
            name,
            fieldName: tempName
          });
        }

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
        let tempName = '!Card_' + name;
        aggregation = {
          name: this.forceFinalize ? tempName : name,
          type: "cardinality",
          fields: [attributeName]
        };
        if (!this.versionBefore('0.10.1')) aggregation.round = true;
        if (this.forceFinalize) {
          postAggregations.push({
            type: 'finalizingFieldAccess',
            name,
            fieldName: tempName
          });
        }

      }
    } else {
      let cardinalityExpressions = this.getCardinalityExpressions(attribute);

      let druidExtractionFnBuilder: DruidExtractionFnBuilder;
      aggregation = {
        name: name,
        type: "cardinality",
        fields: cardinalityExpressions.map(cardinalityExpression => {
          if (cardinalityExpression instanceof RefExpression) return cardinalityExpression.name;

          if (!druidExtractionFnBuilder) druidExtractionFnBuilder = new DruidExtractionFnBuilder(this, true);
          return {
            type: "extraction",
            dimension: cardinalityExpression.getFreeReferences()[0],
            extractionFn: druidExtractionFnBuilder.expressionToExtractionFn(cardinalityExpression)
          };
        })
      };
      if (!this.versionBefore('0.10.1')) aggregation.round = true;

      if (cardinalityExpressions.length > 1) aggregation.byRow = true;
    }

    return this.filterAggregateIfNeeded(expression.operand, aggregation);
  }

  private customAggregateToAggregation(name: string, expression: CustomAggregateExpression, aggregations: Druid.Aggregation[], postAggregations: Druid.PostAggregation[]): void {
    let customAggregationName = expression.custom;
    let customAggregation = this.customAggregations[customAggregationName];
    if (!customAggregation) throw new Error(`could not find '${customAggregationName}'`);

    let nonce = String(Math.random()).substr(2);

    let aggregationObjs = (
      Array.isArray(customAggregation.aggregations) ?
        customAggregation.aggregations :
        (customAggregation.aggregation ? [customAggregation.aggregation] : [])
    ).map(a => {
      try {
        return JSON.parse(JSON.stringify(a).replace(/\{\{random\}\}/g, nonce));
      } catch (e) {
        throw new Error(`must have JSON custom aggregation '${customAggregationName}'`);
      }
    });

    let postAggregationObj = customAggregation.postAggregation;
    if (postAggregationObj) {
      try {
        postAggregationObj = JSON.parse(JSON.stringify(postAggregationObj).replace(/\{\{random\}\}/g, nonce));
      } catch (e) {
        throw new Error(`must have JSON custom post aggregation '${customAggregationName}'`);
      }
      // Name the post aggregation instead and let the aggregation and post aggregation sort out their internal name references
      postAggregationObj.name = name;
      postAggregations.push(postAggregationObj);
    } else {
      if (!aggregationObjs.length) throw new Error(`must have an aggregation or postAggregation in custom aggregation '${customAggregationName}'`);
      aggregationObjs[0].name = name;
    }

    aggregationObjs = aggregationObjs.map(a => this.filterAggregateIfNeeded(expression.operand, a));
    aggregations.push(...aggregationObjs);
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

    const tuning = Expression.parseTuning(expression.tuning);
    const attributeInfo = this.getAttributesInfo(attributeName);
    let histogramAggregationName = "!H_" + name;
    let aggregation: Druid.Aggregation = {
      name: histogramAggregationName,
      type: 'approxHistogram' + (attributeInfo.nativeType === 'approximateHistogram' ? 'Fold' : ''),
      fieldName: attributeName
    };

    for (let k of APPROX_HISTOGRAM_TUNINGS) {
      if (!isNaN(tuning[k] as any)) {
        (aggregation as any)[k] = Number(tuning[k]);
      }
    }

    postAggregations.push({
      name,
      type: "quantile",
      fieldName: histogramAggregationName,
      probability: expression.value
    });

    return this.filterAggregateIfNeeded(expression.operand, aggregation);
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
      if (
        (customAggregation.aggregation && customAggregation.aggregation.type === aggregationType) ||
        (Array.isArray(customAggregation.aggregations) && customAggregation.aggregations.find(a => a.type === aggregationType))
      ) {
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
    const druidExpression = new DruidExpressionBuilder(this).expressionToDruidExpression(ex);

    if (!druidExpression) {
      return this.expressionToLegacyPostAggregation(ex, aggregations, postAggregations);
    }

    return {
      type: "expression",
      expression: druidExpression
    };
  }

  private expressionToLegacyPostAggregation(ex: Expression, aggregations: Druid.Aggregation[], postAggregations: Druid.PostAggregation[]): Druid.PostAggregation {
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
        name: 'dummy', // always need to have a dummy name
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

  private versionBefore(neededVersion: string): boolean {
    const { version } = this;
    return version && External.versionLessThan(version, neededVersion);
  }
}
