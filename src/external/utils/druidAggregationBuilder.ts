/*
 * Copyright 2016-2020 Imply Data, Inc.
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

import { AttributeInfo } from '../../datatypes';
import {
  $,
  AddExpression,
  ApplyExpression,
  CastExpression,
  ConcatExpression,
  CountDistinctExpression,
  CountExpression,
  CustomAggregateExpression,
  DivideExpression,
  Expression,
  FilterExpression,
  LiteralExpression,
  MaxExpression,
  MinExpression,
  MultiplyExpression,
  QuantileExpression,
  RefExpression,
  SubtractExpression,
  SumExpression,
} from '../../expressions';
import { External } from '../baseExternal';

import { DruidExpressionBuilder } from './druidExpressionBuilder';
import { DruidExtractionFnBuilder } from './druidExtractionFnBuilder';
import { DruidFilterBuilder } from './druidFilterBuilder';
import { CustomDruidAggregations, CustomDruidTransforms } from './druidTypes';

export interface AggregationsAndPostAggregations {
  aggregations: Druid.Aggregation[];
  postAggregations: Druid.PostAggregation[];
}

export interface DruidAggregationBuilderOptions {
  rawAttributes: AttributeInfo[];
  timeAttribute: string;
  derivedAttributes: Record<string, Expression>;
  customAggregations: CustomDruidAggregations;
  customTransforms: CustomDruidTransforms;
  rollup: boolean;
  exactResultsOnly: boolean;
  allowEternity: boolean;
}

export class DruidAggregationBuilder {
  static AGGREGATE_TO_FUNCTION: Record<string, Function> = {
    sum: (a: string, b: string) => `${a}+${b}`,
    min: (a: string, b: string) => `Math.min(${a},${b})`,
    max: (a: string, b: string) => `Math.max(${a},${b})`,
  };

  static AGGREGATE_TO_ZERO: Record<string, string> = {
    sum: '0',
    min: 'Infinity',
    max: '-Infinity',
  };

  static APPROX_HISTOGRAM_TUNINGS: string[] = [
    'resolution',
    'numBuckets',
    'lowerLimit',
    'upperLimit',
  ];

  static QUANTILES_DOUBLES_TUNINGS: string[] = ['k'];

  static addOptionsToAggregation(aggregation: Druid.Aggregation, expression: Expression) {
    const options = expression.options;
    if (options && options.csum) {
      (aggregation as any)._csum = true;
    }
  }

  public rawAttributes: AttributeInfo[];
  public timeAttribute: string;
  public derivedAttributes: Record<string, Expression>;
  public customAggregations: CustomDruidAggregations;
  public customTransforms: CustomDruidTransforms;
  public rollup: boolean;
  public exactResultsOnly: boolean;
  public allowEternity: boolean;

  constructor(options: DruidAggregationBuilderOptions) {
    this.rawAttributes = options.rawAttributes;
    this.timeAttribute = options.timeAttribute;
    this.derivedAttributes = options.derivedAttributes;
    this.customAggregations = options.customAggregations;
    this.customTransforms = options.customTransforms;
    this.rollup = options.rollup;
    this.exactResultsOnly = options.exactResultsOnly;
    this.allowEternity = options.allowEternity;
  }

  public makeAggregationsAndPostAggregations(
    applies: ApplyExpression[],
  ): AggregationsAndPostAggregations {
    const { aggregateApplies, postAggregateApplies } = External.segregationAggregateApplies(
      applies.map(apply => {
        let expression = apply.expression;
        expression = this.switchToRollupCount(
          this.inlineDerivedAttributesInAggregate(expression).decomposeAverage(),
        ).distribute();
        return apply.changeExpression(expression);
      }),
    );

    const aggregations: Druid.Aggregation[] = [];
    const postAggregations: Druid.PostAggregation[] = [];

    for (const aggregateApply of aggregateApplies) {
      this.applyToAggregation(aggregateApply, aggregations, postAggregations);
    }

    for (const postAggregateApply of postAggregateApplies) {
      this.applyToPostAggregation(postAggregateApply, aggregations, postAggregations);
    }

    return {
      aggregations,
      postAggregations,
    };
  }

  private applyToAggregation(
    action: ApplyExpression,
    aggregations: Druid.Aggregation[],
    postAggregations: Druid.PostAggregation[],
  ): void {
    const { name, expression } = action;
    this.expressionToAggregation(name, expression, aggregations, postAggregations);
  }

  private applyToPostAggregation(
    apply: ApplyExpression,
    aggregations: Druid.Aggregation[],
    postAggregations: Druid.PostAggregation[],
  ): void {
    const postAgg = this.expressionToPostAggregation(
      apply.expression,
      aggregations,
      postAggregations,
    );
    postAgg.name = apply.name;
    postAggregations.push(postAgg);
  }

  // -----------------------------------

  private filterAggregateIfNeeded(
    datasetExpression: Expression,
    aggregator: Druid.Aggregation,
  ): Druid.Aggregation {
    if (datasetExpression instanceof FilterExpression) {
      return {
        type: 'filtered',
        name: aggregator.name,
        filter: new DruidFilterBuilder(this).timelessFilterToFilter(datasetExpression.expression),
        aggregator,
      };
    } else if (datasetExpression instanceof RefExpression) {
      return aggregator;
    } else {
      throw new Error(`could not construct aggregate on ${datasetExpression}`);
    }
  }

  private expressionToAggregation(
    name: string,
    expression: Expression,
    aggregations: Druid.Aggregation[],
    postAggregations: Druid.PostAggregation[],
  ): void {
    const initAggregationsLength = aggregations.length;

    if (expression instanceof CountExpression) {
      aggregations.push(this.countToAggregation(name, expression));
    } else if (
      expression instanceof SumExpression ||
      expression instanceof MinExpression ||
      expression instanceof MaxExpression
    ) {
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

    // Add options to all the newly added aggregations
    const finalAggregationsLength = aggregations.length;
    for (let i = initAggregationsLength; i < finalAggregationsLength; i++) {
      DruidAggregationBuilder.addOptionsToAggregation(aggregations[i], expression);
    }
  }

  private countToAggregation(name: string, expression: CountExpression): Druid.Aggregation {
    return this.filterAggregateIfNeeded(expression.operand, {
      name,
      type: 'count',
    });
  }

  private sumMinMaxToAggregation(
    name: string,
    expression: SumExpression | MinExpression | MaxExpression,
  ): Druid.Aggregation {
    const op = expression.op;
    const opCap = op[0].toUpperCase() + op.substr(1);

    let aggregation: Druid.Aggregation;

    const aggregateExpression = expression.expression;
    if (aggregateExpression instanceof RefExpression) {
      const refName = aggregateExpression.name;
      const attributeInfo = this.getAttributesInfo(refName);
      if (attributeInfo.nativeType === 'STRING') {
        aggregation = {
          name,
          type: 'double' + opCap,
          expression: new DruidExpressionBuilder(this).expressionToDruidExpression(
            aggregateExpression.cast('NUMBER'),
          ),
        };
      } else {
        aggregation = {
          name,
          type: (attributeInfo.nativeType === 'LONG' ? 'long' : 'double') + opCap,
          fieldName: refName,
        };
      }
    } else {
      aggregation = {
        name,
        type: 'double' + opCap,
        expression: new DruidExpressionBuilder(this).expressionToDruidExpression(
          aggregateExpression,
        ),
      };
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

  private countDistinctToAggregation(
    name: string,
    expression: CountDistinctExpression,
    postAggregations: Druid.PostAggregation[],
  ): Druid.Aggregation {
    if (this.exactResultsOnly) {
      throw new Error('approximate query not allowed');
    }

    let aggregation: Druid.Aggregation;
    const attribute = expression.expression;
    const forceFinalize: boolean = expression.getOptions().forceFinalize;
    if (attribute instanceof RefExpression) {
      const attributeName = attribute.name;

      const attributeInfo = this.getAttributesInfo(attributeName);
      let tempName: string;
      switch (attributeInfo.nativeType) {
        case 'hyperUnique':
          tempName = '!Hyper_' + name;
          aggregation = {
            name: forceFinalize ? tempName : name,
            type: 'hyperUnique',
            fieldName: attributeName,
            round: true,
          };
          if (forceFinalize) {
            postAggregations.push({
              type: 'finalizingFieldAccess',
              name,
              fieldName: tempName,
            });
          }
          break;

        case 'thetaSketch':
          tempName = '!Theta_' + name;
          postAggregations.push({
            type: 'thetaSketchEstimate',
            name: name,
            field: { type: 'fieldAccess', fieldName: tempName },
          });

          aggregation = {
            name: tempName,
            type: 'thetaSketch',
            fieldName: attributeName,
          };
          break;

        case 'HLLSketch':
          tempName = '!HLLSketch_' + name;
          aggregation = {
            name: forceFinalize ? tempName : name,
            type: 'HLLSketchMerge',
            fieldName: attributeName,
            round: true,
          };
          if (forceFinalize) {
            postAggregations.push({
              type: 'finalizingFieldAccess',
              name,
              fieldName: tempName,
            });
          }
          break;

        default:
          tempName = '!Card_' + name;
          aggregation = {
            name: forceFinalize ? tempName : name,
            type: 'cardinality',
            fields: [attributeName],
            round: true,
          };
          if (forceFinalize) {
            postAggregations.push({
              type: 'finalizingFieldAccess',
              name,
              fieldName: tempName,
            });
          }
          break;
      }
    } else {
      const cardinalityExpressions = this.getCardinalityExpressions(attribute);

      let druidExtractionFnBuilder: DruidExtractionFnBuilder;
      aggregation = {
        name: name,
        type: 'cardinality',
        fields: cardinalityExpressions.map(cardinalityExpression => {
          if (cardinalityExpression instanceof RefExpression) return cardinalityExpression.name;

          if (!druidExtractionFnBuilder)
            druidExtractionFnBuilder = new DruidExtractionFnBuilder(this);
          return {
            type: 'extraction',
            dimension: cardinalityExpression.getFreeReferences()[0],
            extractionFn: druidExtractionFnBuilder.expressionToExtractionFn(cardinalityExpression),
          };
        }),
        round: true,
      };

      if (cardinalityExpressions.length > 1) aggregation.byRow = true;
    }

    return this.filterAggregateIfNeeded(expression.operand, aggregation);
  }

  private customAggregateToAggregation(
    name: string,
    expression: CustomAggregateExpression,
    aggregations: Druid.Aggregation[],
    postAggregations: Druid.PostAggregation[],
  ): void {
    const customAggregationName = expression.custom;
    const customAggregation = this.customAggregations[customAggregationName];
    if (!customAggregation) throw new Error(`could not find '${customAggregationName}'`);

    const nonce = String(Math.random()).substr(2);

    let aggregationObjs = (
      Array.isArray(customAggregation.aggregations)
        ? customAggregation.aggregations
        : customAggregation.aggregation
        ? [customAggregation.aggregation]
        : []
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
        postAggregationObj = JSON.parse(
          JSON.stringify(postAggregationObj).replace(/\{\{random\}\}/g, nonce),
        );
      } catch (e) {
        throw new Error(`must have JSON custom post aggregation '${customAggregationName}'`);
      }
      // Name the post aggregation instead and let the aggregation and post aggregation sort out their internal name references
      postAggregationObj.name = name;
      postAggregations.push(postAggregationObj);
    } else {
      if (!aggregationObjs.length)
        throw new Error(
          `must have an aggregation or postAggregation in custom aggregation '${customAggregationName}'`,
        );
      aggregationObjs[0].name = name;
    }

    aggregationObjs = aggregationObjs.map(a => this.filterAggregateIfNeeded(expression.operand, a));
    aggregations.push(...aggregationObjs);
  }

  private quantileToAggregation(
    name: string,
    expression: QuantileExpression,
    postAggregations: Druid.PostAggregation[],
  ): Druid.Aggregation {
    if (this.exactResultsOnly) {
      throw new Error('approximate query not allowed');
    }

    const attribute = expression.expression;
    let attributeName: string;
    if (attribute instanceof RefExpression) {
      attributeName = attribute.name;
    } else {
      throw new Error(`can not compute quantile on derived attribute: ${attribute}`);
    }

    const tuning = Expression.parseTuning(expression.tuning);
    const addTuningsToAggregation = (aggregation: Druid.Aggregation, tuningKeys: string[]) => {
      for (const k of tuningKeys) {
        if (!isNaN(tuning[k] as any)) {
          (aggregation as any)[k] = Number(tuning[k]);
        }
      }
    };

    const attributeInfo = this.getAttributesInfo(attributeName);
    let aggregation: Druid.Aggregation;
    let tempName: string;
    switch (attributeInfo.nativeType) {
      case 'approximateHistogram':
        tempName = '!H_' + name;
        aggregation = {
          name: tempName,
          type: 'approxHistogramFold',
          fieldName: attributeName,
        };
        addTuningsToAggregation(aggregation, DruidAggregationBuilder.APPROX_HISTOGRAM_TUNINGS);

        postAggregations.push({
          name,
          type: 'quantile',
          fieldName: tempName,
          probability: expression.value,
        });

        break;

      case 'quantilesDoublesSketch':
        tempName = '!QD_' + name;
        aggregation = {
          name: tempName,
          type: 'quantilesDoublesSketch',
          fieldName: attributeName,
        };
        addTuningsToAggregation(aggregation, DruidAggregationBuilder.QUANTILES_DOUBLES_TUNINGS);

        postAggregations.push({
          name,
          type: 'quantilesDoublesSketchToQuantile',
          field: {
            type: 'fieldAccess',
            fieldName: tempName,
          },
          fraction: expression.value,
        });
        break;

      default:
        if (Number(tuning['v']) === 2) {
          tempName = '!QD_' + name;
          aggregation = {
            name: tempName,
            type: 'quantilesDoublesSketch',
            fieldName: attributeName,
          };
          addTuningsToAggregation(aggregation, DruidAggregationBuilder.QUANTILES_DOUBLES_TUNINGS);

          postAggregations.push({
            name,
            type: 'quantilesDoublesSketchToQuantile',
            field: {
              type: 'fieldAccess',
              fieldName: tempName,
            },
            fraction: expression.value,
          });
        } else {
          tempName = '!H_' + name;
          aggregation = {
            name: tempName,
            type: 'approxHistogram',
            fieldName: attributeName,
          };
          addTuningsToAggregation(aggregation, DruidAggregationBuilder.APPROX_HISTOGRAM_TUNINGS);

          postAggregations.push({
            name,
            type: 'quantile',
            fieldName: tempName,
            probability: expression.value,
          });
        }
        break;
    }

    return this.filterAggregateIfNeeded(expression.operand, aggregation);
  }

  // ------------------------------

  private getAccessTypeForAggregation(aggregationType: string): string {
    if (aggregationType === 'hyperUnique' || aggregationType === 'cardinality')
      return 'hyperUniqueCardinality';

    const customAggregations = this.customAggregations;
    for (const customName in customAggregations) {
      if (!hasOwnProp(customAggregations, customName)) continue;
      const customAggregation = customAggregations[customName];
      if (
        (customAggregation.aggregation && customAggregation.aggregation.type === aggregationType) ||
        (Array.isArray(customAggregation.aggregations) &&
          customAggregation.aggregations.find(a => a.type === aggregationType))
      ) {
        return customAggregation.accessType || 'fieldAccess';
      }
    }
    return 'fieldAccess';
  }

  private getAccessType(aggregations: Druid.Aggregation[], aggregationName: string): string {
    for (const aggregation of aggregations) {
      if (aggregation.name === aggregationName) {
        let aggregationType = aggregation.type;
        if (aggregationType === 'filtered') aggregationType = aggregation.aggregator.type;
        return this.getAccessTypeForAggregation(aggregationType);
      }
    }
    return 'fieldAccess'; // If not found it must be a post-agg
  }

  private expressionToPostAggregation(
    ex: Expression,
    aggregations: Druid.Aggregation[],
    postAggregations: Druid.PostAggregation[],
  ): Druid.PostAggregation {
    const druidExpression = new DruidExpressionBuilder(this).expressionToDruidExpression(ex);

    if (!druidExpression) {
      return this.expressionToLegacyPostAggregation(ex, aggregations, postAggregations);
    }

    return {
      type: 'expression',
      expression: druidExpression,
    };
  }

  private expressionToLegacyPostAggregation(
    ex: Expression,
    aggregations: Druid.Aggregation[],
    postAggregations: Druid.PostAggregation[],
  ): Druid.PostAggregation {
    if (ex instanceof RefExpression) {
      const refName = ex.name;
      return {
        type: this.getAccessType(aggregations, refName),
        fieldName: refName,
      };
    } else if (ex instanceof LiteralExpression) {
      if (ex.type !== 'NUMBER') throw new Error('must be a NUMBER type');
      return {
        type: 'constant',
        value: ex.value,
      };
    } else if (ex instanceof AddExpression) {
      return {
        type: 'arithmetic',
        fn: '+',
        fields: ex
          .getExpressionList()
          .map(e => this.expressionToPostAggregation(e, aggregations, postAggregations)),
      };
    } else if (ex instanceof SubtractExpression) {
      return {
        type: 'arithmetic',
        fn: '-',
        fields: ex
          .getExpressionList()
          .map(e => this.expressionToPostAggregation(e, aggregations, postAggregations)),
      };
    } else if (ex instanceof MultiplyExpression) {
      return {
        type: 'arithmetic',
        fn: '*',
        fields: ex
          .getExpressionList()
          .map(e => this.expressionToPostAggregation(e, aggregations, postAggregations)),
      };
    } else if (ex instanceof DivideExpression) {
      return {
        type: 'arithmetic',
        fn: '/',
        fields: ex
          .getExpressionList()
          .map(e => this.expressionToPostAggregation(e, aggregations, postAggregations)),
      };
    } else {
      throw new Error(`can not convert expression to post agg: ${ex}`);
    }
  }

  private switchToRollupCount(expression: Expression): Expression {
    if (!this.rollup) return expression;

    let countRef: RefExpression = null;
    return expression.substitute(ex => {
      if (ex instanceof CountExpression) {
        if (!countRef) countRef = $(this.getRollupCountName(), 'NUMBER');
        return ex.operand.sum(countRef);
      }
      return null;
    });
  }

  private getRollupCountName(): string {
    const { rawAttributes } = this;
    for (const attribute of rawAttributes) {
      const maker = attribute.maker;
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
    return expression.substitute(ex => {
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
