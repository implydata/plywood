/*
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

import * as Q from 'q';
import { Expression, ExpressionValue, ExpressionJS, Alterations, Indexer } from "./baseExpression";
import { SQLDialect } from "../dialect/baseDialect";
import { PlywoodValue } from "../datatypes/index";
import { Action } from "../actions/baseAction";
import { ComputeFn } from "../datatypes/dataset";
import { External } from "../external/baseExternal"

export class ExternalExpression extends Expression {
  static fromJS(parameters: ExpressionJS): Expression {
    var value: ExpressionValue = {
      op: parameters.op
    };
    value.external = External.fromJS(parameters.external);
    return new ExternalExpression(value);
  }

  public external: External;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    var external = parameters.external;
    if (!external) throw new Error('must have an external');
    this.external = external;
    this._ensureOp('external');
    this.type = external.mode === 'value' ? 'NUMBER' : 'DATASET'; // ToDo: not always number
    this.simple = true;
  }

  public valueOf(): ExpressionValue {
    var value = super.valueOf();
    value.external = this.external;
    return value;
  }

  public toJS(): ExpressionJS {
    var js = super.toJS();
    js.external = this.external.toJS();
    return js;
  }

  public toString(): string {
    return `E:${this.external}`;
  }

  public getFn(): ComputeFn {
    throw new Error('should not call getFn on External');
  }

  public getJS(datumVar: string): string {
    throw new Error('should not call getJS on External');
  }

  public getSQL(dialect: SQLDialect): string {
    throw new Error('should not call getSQL on External');
  }

  public equals(other: ExternalExpression): boolean {
    return super.equals(other) &&
      this.external.equals(other.external);
  }

  public _fillRefSubstitutions(typeContext: DatasetFullType, indexer: Indexer, alterations: Alterations): FullType {
    indexer.index++;
    const { external } = this;
    if (external.mode === 'value') {
      return { type: 'NUMBER' };
    } else {
      var newTypeContext = this.external.getFullType();
      newTypeContext.parent = typeContext;
      return newTypeContext;
    }
  }

  public _computeResolvedSimulate(lastNode: boolean, simulatedQueries: any[]): PlywoodValue {
    var external = this.external;
    if (external.suppress) return external;
    return external.simulateValue(lastNode, simulatedQueries);
  }

  public _computeResolved(lastNode: boolean): Q.Promise<PlywoodValue> {
    var external = this.external;
    if (external.suppress) return Q(external);
    return external.queryValue(lastNode);
  }

  public unsuppress(): ExternalExpression {
    var value = this.valueOf();
    value.external = this.external.show();
    return new ExternalExpression(value);
  }

  public addAction(action: Action): ExternalExpression {
    var newExternal = this.external.addAction(action);
    if (!newExternal) return null;
    return new ExternalExpression({ external: newExternal });
  }

  public maxPossibleSplitValues(): number {
    return Infinity;
  }
}

Expression.register(ExternalExpression);
