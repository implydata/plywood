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

import { Timezone } from 'chronoshift';

import { Environment } from '../../types';
import { Expression, ExpressionValue } from '../baseExpression';

export class HasTimezone {
  public valueOf: () => ExpressionValue;

  public timezone: Timezone;

  public getTimezone(): Timezone {
    return this.timezone || Timezone.UTC;
  }

  public changeTimezone(timezone: Timezone): Expression {
    if (timezone.equals(this.timezone)) return this as any;
    const value = this.valueOf();
    value.timezone = timezone;
    return Expression.fromValue(value);
  }

  public needsEnvironment(): boolean {
    return !this.timezone;
  }

  public defineEnvironment(environment: Environment): Expression {
    if (!environment.timezone) environment = { timezone: Timezone.UTC };

    // Allow strings as well
    if (typeof environment.timezone === 'string')
      environment = { timezone: Timezone.fromJS(environment.timezone as any) };

    if (this.timezone || !environment.timezone) return this as any;
    return this.changeTimezone(environment.timezone).substitute(ex => {
      if (ex.needsEnvironment()) {
        return ex.defineEnvironment(environment);
      }
      return null;
    });
  }

  /*
  // HasTimezone mixin:
  public getTimezone: () => Timezone;
  public changeTimezone: (timezone: Timezone) => this;
  */
}
