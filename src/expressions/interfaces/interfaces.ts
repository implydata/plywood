/*
 * Copyright 2016-2016 Imply Data, Inc.
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

import { Timezone, Duration } from 'chronoshift';
import { ExpressionValue, BaseExpressionJS } from '../baseExpression';
import { PlyTypeSimple } from '../../types';

export interface OutputTypeValue extends ExpressionValue {
  outputType?: PlyTypeSimple;
}

export interface OutputTypeJS extends BaseExpressionJS {
  outputType?: PlyTypeSimple;
}

export interface RegexExpressionValue extends ExpressionValue {
  regexp: string;
}

export interface RegexExpressionJS extends BaseExpressionJS {
  regexp: string;
}

export interface TimezoneExpressionValue extends ExpressionValue {
  timezone?: Timezone;
}

export interface TimezoneExpressionJS extends BaseExpressionJS {
  timezone?: string;
}

export interface DurationedExpressionValue extends TimezoneExpressionValue {
  duration: Duration;
}

export interface DurationedExpressionJS extends TimezoneExpressionJS {
  duration: string;
}
