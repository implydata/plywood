/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2018 Imply Data, Inc.
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

import { Class, Instance } from 'immutable-class';
import { Range } from './range';

export interface NumberRangeValue {
  start: number;
  end: number;
  bounds?: string;
}

export interface NumberRangeJS {
  start: any;
  end: any;
  bounds?: string;
}

function finiteOrNull(n: number): number {
  return (isNaN(n) || isFinite(n)) ? n : null;
}

let check: Class<NumberRangeValue, NumberRangeJS>;
export class NumberRange extends Range<number> implements Instance<NumberRangeValue, NumberRangeJS> {
  static type = 'NUMBER_RANGE';

  static isNumberRange(candidate: any): candidate is NumberRange {
    return candidate instanceof NumberRange;
  }

  static numberBucket(num: number, size: number, offset: number): NumberRange {
    let start = Math.floor((num - offset) / size) * size + offset;
    return new NumberRange({
      start: start,
      end: start + size,
      bounds: Range.DEFAULT_BOUNDS
    });
  }

  static fromNumber(n: number): NumberRange {
    return new NumberRange({ start: n, end: n, bounds: '[]' });
  }

  static fromJS(parameters: NumberRangeJS): NumberRange {
    if (typeof parameters !== "object") {
      throw new Error("unrecognizable numberRange");
    }
    let start = parameters.start;
    let end = parameters.end;
    return new NumberRange({
      start: start === null ? null : finiteOrNull(Number(start)),
      end: end === null ? null : finiteOrNull(Number(end)),
      bounds: parameters.bounds
    });
  }

  constructor(parameters: NumberRangeValue) {
    // So isNaN(null) === false
    if (isNaN(parameters.start)) throw new TypeError('`start` must be a number');
    if (isNaN(parameters.end)) throw new TypeError('`end` must be a number');
    super(parameters.start, parameters.end, parameters.bounds);
  }

  public valueOf(): NumberRangeValue {
    return {
      start: this.start,
      end: this.end,
      bounds: this.bounds
    };
  }

  public toJS(): NumberRangeJS {
    let js: NumberRangeJS = {
      start: this.start,
      end: this.end
    };
    if (this.bounds !== Range.DEFAULT_BOUNDS) js.bounds = this.bounds;
    return js;
  }

  public equals(other: NumberRange): boolean {
    return other instanceof NumberRange && this._equalsHelper(other);
  }

  public midpoint(): number {
    return (this.start + this.end) / 2;
  }
}
check = NumberRange;
Range.register(NumberRange);
