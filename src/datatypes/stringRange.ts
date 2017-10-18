/*
 * Copyright 2015-2017 Imply Data, Inc.
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

export interface StringRangeValue {
  start: string;
  end: string;
  bounds?: string;
}

export interface StringRangeJS {
  start: string;
  end: string;
  bounds?: string;
}

let check: Class<StringRangeValue, StringRangeJS>;
export class StringRange extends Range<string> implements Instance<StringRangeValue, StringRangeJS> {
  static type = 'STRING_RANGE';

  static isStringRange(candidate: any): candidate is StringRange {
    return candidate instanceof StringRange;
  }

  static fromString(s: string): StringRange {
    return new StringRange({ start: s, end: s, bounds: '[]' });
  }

  static fromJS(parameters: StringRangeJS): StringRange {
    if (typeof parameters !== "object") {
      throw new Error("unrecognizable StringRange");
    }
    let start = parameters.start;
    let end = parameters.end;
    let bounds = parameters.bounds;

    return new StringRange({
      start, end, bounds
    });
  }

  constructor(parameters: StringRangeValue) {
    let { start, end } = parameters;
    if (typeof start !== 'string' && start !== null) throw new TypeError('`start` must be a string');
    if (typeof end !== 'string' && end !== null) throw new TypeError('`end` must be a string');
    super(start, end, parameters.bounds);
  }

  public valueOf(): StringRangeValue {
    return {
      start: this.start,
      end: this.end,
      bounds: this.bounds
    };
  }

  public toJS(): StringRangeJS {
    let js: StringRangeJS = {
      start: this.start,
      end: this.end
    };
    if (this.bounds !== Range.DEFAULT_BOUNDS) js.bounds = this.bounds;
    return js;
  }

  public equals(other: StringRange): boolean {
    return other instanceof StringRange && this._equalsHelper(other);
  }

  public midpoint(): string {
    throw new Error("midpoint not supported in string range");
  }

  protected _zeroEndpoint() {
    return "";
  }

  protected validMemberType(val: any): boolean {
    return typeof val === 'string';
  }
}
check = StringRange;
Range.register(StringRange);
