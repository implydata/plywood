/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2020 Imply Data, Inc.
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

import type { Timezone } from 'chronoshift';

import { PlyType } from '../types';

const BOUNDS_REG_EXP = /^[\[(][\])]$/;

export type PlywoodRange = Range<number | Date | string>;

export interface PlywoodRangeJS {
  start: null | number | Date | string;
  end: null | number | Date | string;
  bounds?: string;
}

export abstract class Range<T> {
  static DEFAULT_BOUNDS = '[)';

  static areEquivalentBounds(bounds1: string | undefined, bounds2: string | undefined): boolean {
    return (
      bounds1 === bounds2 ||
      (!bounds1 && bounds2 === Range.DEFAULT_BOUNDS) ||
      (!bounds2 && bounds1 === Range.DEFAULT_BOUNDS)
    );
  }

  static isRange(candidate: any): candidate is PlywoodRange {
    return candidate instanceof Range;
  }

  static isRangeType(type: PlyType): boolean {
    return type && type.indexOf('_RANGE') > 0;
  }

  static unwrapRangeType(type: PlyType): PlyType | null {
    if (!type) return null;
    return Range.isRangeType(type) ? <PlyType>type.substr(0, type.length - 6) : type;
  }

  static classMap: Record<string, typeof Range> = {};

  static register(ctr: any): void {
    const rangeType = ctr.type.replace('_RANGE', '').toLowerCase();
    Range.classMap[rangeType] = ctr;
  }

  static fromJS(parameters: PlywoodRangeJS): PlywoodRange {
    let ctr: string;
    if (typeof parameters.start === 'number' || typeof parameters.end === 'number') {
      ctr = 'number';
    } else if (typeof parameters.start === 'string' || typeof parameters.end === 'string') {
      ctr = 'string';
    } else {
      ctr = 'time';
    }
    return (Range.classMap[ctr] as any).fromJS(parameters);
  }

  public start: T;
  public end: T;
  public bounds: string;

  constructor(start: T, end: T, bounds: string) {
    // Check bounds
    if (bounds) {
      if (!BOUNDS_REG_EXP.test(bounds)) {
        throw new Error(`invalid bounds ${bounds}`);
      }
    } else {
      bounds = Range.DEFAULT_BOUNDS;
    }

    if (start !== null && end !== null && this._endpointEqual(start, end)) {
      if (bounds !== '[]') {
        start = end = this._zeroEndpoint(); // empty set => make canonically [0, 0)
      }
      if (bounds === '(]' || bounds === '()') this.bounds = '[)';
    } else {
      if (start !== null && end !== null && end < start) {
        throw new Error('must have start <= end');
      }
      if (start === null && bounds[0] === '[') {
        bounds = '(' + bounds[1];
      }
      if (end === null && bounds[1] === ']') {
        bounds = bounds[0] + ')';
      }
    }

    this.start = start;
    this.end = end;
    this.bounds = bounds;
  }

  protected _zeroEndpoint(): T {
    return <any>0;
  }

  protected _endpointEqual(a: T, b: T): boolean {
    return a === b;
  }

  protected _endpointToString(a: T, _tz?: Timezone): string {
    return String(a);
  }

  protected _equalsHelper(other: Range<T>) {
    return (
      Boolean(other) &&
      this.bounds === other.bounds &&
      this._endpointEqual(this.start, other.start) &&
      this._endpointEqual(this.end, other.end)
    );
  }

  public abstract equals(other: Range<T>): boolean;

  public abstract toJS(): PlywoodRangeJS;

  public toJSON(): any {
    return this.toJS();
  }

  public toString(tz?: Timezone): string {
    const bounds = this.bounds;
    return (
      '[' +
      (bounds[0] === '(' ? '~' : '') +
      this._endpointToString(this.start, tz) +
      ',' +
      this._endpointToString(this.end, tz) +
      (bounds[1] === ')' ? '' : '!') +
      ']'
    );
  }

  public compare(other: Range<T>): number {
    const myStart = this.start;
    const otherStart = other.start;
    return myStart < otherStart ? -1 : otherStart < myStart ? 1 : 0;
  }

  public openStart(): boolean {
    return this.bounds[0] === '(';
  }

  public openEnd(): boolean {
    return this.bounds[1] === ')';
  }

  public empty(): boolean {
    return this._endpointEqual(this.start, this.end) && this.bounds === '[)';
  }

  public degenerate(): boolean {
    return this._endpointEqual(this.start, this.end) && this.bounds === '[]';
  }

  public contains(val: T | Range<T>): boolean {
    if (val instanceof Range) {
      const valStart = val.start;
      const valEnd = val.end;
      const valBound = val.bounds;
      if (valBound[0] === '[') {
        if (!this.containsValue(valStart)) return false;
      } else {
        if (!this.containsValue(valStart) && valStart.valueOf() !== this.start.valueOf())
          return false;
      }
      if (valBound[1] === ']') {
        if (!this.containsValue(valEnd)) return false;
      } else {
        if (!this.containsValue(valEnd) && valEnd.valueOf() !== this.end.valueOf()) return false;
      }
      return true;
    } else {
      return this.containsValue(val);
    }
  }

  protected validMemberType(val: any): boolean {
    return typeof val === 'number';
  }

  public containsValue(val: T): boolean {
    if (val === null) return false;
    val = (val as any).valueOf(); // Turn a Date into a number
    if (!this.validMemberType(val)) return false;

    const start = this.start;
    const end = this.end;
    const bounds = this.bounds;

    if (bounds[0] === '[') {
      if (val < start) return false;
    } else {
      if (start !== null && val <= start) return false;
    }
    if (bounds[1] === ']') {
      if (end < val) return false;
    } else {
      if (end !== null && end <= val) return false;
    }
    return true;
  }

  public intersects(other: Range<T>): boolean {
    return (
      this.containsValue(other.start) ||
      this.containsValue(other.end) ||
      other.containsValue(this.start) ||
      other.containsValue(this.end) ||
      this._equalsHelper(other)
    ); // in case of (0, 1) and (0, 1)
  }

  /**
   * Detects when ranges are touching such as [0, 1) and [1, 0)
   *
   * @param other The range to check against
   */
  public adjacent(other: Range<T>): boolean {
    return (
      (this._endpointEqual(this.end, other.start) && this.openEnd() !== other.openStart()) ||
      (this._endpointEqual(this.start, other.end) && this.openStart() !== other.openEnd())
    );
  }

  /**
   * Detects if the two ranges can be merged such as [0, 1) and [1, 0)
   *
   * @param other The range to check against
   */
  public mergeable(other: Range<T>): boolean {
    return this.intersects(other) || this.adjacent(other);
  }

  /**
   * Computes the union of the ranges, if the ranges can not be merged null is returned
   *
   * @param other The range to union with
   */
  public union(other: Range<T>): Range<T> {
    if (!this.mergeable(other)) return null;
    return this.extend(other);
  }

  /**
   * Returns the extent of the single range (itself)
   */
  public extent(): Range<T> {
    return this;
  }

  /**
   * Computes the extent of the ranges
   *
   * @param other The range to extend with
   */
  public extend(other: Range<T>): Range<T> {
    const thisStart = this.start;
    const thisEnd = this.end;
    const otherStart = other.start;
    const otherEnd = other.end;

    let start: T | null;
    let startBound: string;
    if (thisStart === null || otherStart === null) {
      start = null;
      startBound = '(';
    } else if (thisStart < otherStart) {
      start = thisStart;
      startBound = this.bounds[0];
    } else {
      start = otherStart;
      startBound = other.bounds[0];
    }

    let end: T | null;
    let endBound: string;
    if (thisEnd === null || otherEnd === null) {
      end = null;
      endBound = ')';
    } else if (thisEnd < otherEnd) {
      end = otherEnd;
      endBound = other.bounds[1];
    } else {
      end = thisEnd;
      endBound = this.bounds[1];
    }

    return new (<any>this.constructor)({ start: start, end: end, bounds: startBound + endBound });
  }

  /**
   * Computes the intersection of the ranges, if the ranges do not intersect null is returned
   * If the ranges are adjacent, like [0, 1) and [1, 2), the empty range [0, 0) is returned.
   *
   * @param other The range to union with
   */
  public intersect(other: Range<T>): Range<T> | null {
    if (!this.mergeable(other)) return null;

    const thisStart = this.start;
    const thisEnd = this.end;
    const otherStart = other.start;
    const otherEnd = other.end;

    let start: T;
    let startBound: string;
    if (thisStart === null || otherStart === null) {
      if (otherStart === null) {
        start = thisStart;
        startBound = this.bounds[0];
      } else {
        start = otherStart;
        startBound = other.bounds[0];
      }
    } else if (otherStart < thisStart) {
      start = thisStart;
      startBound = this.bounds[0];
    } else {
      start = otherStart;
      startBound = other.bounds[0];
    }

    let end: T;
    let endBound: string;
    if (thisEnd === null || otherEnd === null) {
      if (thisEnd == null) {
        end = otherEnd;
        endBound = other.bounds[1];
      } else {
        end = thisEnd;
        endBound = this.bounds[1];
      }
    } else if (otherEnd < thisEnd) {
      end = otherEnd;
      endBound = other.bounds[1];
    } else {
      end = thisEnd;
      endBound = this.bounds[1];
    }

    return new (<any>this.constructor)({ start: start, end: end, bounds: startBound + endBound });
  }

  /**
   * Computes the midpoint of the range
   */
  public abstract midpoint(): T;

  public isFinite(): boolean {
    return this.start !== null && this.end !== null;
  }
}
