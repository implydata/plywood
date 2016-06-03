module Plywood {
  const BOUNDS_REG_EXP = /^[\[(][\])]$/;

  export type PlywoodRange = Range<number | Date>;

  export class Range<T> {
    static DEFAULT_BOUNDS = '[)';

    static isRange(candidate: any): candidate is PlywoodRange {
      return isInstanceOf(candidate, Range);
    }

    // ToDo: enforce stricter typing here
    static fromJS(parameters: any): PlywoodRange {
      if (typeof parameters.start === 'number' || typeof parameters.end === 'number') {
        return NumberRange.fromJS(parameters);
      } else {
        return TimeRange.fromJS(parameters);
      }
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

    protected _endpointToString(a: T): string {
      return String(a);
    }

    protected _equalsHelper(other: Range<T>) {
      return Boolean(other) &&
        this.bounds === other.bounds &&
        this._endpointEqual(this.start, other.start) &&
        this._endpointEqual(this.end, other.end);
    }

    public toString(): string {
      var bounds = this.bounds;
      return bounds[0] + this._endpointToString(this.start) + ',' + this._endpointToString(this.end) + bounds[1];
    }

    public compare(other: Range<T>): number {
      const myStart = this.start;
      const otherStart = other.start;
      return myStart < otherStart ? -1 : (otherStart < myStart ? 1 : 0);
    }

    public openStart(): boolean {
      return this.bounds[0] === '(';
    }

    public openEnd(): boolean {
      return this.bounds[1] === ')';
    }

    public empty(): boolean {
      return this._endpointEqual(this.start, this.end) && this.bounds === '[)'
    }

    public degenerate(): boolean {
      return this._endpointEqual(this.start, this.end) && this.bounds === '[]'
    }

    public contains(val: T): boolean {
      if (val === null) return false;

      var start = this.start;
      var end = this.end;
      var bounds = this.bounds;

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
      return this.contains(other.start) || this.contains(other.end)
        || other.contains(this.start) || other.contains(this.end)
        || this._equalsHelper(other); // in case of (0, 1) and (0, 1)
    }

    /**
     * Detects when ranges are touching such as [0, 1) and [1, 0)
     *
     * @param other The range to check against
     */
    public adjacent(other: Range<T>): boolean {
      return (this._endpointEqual(this.end, other.start) && this.openEnd() !== other.openStart())
          || (this._endpointEqual(this.start, other.end) && this.openStart() !== other.openEnd());
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
      var thisStart = this.start;
      var thisEnd = this.end;
      var otherStart = other.start;
      var otherEnd = other.end;

      var start: T;
      var startBound: string;
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

      var end: T;
      var endBound: string;
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

      return new (<any>this.constructor)({start: start, end: end, bounds: startBound + endBound});
    }

    /**
     * Computes the intersection of the ranges, if the ranges do not intersect null is returned
     * If the ranges are adjacent, like [0, 1) and [1, 2), the empty range [0, 0) is returned.
     *
     * @param other The range to union with
     */
    public intersect(other: Range<T>): Range<T> {
      if (!this.mergeable(other)) return null;

      var thisStart = this.start;
      var thisEnd = this.end;
      var otherStart = other.start;
      var otherEnd = other.end;

      var start: T;
      var startBound: string;
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

      var end: T;
      var endBound: string;
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

      return new (<any>this.constructor)({start: start, end: end, bounds: startBound + endBound});
    }
  }
}
