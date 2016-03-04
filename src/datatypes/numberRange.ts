module Plywood {
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

  var check: Class<NumberRangeValue, NumberRangeJS>;
  export class NumberRange extends Range<number> implements Instance<NumberRangeValue, NumberRangeJS> {
    static type = 'NUMBER_RANGE';

    static isNumberRange(candidate: any): candidate is NumberRange {
      return isInstanceOf(candidate, NumberRange);
    }

    static numberBucket(num: number, size: number, offset: number): NumberRange {
      var start = Math.floor((num - offset) / size) * size + offset;
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
      var start = parameters.start;
      var end = parameters.end;
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
      var js: NumberRangeJS = {
        start: this.start,
        end: this.end
      };
      if (this.bounds !== Range.DEFAULT_BOUNDS) js.bounds = this.bounds;
      return js;
    }

    public toJSON(): NumberRangeJS {
      return this.toJS();
    }

    public equals(other: NumberRange): boolean {
      return NumberRange.isNumberRange(other) && this._equalsHelper(other);
    }

    public midpoint(): number {
      return (this.start + this.end) / 2;
    }
  }
  check = NumberRange;
}
