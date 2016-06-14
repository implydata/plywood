module Plywood {
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

  var check: Class<StringRangeValue, StringRangeJS>;
  export class StringRange extends Range<string> implements Instance<StringRangeValue, StringRangeJS> {
    static type = 'STRING_RANGE';

    static isStringRange(candidate: any): candidate is StringRange {
      return isInstanceOf(candidate, StringRange);
    }

    static fromString(s: string): StringRange {
      return new StringRange({ start: s, end: s, bounds: '[]' });
    }

    static fromJS(parameters: StringRangeJS): StringRange {
      if (typeof parameters !== "object") {
        throw new Error("unrecognizable StringRange");
      }
      var start = parameters.start;
      var end = parameters.end;
      var bounds = parameters.bounds;

      return new StringRange({
        start, end, bounds
      });
    }

    constructor(parameters: StringRangeValue) {
      var { start, end } = parameters;
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
      var js: StringRangeJS = {
        start: this.start,
        end: this.end
      };
      if (this.bounds !== Range.DEFAULT_BOUNDS) js.bounds = this.bounds;
      return js;
    }

    public toJSON(): StringRangeJS {
      return this.toJS();
    }

    public equals(other: StringRange): boolean {
      return StringRange.isStringRange(other) && this._equalsHelper(other);
    }

    public midpoint(): string {
      return String.fromCharCode(Math.floor((this.start.charCodeAt(0) + this.end.charCodeAt(0)) / 2));
    }

    protected _zeroEndpoint() {
      return "";
    }
  }
  check = StringRange;
}
