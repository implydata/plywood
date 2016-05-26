declare module Chronoshift {
    interface WallTime {
        rules: any;
        UTCToWallTime(date: Date, timezone: string): Date;
        WallTimeToUTC(timezone: string, years: number, months: number, days: number, hours: number, minutes: number, seconds: number, milliseconds: number): Date;
        init(a: any, b: any): void;
    }
    interface Parser {
        parse: (str: string) => any;
    }
    var WallTime: WallTime;
    var isInstanceOf: (thing: any, constructor: any) => boolean;
    export import Class = ImmutableClass.Class;
    export import Instance = ImmutableClass.Instance;
    interface Lookup<T> {
        [key: string]: T;
    }
    function isDate(d: any): boolean;
}
declare module Chronoshift {
    class Timezone implements Instance<string, string> {
        static UTC: Timezone;
        private timezone;
        static isTimezone(candidate: any): boolean;
        static fromJS(spec: string): Timezone;
        constructor(timezone: string);
        valueOf(): string;
        toJS(): string;
        toJSON(): string;
        toString(): string;
        equals(other: Timezone): boolean;
        isUTC(): boolean;
    }
}
declare module Chronoshift {
    interface AlignFn {
        (dt: Date, tz: Timezone): Date;
    }
    interface ShiftFn {
        (dt: Date, tz: Timezone, step: number): Date;
    }
    interface RoundFn {
        (dt: Date, roundTo: number, tz: Timezone): Date;
    }
    interface TimeShifter {
        canonicalLength: number;
        siblings?: number;
        floor: AlignFn;
        round?: RoundFn;
        shift: ShiftFn;
        ceil?: AlignFn;
        move?: ShiftFn;
    }
    var second: TimeShifter;
    var minute: TimeShifter;
    var hour: TimeShifter;
    var day: TimeShifter;
    var week: TimeShifter;
    var month: TimeShifter;
    var year: TimeShifter;
    var shifters: Lookup<TimeShifter>;
    var movers: Lookup<TimeShifter>;
}
declare module Chronoshift {
    interface DurationValue {
        year?: number;
        month?: number;
        week?: number;
        day?: number;
        hour?: number;
        minute?: number;
        second?: number;
        [span: string]: number;
    }
    class Duration implements Instance<DurationValue, string> {
        singleSpan: string;
        spans: DurationValue;
        static fromJS(durationStr: string): Duration;
        static fromCanonicalLength(length: number): Duration;
        static isDuration(candidate: any): boolean;
        constructor(spans: DurationValue);
        constructor(start: Date, end: Date, timezone: Timezone);
        toString(): string;
        add(duration: Duration): Duration;
        subtract(duration: Duration): Duration;
        valueOf(): DurationValue;
        toJS(): string;
        toJSON(): string;
        equals(other: Duration): boolean;
        isSimple(): boolean;
        isFloorable(): boolean;
        floor(date: Date, timezone: Timezone): Date;
        shift(date: Date, timezone: Timezone, step?: number): Date;
        materialize(start: Date, end: Date, timezone: Timezone, step?: number): Date[];
        isAligned(date: Date, timezone: Timezone): boolean;
        dividesBy(smaller: Duration): boolean;
        getCanonicalLength(): number;
        getDescription(capitalize?: boolean): string;
        getSingleSpan(): string;
        getSingleSpanValue(): number;
    }
}
declare module Chronoshift {
    function parseSQLDate(type: string, v: string): Date;
    function parseISODate(date: string, timezone?: Timezone): Date;
    interface IntervalParse {
        computedStart: Date;
        computedEnd: Date;
        start?: Date;
        end?: Date;
        duration?: Duration;
    }
    function parseInterval(str: string, timezone?: Timezone, now?: Date): IntervalParse;
}

declare var chronoshift: typeof Chronoshift;
declare module "chronoshift" {
    export = chronoshift;
}