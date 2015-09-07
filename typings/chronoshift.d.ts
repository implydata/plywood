/// <reference path="../typings/immutable-class.d.ts" />
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
        /**
         * Constructs a timezone form the string representation by checking that it is defined
         */
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
    interface MoveFn {
        (dt: Date, tz: Timezone, step: number): Date;
    }
    interface TimeMover {
        canonicalLength: number;
        floor: AlignFn;
        move: MoveFn;
        ceil: AlignFn;
    }
    var second: TimeMover;
    var minute: TimeMover;
    var hour: TimeMover;
    var day: TimeMover;
    var week: TimeMover;
    var month: TimeMover;
    var year: TimeMover;
    var movers: Lookup<TimeMover>;
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
        private singleSpan;
        private spans;
        static fromJS(durationStr: string): Duration;
        static fromCanonicalLength(length: number): Duration;
        static isDuration(candidate: any): boolean;
        /**
         * Constructs an ISO duration like P1DT3H from a string
         */
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
        /**
         * Floors the date according to this duration.
         * @param date The date to floor
         * @param timezone The timezone within which to floor
         */
        floor(date: Date, timezone: Timezone): Date;
        /**
         * Moves the given date by 'step' times of the duration
         * Negative step value will move back in time.
         * @param date The date to move
         * @param timezone The timezone within which to make the move
         * @param step The number of times to step by the duration
         */
        move(date: Date, timezone: Timezone, step?: number): Date;
        getCanonicalLength(): number;
        canonicalLength(): number;
        getDescription(): string;
    }
}

declare var chronoshift: typeof Chronoshift;
declare module "chronoshift" {
    export = chronoshift;
}