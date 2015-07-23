/// <reference path="../definitions/higher-object.d.ts" />
declare module Chronology {
    interface WallTime {
        rules: any;
        UTCToWallTime(date: Date, timezone: string): Date;
        WallTimeToUTC(timezone: string, years: number, months: number, days: number, hours: number, minutes: number, seconds: number, milliseconds: number): Date;
        init(a: any, b: any): void;
    }
    interface Parser {
        parse: (str: string) => any;
    }
    var HigherObject: HigherObject.Base;
    var WallTime: WallTime;
    var dateMath: Parser;
    interface Lookup<T> {
        [key: string]: T;
    }
    var isInstanceOf: (thing: any, constructor: any) => boolean;
    export import ImmutableClass = HigherObject.ImmutableClass;
    export import ImmutableInstance = HigherObject.ImmutableInstance;
    /**
     * An interface that represents some variable date context.
     * Every value must be a Date.
     */
    interface DateContext {
        [name: string]: Date;
    }
    function isDate(d: any): boolean;
}
declare module Chronology {
    class Timezone implements ImmutableInstance<string, string> {
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
declare module Chronology {
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
declare module Chronology {
    class DateExpression implements ImmutableInstance<any, any> {
        private _date;
        private _subExpression;
        private _action;
        private _param;
        private _duration;
        static fromJS(dateJS: string | Date): DateExpression;
        static isDateExpression(candidate: any): boolean;
        constructor(dateJS: any);
        valueOf(): any;
        /**
         * Produces the string form of the date expression
         */
        toString(): string;
        /**
         * Produces the spec of the date expression
         */
        toJS(): any;
        /**
         * Produces the JSON of the date expression
         */
        toJSON(): string;
        move(duration: Duration, tz: Timezone, dir: number): DateExpression;
        add(duration: Duration, tz: Timezone): DateExpression;
        subtract(duration: Duration, tz: Timezone): DateExpression;
        /**
         * Checks if this date expression is equivalent to another date expression
         * @param other The other date expression to check
         */
        equals(other: DateExpression): boolean;
        /**
         * Checks if this date expression is just a date literal and not some fancy math
         */
        isLiteralDate(): boolean;
        /**
         * Checks if this date expression has a duration to add/remove to its core expression
         */
        hasDuration(): boolean;
        /**
         * Evaluates this expression for the given timezone and context
         * @param timezone the timezone within which to evaluate
         * @param context the date variables to substitute
         */
        evaluate(timezone: Timezone, context?: DateContext): Date;
    }
}
declare module Chronology {
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
    class Duration implements ImmutableInstance<DurationValue, string> {
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
declare module Chronology {
    /**
     * Interface that describes the value representation of a TimeRange
     */
    interface TimeRangeValue {
        start?: DateExpression;
        duration?: Duration;
        end?: DateExpression;
    }
    /**
     * Interface that describes the spec a TimeRange all 'dates' are returned as ISO strings
     */
    interface TimeRangeJS {
        start?: string | Date;
        duration?: string;
        end?: string | Date;
    }
    class TimeRange implements ImmutableInstance<TimeRangeValue, TimeRangeJS> {
        start: DateExpression;
        end: DateExpression;
        duration: Duration;
        static fromJS(spec: TimeRangeJS): TimeRange;
        static fromString(str: string): TimeRange;
        static isTimeRange(candidate: any): boolean;
        constructor(parameters: TimeRangeValue);
        valueOf(): TimeRangeValue;
        toJS(): TimeRangeJS;
        toJSON(): TimeRangeJS;
        toString(): string;
        equals(other: TimeRange): boolean;
        /**
         * Evaluates this range for the given timezone and context
         * @param timezone the timezone within which to evaluate
         * @param context the date variables to substitute
         */
        evaluate(timezone: Timezone, context?: DateContext): Date[];
        /**
         * Get the duration for a given timezone and context
         * @param timezone the timezone within which to evaluate
         * @param context the date variables to substitute
         */
        getDuration(timezone: Timezone, context?: DateContext): Duration;
        canMove(): boolean;
        move(moveDuration: Duration, timezone: Timezone, step?: number): TimeRange;
        add(duration: Duration, timezone: Timezone): TimeRange;
        subtract(duration: Duration, timezone: Timezone): TimeRange;
    }
}

declare var chronology: typeof Chronology;
declare module "chronology" {
    export = chronology;
}