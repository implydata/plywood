module Plywood {
  export module helper {

    function parseYear(v: string): int {
      if (v.length === 2) {
        var vn = parseInt(v, 10);
        return (vn < 70 ? 2000 : 1900) + vn;
      } else if (v.length === 4) {
        return parseInt(v, 10);
      } else {
        throw new Error('Invalid year in date');
      }
    }

    function parseMonth(v: string): int {
      var vn = parseInt(v, 10);
      if (vn <= 0 || 12 < vn) throw new Error('Invalid month in date');
      return vn - 1;
    }

    function parseDay(v: string): int {
      var vn = parseInt(v, 10);
      if (vn <= 0 || 31 < vn) throw new Error('Invalid day in date');
      return vn;
    }

    function parseHour(v: string): int {
      var vn = parseInt(v, 10);
      if (vn < 0 || 24 < vn) throw new Error('Invalid hour in date');
      return vn;
    }

    function parseMinute(v: string): int {
      var vn = parseInt(v, 10);
      if (vn < 0 || 60 < vn) throw new Error('Invalid minute in date');
      return vn;
    }

    function parseSecond(v: string): int {
      var vn = parseInt(v, 10);
      if (vn < 0 || 60 < vn) throw new Error('Invalid second in date');
      return vn;
    }

    function parseMillisecond(v: string): int {
      if (!v) return 0;
      return parseInt(v.substr(0, 3), 10);
    }

    export function parseSQLDate(type: string, v: string): Date {
      if (type === 't') throw new Error('time literals are not supported');
      var m: string[];
      var d: number;
      if (type === 'ts') {
        if (m = v.match(/^(\d{2}(?:\d{2})?)(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/)) {
          d = Date.UTC(parseYear(m[1]), parseMonth(m[2]), parseDay(m[3]), parseHour(m[4]), parseMinute(m[5]), parseSecond(m[6]));
        } else if (m = v.match(/^(\d{2}(?:\d{2})?)[~!@#$%^&*()_+=:.\-\/](\d{1,2})[~!@#$%^&*()_+=:.\-\/](\d{1,2})[T ](\d{1,2})[~!@#$%^&*()_+=:.\-\/](\d{1,2})[~!@#$%^&*()_+=:.\-\/](\d{1,2})(?:\.(\d{1,6}))?$/)) {
          d = Date.UTC(parseYear(m[1]), parseMonth(m[2]), parseDay(m[3]), parseHour(m[4]), parseMinute(m[5]), parseSecond(m[6]), parseMillisecond(m[7]));
        } else {
          throw new Error('Invalid timestamp');
        }
      } else {
        if (m = v.match(/^(\d{2}(?:\d{2})?)(\d{2})(\d{2})$/)) {
          d = Date.UTC(parseYear(m[1]), parseMonth(m[2]), parseDay(m[3]));
        } else if (m = v.match(/^(\d{2}(?:\d{2})?)[~!@#$%^&*()_+=:.\-\/](\d{1,2})[~!@#$%^&*()_+=:.\-\/](\d{1,2})$/)) {
          d = Date.UTC(parseYear(m[1]), parseMonth(m[2]), parseDay(m[3]));
        } else {
          throw new Error('Invalid date');
        }
      }
      return new Date(d);
    }

    // Taken from: https://github.com/csnover/js-iso8601/blob/lax/iso8601.js
    const numericKeys = [1, 4, 5, 6, 10, 11];
    export function parseISODate(date: string): Date {
      var struct: any[], minutesOffset = 0;

      /*
      (\d{4}|[+\-]\d{6})
      (?:
        -?
        (\d{2})
        (?:
          -?
          (\d{2})
        )?
      )?
      (?:
        [ T]?
        (\d{2})
        (?:
          :?
          (\d{2})
          (?:
            :?
            (\d{2})
            (?:
              [,\.]
              (\d{1,})
            )?
          )?
        )?
      )?
      (?:
        (Z)
      |
        ([+\-])
        (\d{2})
        (?:
          :?
          (\d{2})
        )?
      )?
      */

      //              1 YYYY                 2 MM        3 DD               4 HH        5 mm        6 ss           7 msec             8 Z 9 ±    10 tzHH    11 tzmm
      if ((struct = /^(\d{4}|[+\-]\d{6})(?:-?(\d{2})(?:-?(\d{2}))?)?(?:[ T]?(\d{2})(?::?(\d{2})(?::?(\d{2})(?:[,\.](\d{1,}))?)?)?)?(?:(Z)|([+\-])(\d{2})(?::?(\d{2}))?)?$/.exec(date))) {
        // avoid NaN timestamps caused by “undefined” values being passed to Date.UTC
        for (var i = 0, k: number; (k = numericKeys[i]); ++i) {
          struct[k] = +struct[k] || 0;
        }

        // allow undefined days and months
        struct[2] = (+struct[2] || 1) - 1;
        struct[3] = +struct[3] || 1;

        // allow arbitrary sub-second precision beyond milliseconds
        struct[7] = struct[7] ? + (struct[7] + "00").substr(0, 3) : 0;

        if (struct[8] !== 'Z' && struct[9] !== undefined) {
          minutesOffset = struct[10] * 60 + struct[11];

          if (struct[9] === '+') {
            minutesOffset = 0 - minutesOffset;
          }
        }

        var timestamp = Date.UTC(struct[1], struct[2], struct[3], struct[4], struct[5] + minutesOffset, struct[6], struct[7]);
        if (isNaN(timestamp)) throw new Error(`something went wrong with '${date}'`);
        return new Date(timestamp);
      }
      else {
        return null;
      }

    }

  }
}
