module Plywood {
  export class MySQLDialect extends SQLDialect {
    static TIME_BUCKETING: Lookup<string> = {
      "PT1S": "%Y-%m-%d %H:%i:%SZ",
      "PT1M": "%Y-%m-%d %H:%i:00Z",
      "PT1H": "%Y-%m-%d %H:00:00Z",
      "P1D":  "%Y-%m-%d 00:00:00Z",
      "P1M":  "%Y-%m-01 00:00:00Z",
      "P1Y":  "%Y-01-01 00:00:00Z"
    };

    static TIME_PART_TO_FUNCTION: Lookup<string> = {
      SECOND_OF_MINUTE: 'SECOND($$)',
      SECOND_OF_HOUR: '(MINUTE($$)*60+SECOND($$))',
      SECOND_OF_DAY: '((HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
      SECOND_OF_WEEK: '(((WEEKDAY($$)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
      SECOND_OF_MONTH: '((((DAYOFMONTH($$)-1)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
      SECOND_OF_YEAR: '((((DAYOFYEAR($$)-1)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',

      MINUTE_OF_HOUR: 'MINUTE($$)',
      MINUTE_OF_DAY: 'HOUR($$)*60+MINUTE($$)',
      MINUTE_OF_WEEK: '(WEEKDAY($$)*24)+HOUR($$)*60+MINUTE($$)',
      MINUTE_OF_MONTH: '((DAYOFMONTH($$)-1)*24)+HOUR($$)*60+MINUTE($$)',
      MINUTE_OF_YEAR: '((DAYOFYEAR($$)-1)*24)+HOUR($$)*60+MINUTE($$)',

      HOUR_OF_DAY: 'HOUR($$)',
      HOUR_OF_WEEK: '(WEEKDAY($$)*24+HOUR($$))',
      HOUR_OF_MONTH: '((DAYOFMONTH($$)-1)*24+HOUR($$))',
      HOUR_OF_YEAR: '((DAYOFYEAR($$)-1)*24+HOUR($$))',

      DAY_OF_WEEK: '(WEEKDAY($$)+1)',
      DAY_OF_MONTH: 'DAYOFMONTH($$)',
      DAY_OF_YEAR: 'DAYOFYEAR($$)',

      WEEK_OF_MONTH: null,
      WEEK_OF_YEAR: 'WEEK($$)', // ToDo: look into mode (https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_week)

      MONTH_OF_YEAR: 'MONTH($$)',
      YEAR: 'YEAR($$)'
    };

    constructor() {
      super();
    }

    public escapeName(name: string): string {
      name = name.replace(/`/g, '``');
      return '`' + name + '`';
    }

    public escapeLiteral(name: string): string {
      return JSON.stringify(name);
    }

    public timeToSQL(date: Date): string {
      if (!date) return 'NULL';
      return `TIMESTAMP('${this.dateToSQLDateString(date)}')`;
    }

    public concatExpression(a: string, b: string): string {
      return `CONCAT(${a},${b})`;
    }

    public containsExpression(a: string, b: string): string {
      return `LOCATE(${a},${b})>0`;
    }

    public isNotDistinctFromExpression(a: string, b: string): string {
      return `(${a}<=>${b})`;
    }

    public regexpExpression(expression: string, regexp: string): string {
      return `(${expression} REGEXP '${regexp}')`; // ToDo: escape this.regexp
    }

    public utcToWalltime(operand: string, timezone: Timezone): string {
      if (timezone.isUTC()) return operand;
      return `CONVERT_TZ(${operand},'+0:00','${timezone}')`;
    }

    public walltimeToUTC(operand: string, timezone: Timezone): string {
      if (timezone.isUTC()) return operand;
      return `CONVERT_TZ(${operand},'${timezone}','+0:00')`;
    }

    public timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string {
      var bucketFormat = MySQLDialect.TIME_BUCKETING[duration.toString()];
      if (!bucketFormat) throw new Error(`unsupported duration '${duration}'`);
      return this.walltimeToUTC(`DATE_FORMAT(${this.utcToWalltime(operand, timezone)},'${bucketFormat}')`, timezone);
    }

    public timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string {
      return this.timeFloorExpression(operand, duration, timezone);
    }

    public timePartExpression(operand: string, part: string, timezone: Timezone): string {
      var timePartFunction = MySQLDialect.TIME_PART_TO_FUNCTION[part];
      if (!timePartFunction) throw new Error(`unsupported part ${part} in MySQL dialect`);
      return timePartFunction.replace(/\$\$/g, this.utcToWalltime(operand, timezone));
    }

    public timeShiftExpression(operand: string, duration: Duration, timezone: Timezone): string {
      // https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_date-add
      var sqlFn = "DATE_ADD("; //warpDirection > 0 ? "DATE_ADD(" : "DATE_SUB(";
      var spans = duration.valueOf();
      if (spans.week) {
        return sqlFn + operand + ", INTERVAL " + String(spans.week) + ' WEEK)';
      }
      if (spans.year || spans.month) {
        var expr = String(spans.year || 0) + "-" + String(spans.month || 0);
        operand = sqlFn + operand + ", INTERVAL '" + expr + "' YEAR_MONTH)";
      }
      if (spans.day || spans.hour || spans.minute || spans.second) {
        var expr = String(spans.day || 0) + " " + [spans.hour || 0, spans.minute || 0, spans.second || 0].join(':');
        operand = sqlFn + operand + ", INTERVAL '" + expr + "' DAY_SECOND)";
      }
      return operand
    }

    public extractExpression(operand: string, regexp: string): string {
      throw new Error('MySQL must implement extractExpression (https://github.com/mysqludf/lib_mysqludf_preg)');
    }
  }

}
