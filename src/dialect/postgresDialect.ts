module Plywood {
  export class PostgresDialect extends SQLDialect {
    static TIME_BUCKETING: Lookup<string> = {
      "PT1S": "second",
      "PT1M": "minute",
      "PT1H": "hour",
      "P1D":  "day",
      "P1W":  "week",
      "P1M":  "month",
      "P3M":  "quarter",
      "P1Y":  "year"
    };

    static TIME_PART_TO_FUNCTION: Lookup<string> = {
      SECOND_OF_MINUTE: "DATE_PART('second', $$)",
      SECOND_OF_HOUR: '(MINUTE($$)*60+SECOND($$))',
      SECOND_OF_DAY: '((HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
      SECOND_OF_WEEK: '(((WEEKDAY($$)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
      SECOND_OF_MONTH: '((((DAYOFMONTH($$)-1)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',
      SECOND_OF_YEAR: '((((DAYOFYEAR($$)-1)*24)+HOUR($$)*60+MINUTE($$))*60+SECOND($$))',

      MINUTE_OF_HOUR: "DATE_PART('minute', $$)",
      MINUTE_OF_DAY: 'HOUR($$)*60+MINUTE($$)',
      MINUTE_OF_WEEK: '(WEEKDAY($$)*24)+HOUR($$)*60+MINUTE($$)',
      MINUTE_OF_MONTH: '((DAYOFMONTH($$)-1)*24)+HOUR($$)*60+MINUTE($$)',
      MINUTE_OF_YEAR: '((DAYOFYEAR($$)-1)*24)+HOUR($$)*60+MINUTE($$)',

      HOUR_OF_DAY: "DATE_PART('hour', $$)",
      HOUR_OF_WEEK: '(WEEKDAY($$)*24+HOUR($$))',
      HOUR_OF_MONTH: '((DAYOFMONTH($$)-1)*24+HOUR($$))',
      HOUR_OF_YEAR: '((DAYOFYEAR($$)-1)*24+HOUR($$))',

      DAY_OF_WEEK: '(WEEKDAY($$)+1)',
      DAY_OF_MONTH: 'DAYOFMONTH($$)',
      DAY_OF_YEAR: 'DAYOFYEAR($$)',

      WEEK_OF_MONTH: null,
      WEEK_OF_YEAR: 'WEEK($$)', // ToDo: look into mode (https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_week)

      MONTH_OF_YEAR: "DATE_PART('month', $$)",
      YEAR: "DATE_PART('year', $$)",
    };

    constructor() {
      super();
    }

    public constantGroupBy(): string {
      return "GROUP BY ''=''";
    }

    public concatExpression(a: string, b: string): string {
      return `(${a}||${b})`;
    }

    public containsExpression(a: string, b: string): string {
      return `POSITION(${a} IN ${b})>0`;
    }

    public utcToWalltime(operand: string, timezone: Timezone): string {
      if (timezone.isUTC()) return operand;
      return `(${operand} AT TIME ZONE 'UTC' AT TIME ZONE '${timezone}')`;
    }

    public walltimeToUTC(operand: string, timezone: Timezone): string {
      if (timezone.isUTC()) return operand;
      return `(${operand} AT TIME ZONE '${timezone}' AT TIME ZONE 'UTC')`;
    }

    public timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string {
      var bucketFormat = PostgresDialect.TIME_BUCKETING[duration.toString()];
      if (!bucketFormat) throw new Error(`unsupported duration '${duration}'`);
      return this.walltimeToUTC(`DATE_TRUNC('${bucketFormat}',${this.utcToWalltime(operand, timezone)})`, timezone);
    }

    public timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string {
      return this.timeFloorExpression(operand, duration, timezone);
    }

    public timePartExpression(operand: string, part: string, timezone: Timezone): string {
      var timePartFunction = PostgresDialect.TIME_PART_TO_FUNCTION[part];
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
  }

}
