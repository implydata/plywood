module Plywood {
  const TIME_BUCKETING: Lookup<string> = {
    "PT1S": "%Y-%m-%dT%H:%i:%SZ",
    "PT1M": "%Y-%m-%dT%H:%iZ",
    "PT1H": "%Y-%m-%dT%H:00Z",
    "P1D":  "%Y-%m-%dZ",
    //"P1W":  "%Y-%m-%dZ",
    "P1M":  "%Y-%m-01Z",
    "P1Y":  "%Y-01-01Z"
  };

  export class SQLDialect {
    constructor() {
    }

    public escapeName(name: string): string {
      if (name.indexOf('`') !== -1) throw new Error("can not convert to SQL"); // ToDo: fix this
      return '`' + name + '`';
    }

    public escapeLiteral(name: string): string {
      return JSON.stringify(name);
    }

    public booleanToSQL(bool: boolean): string {
      return ('' + bool).toUpperCase();
    }

    public numberToSQL(num: number): string {
      if (num === null) return 'NULL';
      return '' + num;
    }

    public timeToSQL(date: Date): string {
      if (!date) return 'NULL';
      var str = date.toISOString()
        .replace("T", " ")
        .replace(/\.\d\d\dZ$/, "")
        .replace(" 00:00:00", "");
      return "'" + str + "'";
    }

    public inExpression(operand: string, start: string, end: string, bounds: string) {
      var startSQL: string = null;
      if (start !== 'NULL') {
        startSQL = start + (bounds[0] === '[' ? '<=' : '<') + operand;
      }
      var endSQL: string = null;
      if (end !== 'NULL') {
        endSQL = operand + (bounds[1] === ']' ? '<=' : '<') + end;
      }
      if (startSQL) {
        return endSQL ? `(${startSQL} AND ${endSQL})` : startSQL;
      } else {
        return endSQL ? endSQL : 'TRUE';
      }
    }

    public timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string {
      throw new Error('Must implement timeBucketExpression');
    }

    public offsetTimeExpression(operand: string, duration: Duration): string {
      throw new Error('Must implement offsetTimeExpression');
    }
  }

  export class MySQLDialect extends SQLDialect {
    constructor() {
      super();
    }

    public timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string {
      var bucketFormat = TIME_BUCKETING[duration.toString()];
      if (!bucketFormat) throw new Error("unsupported duration '" + duration + "'");

      var bucketTimezone = timezone.toString();
      if (bucketTimezone !== "Etc/UTC") {
        operand = `CONVERT_TZ(${operand},'+0:00','${bucketTimezone}')`;
      }

      return `DATE_FORMAT(${operand},'${bucketFormat}')`;
    }

    public offsetTimeExpression(operand: string, duration: Duration): string {
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

  // todo: Look into: WEEKDAY ( https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html )
}
