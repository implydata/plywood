module Plywood {
  export class SQLDialect {
    constructor() {
    }

    public inExpression(operand: string, start: string, end: string, bounds: string) {
      var startSQL: string = null;
      if (start !== null) {
        startSQL = start + (bounds[0] === '[' ? '<=' : '<') + operand;
      }
      var endSQL: string = null;
      if (end !== null) {
        endSQL = operand + (bounds[1] === ']' ? '<=' : '<') + end;
      }
      if (startSQL) {
        return endSQL ? `(${startSQL} AND ${endSQL})` : startSQL;
      } else {
        return endSQL ? endSQL : 'TRUE';
      }
    }

    public offsetTimeExpression(operand: string, duration: Duration): string {
      throw new Error('Must implement offsetTimeExpression');
    }
  }

  export class MySQLDialect extends SQLDialect {
    constructor() {
      super();
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
}
