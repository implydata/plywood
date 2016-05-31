module Plywood {
  export class SQLDialect {
    constructor() {}

    public constantGroupBy(): string {
      return "GROUP BY ''";
    }

    public escapeName(name: string): string {
      name = name.replace(/"/g, '""');
      return '"' + name + '"';
    }

    public escapeLiteral(name: string): string {
      name = name.replace(/'/g, "''");
      return "'" + name + "'";
    }

    public booleanToSQL(bool: boolean): string {
      return ('' + bool).toUpperCase();
    }

    public numberOrTimeToSQL(x: number | Date): string {
      if (x === null) return 'NULL';
      if ((x as Date).toISOString) {
        return this.timeToSQL(x as Date);
      } else {
        return this.numberToSQL(x as number);
      }
    }

    public numberToSQL(num: number): string {
      if (num === null) return 'NULL';
      return '' + num;
    }

    public dateToSQLDateString(date: Date): string {
      return date.toISOString()
        .replace('T', ' ')
        .replace('Z', '')
        .replace(/\.000$/, '')
        .replace(/ 00:00:00$/, '');
    }

    public timeToSQL(date: Date): string {
      throw new Error('must implement');
    }

    public aggregateFilterIfNeeded(inputSQL: string, expressionSQL: string, zeroSQL: string = '0'): string {
      var whereIndex = inputSQL.indexOf(' WHERE ');
      if (whereIndex === -1) return expressionSQL;
      var filterSQL = inputSQL.substr(whereIndex + 7);
      return this.conditionalExpression(filterSQL, expressionSQL, zeroSQL);
    }

    public conditionalExpression(condition: string, thenPart: string, elsePart: string): string {
      return `IF(${condition},${thenPart},${elsePart})`
    }

    public concatExpression(a: string, b: string): string {
      throw new Error('must implement');
    }

    public containsExpression(a: string, b: string): string {
      throw new Error('must implement');
    }

    public isNotDistinctFromExpression(a: string, b: string): string {
      if (a === 'NULL') return `${b} IS NULL`;
      if (b === 'NULL') return `${a} IS NULL`;
      return `(${a} IS NOT DISTINCT FROM ${b})`;
    }

    public regexpExpression(expression: string, regexp: string): string {
      throw new Error('must implement');
    }

    public inExpression(operand: string, start: string, end: string, bounds: string) {
      if (start === end && bounds === '[]') return `${operand}=${start}`;
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

    public timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string {
      throw new Error('Dialect must implement timeFloorExpression');
    }

    public timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string {
      throw new Error('Dialect must implement timeBucketExpression');
    }

    public timePartExpression(operand: string, part: string, timezone: Timezone): string {
      throw new Error('Dialect must implement timePartExpression');
    }

    public timeShiftExpression(operand: string, duration: Duration, timezone: Timezone): string {
      throw new Error('Dialect must implement timeShiftExpression');
    }

    public extractExpression(operand: string, regexp: string): string {
      throw new Error('Dialect must implement extractExpression');
    }
  }

}
