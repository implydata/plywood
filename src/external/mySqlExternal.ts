module Plywood {
  var mySQLDialect = new MySQLDialect();

  interface SQLDescribeRow {
    Field: string;
    Type: string;
  }

  function correctResult(result: any[]): boolean {
    return Array.isArray(result) && (result.length === 0 || typeof result[0] === 'object');
  }

  function postProcessFactory(split: Expression, label: string): PostProcess {
    if (split instanceof ChainExpression) {
      var firstAction = split.actions[0];
      if (firstAction instanceof TimeBucketAction) {
        var duration = firstAction.duration;
        var timezone = firstAction.timezone;
      } else if (firstAction instanceof NumberBucketAction) {
        var size = firstAction.size;
      }
    }

    return (res: any[]): Dataset => {
      if (!correctResult(res)) {
        var err = new Error("unexpected result from MySQL");
        (<any>err).result = res; // ToDo: special error type
        throw err;
      }
      if (duration || size) {
        res.forEach((d: Datum) => {
          var v = d[label];
          if (duration) {
            v = new Date(v);
            d[label] = new TimeRange({ start: v, end: duration.move(v, timezone) })
          } else {
            d[label] = new NumberRange({ start: v, end: v + size })
          }
          return d;
        })
      }
      return new Dataset({ data: res });
    }
  }

  function postProcessIntrospect(columns: SQLDescribeRow[]): Attributes {
    return columns.map((column: SQLDescribeRow) => {
      var name = column.Field;
      var sqlType = column.Type;
      if (sqlType === "datetime") {
        return new AttributeInfo({ name, type: 'TIME' });
      } else if (sqlType.indexOf("varchar(") === 0) {
        return new AttributeInfo({ name, type: 'STRING' });
      } else if (sqlType.indexOf("int(") === 0 || sqlType.indexOf("bigint(") === 0) {
        // ToDo: make something special for integers
        return new AttributeInfo({ name, type: 'NUMBER' });
      } else if (sqlType.indexOf("decimal(") === 0) {
        return new AttributeInfo({ name, type: 'NUMBER' });
      }
    });
  }

  export class MySQLExternal extends External {
    static type = 'DATASET';

    static fromJS(datasetJS: any): MySQLExternal {
      var value: ExternalValue = External.jsToValue(datasetJS);
      value.table = datasetJS.table;
      return new MySQLExternal(value);
    }

    public table: string;

    constructor(parameters: ExternalValue) {
      super(parameters, dummyObject);
      this._ensureEngine("mysql");
      this.table = parameters.table;
    }

    public valueOf(): ExternalValue {
      var value: ExternalValue = super.valueOf();
      value.table = this.table;
      return value;
    }

    public toJS(): ExternalJS {
      var js: ExternalJS = super.toJS();
      js.table = this.table;
      return js;
    }

    public equals(other: MySQLExternal): boolean {
      return super.equals(other) &&
        this.table === other.table;
    }

    public getId(): string {
      return super.getId() + ':' + this.table;
    }

    // -----------------

    public canHandleFilter(ex: Expression): boolean {
      return true;
    }

    public canHandleTotal(): boolean {
      return true;
    }

    public canHandleSplit(ex: Expression): boolean {
      return true;
    }

    public canHandleApply(ex: Expression): boolean {
      return true;
    }

    public canHandleSort(sortAction: SortAction): boolean {
      return true;
    }

    public canHandleLimit(limitAction: LimitAction): boolean {
      return true;
    }

    public canHandleHavingFilter(ex: Expression): boolean {
      return true;
    }

    // -----------------

    public getQueryAndPostProcess(): QueryAndPostProcess<string> {
      var table = "`" + this.table + "`";
      var query = ['SELECT'];
      switch (this.mode) {
        case 'raw':
          query.push('`' + Object.keys(this.attributes).join('`, `') + '`');
          query.push('FROM ' + table);
          if (!(this.filter.equals(Expression.TRUE))) {
            query.push('WHERE ' + this.filter.getSQL(mySQLDialect));
          }
          break;

        case 'total':
          query.push(this.applies.map(apply => apply.getSQL('', mySQLDialect)).join(',\n'));
          query.push('FROM ' + table);
          if (!(this.filter.equals(Expression.TRUE))) {
            query.push('WHERE ' + this.filter.getSQL(mySQLDialect));
          }
          query.push("GROUP BY ''");
          break;

        case 'split':
          query.push(
            [`${this.split.getSQL(mySQLDialect)} AS '${this.key}'`]
              .concat(this.applies.map(apply => apply.getSQL('', mySQLDialect))).join(',\n')
          );
          query.push('FROM ' + table);
          if (!(this.filter.equals(Expression.TRUE))) {
            query.push('WHERE ' + this.filter.getSQL(mySQLDialect));
          }
          query.push('GROUP BY ' + this.split.getSQL(mySQLDialect));
          if (!(this.havingFilter.equals(Expression.TRUE))) {
            query.push('HAVING ' + this.havingFilter.getSQL(mySQLDialect));
          }
          if (this.sort) {
            query.push(this.sort.getSQL('', mySQLDialect));
          }
          if (this.limit) {
            query.push(this.limit.getSQL('', mySQLDialect));
          }
          break;

        default:
          throw new Error("can not get query for: " + this.mode);
      }

      return {
        query: query.join('\n'),
        postProcess: postProcessFactory(this.split, this.key)
      };
    }

    public getIntrospectQueryAndPostProcess(): IntrospectQueryAndPostProcess<string> {
      return {
        query: "DESCRIBE `" + this.table + "`",
        postProcess: postProcessIntrospect
      };
    }
  }

  External.register(MySQLExternal, 'mysql');
}
