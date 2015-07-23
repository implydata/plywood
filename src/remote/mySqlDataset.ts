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
    if (split instanceof TimeBucketExpression) {
      var duration = split.duration;
      var timezone = split.timezone;
    } else if (split instanceof NumberBucketExpression) {
      var size = split.size;
    }

    return (res: any[]): NativeDataset => {
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
            d[label] = new TimeRange({ start: v, end: v + size })
          }
          return d;
        })
      }
      return new NativeDataset({ source: 'native', data: res });
    }
  }

  function postProcessIntrospect(columns: SQLDescribeRow[]): Attributes {
    var attributes: Attributes = Object.create(null);
    columns.forEach((column: SQLDescribeRow) => {
      var sqlType = column.Type;
      if (sqlType === "datetime") {
        attributes[column.Field] = new AttributeInfo({ type: 'TIME' });
      } else if (sqlType.indexOf("varchar(") === 0) {
        attributes[column.Field] = new AttributeInfo({ type: 'STRING' });
      } else if (sqlType.indexOf("int(") === 0 || sqlType.indexOf("bigint(") === 0) {
        // ToDo: make something special for integers
        attributes[column.Field] = new AttributeInfo({ type: 'NUMBER' });
      } else if (sqlType.indexOf("decimal(") === 0) {
        attributes[column.Field] = new AttributeInfo({ type: 'NUMBER' });
      }
    });
    return attributes;
  }

  export interface MySQLDatasetValue extends DatasetValue{
    table?: string;
  }

  export interface MySQLDatasetJS extends DatasetJS {
    table?: string;
  }

  export class MySQLDataset extends RemoteDataset {
    static type = 'DATASET';

    static fromJS(datasetJS: any): MySQLDataset {
      var value: MySQLDatasetValue = RemoteDataset.jsToValue(datasetJS);
      value.table = datasetJS.table;
      return new MySQLDataset(value);
    }

    public table: string;

    constructor(parameters: MySQLDatasetValue) {
      super(parameters, dummyObject);
      this._ensureSource("mysql");
      this.table = parameters.table;
    }

    public valueOf(): DatasetValue {
      var value: MySQLDatasetValue = super.valueOf();
      value.table = this.table;
      return value;
    }

    public toJS(): DatasetJS {
      var js: MySQLDatasetJS = super.toJS();
      js.table = this.table;
      return js;
    }

    public equals(other: MySQLDataset): boolean {
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
          query.push(this.applies.map(apply => apply.getSQL(mySQLDialect)).join(',\n'));
          query.push('FROM ' + table);
          if (!(this.filter.equals(Expression.TRUE))) {
            query.push('WHERE ' + this.filter.getSQL(mySQLDialect));
          }
          query.push("GROUP BY ''");
          break;

        case 'split':
          query.push(
            [`${this.split.getSQL(mySQLDialect)} AS '${this.key}'`]
              .concat(this.applies.map(apply => apply.getSQL(mySQLDialect))).join(',\n')
          );
          query.push('FROM ' + table);
          if (!(this.filter.equals(Expression.TRUE))) {
            query.push('WHERE ' + this.filter.getSQL(mySQLDialect));
          }
          query.push('GROUP BY ' + this.split.getSQL(mySQLDialect, true));
          if (!(this.havingFilter.equals(Expression.TRUE))) {
            query.push('HAVING ' + this.havingFilter.getSQL(mySQLDialect));
          }
          if (this.sort) {
            query.push(this.sort.getSQL(mySQLDialect));
          }
          if (this.limit) {
            query.push(this.limit.getSQL(mySQLDialect));
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
  Dataset.register(MySQLDataset, 'mysql');
}
