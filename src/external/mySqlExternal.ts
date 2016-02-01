module Plywood {
  var mySQLDialect = new MySQLDialect();

  const DEFAULT_TIMEZONE = Timezone.UTC;

  interface SQLDescribeRow {
    Field: string;
    Type: string;
  }

  function correctResult(result: any[]): boolean {
    return Array.isArray(result) && (result.length === 0 || typeof result[0] === 'object');
  }

  function postProcessFactory(split: SplitAction): PostProcess {
    var inflaters = split ? split.mapSplits((label, splitExpression) => {
      if (splitExpression instanceof ChainExpression) {
        var lastAction = splitExpression.lastAction();

        if (lastAction instanceof TimeBucketAction) {
          return External.timeRangeInflaterFactory(label, lastAction.duration, lastAction.timezone || DEFAULT_TIMEZONE);
        }

        if (lastAction instanceof NumberBucketAction) {
          return External.numberRangeInflaterFactory(label, lastAction.size);
        }
      }
    }) : [];

    return (data: any[]): Dataset => {
      if (!correctResult(data)) {
        var err = new Error("unexpected result from MySQL");
        (<any>err).result = data; // ToDo: special error type
        throw err;
      }

      var n = data.length;
      for (var inflater of inflaters) {
        for (var i = 0; i < n; i++) {
          inflater(data[i], i, data);
        }
      }

      return new Dataset({ data });
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
          query.push(this.attributes.map(a => mySQLDialect.escapeName(a.name)).join(', '));
          query.push('FROM ' + table);
          if (!(this.filter.equals(Expression.TRUE))) {
            query.push('WHERE ' + this.filter.getSQL(mySQLDialect));
          }
          if (this.sort) {
            query.push(this.sort.getSQL('', mySQLDialect));
          }
          if (this.limit) {
            query.push(this.limit.getSQL('', mySQLDialect));
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
            this.split.getSelectSQL(mySQLDialect)
              .concat(this.applies.map(apply => apply.getSQL('', mySQLDialect)))
              .join(',\n')
          );
          query.push('FROM ' + table);
          if (!(this.filter.equals(Expression.TRUE))) {
            query.push('WHERE ' + this.filter.getSQL(mySQLDialect));
          }
          query.push(this.split.getShortGroupBySQL());
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
        postProcess: postProcessFactory(this.split)
      };
    }

    public getIntrospectAttributes(): Q.Promise<Attributes> {
      return this.requester({ query: "DESCRIBE `" + this.table + "`", }).then(postProcessIntrospect);
    }
  }

  External.register(MySQLExternal, 'mysql');
}
