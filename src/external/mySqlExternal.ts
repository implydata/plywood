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

  function getSplitInflaters(split: SplitAction): Inflater[] {
    return split.mapSplits((label, splitExpression) => {
      if (splitExpression.type === 'BOOLEAN') {
        return External.booleanInflaterFactory(label);
      }

      if (splitExpression instanceof ChainExpression) {
        var lastAction = splitExpression.lastAction();

        if (lastAction instanceof TimeBucketAction) {
          return External.timeRangeInflaterFactory(label, lastAction.duration, lastAction.timezone || DEFAULT_TIMEZONE);
        }

        if (lastAction instanceof NumberBucketAction) {
          return External.numberRangeInflaterFactory(label, lastAction.size);
        }
      }

      return;
    })
  }

  function valuePostProcess(data: any[]): PlywoodValue {
    if (!correctResult(data)) {
      var err = new Error("unexpected result from MySQL (value)");
      (<any>err).result = data; // ToDo: special error type
      throw err;
    }

    return data.length ? data[0][External.VALUE_NAME] : 0;
  }

  function postProcessFactory(inflaters: Inflater[]): PostProcess {
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

  function postProcessIntrospect(columns: SQLDescribeRow[]): IntrospectResult {
    var attributes = columns.map((column: SQLDescribeRow) => {
      var name = column.Field;
      var sqlType = column.Type.toLowerCase();
      if (sqlType === "datetime") {
        return new AttributeInfo({ name, type: 'TIME' });
      } else if (sqlType.indexOf("varchar(") === 0 || sqlType.indexOf("blob") === 0) {
        return new AttributeInfo({ name, type: 'STRING' });
      } else if (sqlType.indexOf("int(") === 0 || sqlType.indexOf("bigint(") === 0) {
        // ToDo: make something special for integers
        return new AttributeInfo({ name, type: 'NUMBER' });
      } else if (sqlType.indexOf("decimal(") === 0 || sqlType.indexOf("float") === 0 || sqlType.indexOf("double") === 0) {
        return new AttributeInfo({ name, type: 'NUMBER' });
      } else if (sqlType.indexOf("tinyint(1)") === 0) {
        return new AttributeInfo({ name, type: 'BOOLEAN' });
      }
      return null;
    }).filter(Boolean);

    return {
      version: null,
      attributes
    }
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
      const { table, mode, split, applies, sort, limit, derivedAttributes } = this;

      var query = ['SELECT'];
      var postProcess: PostProcess = null;
      var inflaters: Inflater[] = [];

      var from = "FROM `" + table + "`";
      var filter = this.getQueryFilter();
      if (!filter.equals(Expression.TRUE)) {
        from += '\nWHERE ' + filter.getSQL(mySQLDialect);
      }

      switch (mode) {
        case 'raw':
          var selectedAttributes = this.getSelectedAttributes();

          selectedAttributes.forEach(attribute => {
            if (attribute.type === 'BOOLEAN') {
              inflaters.push(External.booleanInflaterFactory(attribute.name));
            }
          });

          query.push(
            selectedAttributes.map(a => {
              var name = a.name;
              if (derivedAttributes[name]) {
                return new ApplyAction({ name, expression: derivedAttributes[name] }).getSQL('', mySQLDialect)
              } else {
                return mySQLDialect.escapeName(name);
              }
            }).join(', '),
            from
          );
          if (sort) {
            query.push(sort.getSQL('', mySQLDialect));
          }
          if (limit) {
            query.push(limit.getSQL('', mySQLDialect));
          }
          break;

        case 'value':
          query.push(
            this.toValueApply().getSQL('', mySQLDialect),
            from,
            "GROUP BY ''"
          );
          postProcess = valuePostProcess;
          break;

        case 'total':
          query.push(
            applies.map(apply => apply.getSQL('', mySQLDialect)).join(',\n'),
            from,
            "GROUP BY ''"
          );
          break;

        case 'split':
          query.push(
            split.getSelectSQL(mySQLDialect)
              .concat(applies.map(apply => apply.getSQL('', mySQLDialect)))
              .join(',\n'),
            from,
            split.getShortGroupBySQL()
          );
          if (!(this.havingFilter.equals(Expression.TRUE))) {
            query.push('HAVING ' + this.havingFilter.getSQL(mySQLDialect));
          }
          if (sort) {
            query.push(sort.getSQL('', mySQLDialect));
          }
          if (limit) {
            query.push(limit.getSQL('', mySQLDialect));
          }
          inflaters = getSplitInflaters(split);
          break;

        default:
          throw new Error(`can not get query for mode: ${mode}`);
      }

      return {
        query: query.join('\n'),
        postProcess: postProcess || postProcessFactory(inflaters)
      };
    }

    public getIntrospectAttributes(): Q.Promise<IntrospectResult> {
      return this.requester({ query: "DESCRIBE `" + this.table + "`", }).then(postProcessIntrospect);
    }
  }

  External.register(MySQLExternal, 'mysql');
}
