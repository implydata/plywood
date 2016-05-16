module Plywood {
  function correctResult(result: any[]): boolean {
    return Array.isArray(result) && (result.length === 0 || typeof result[0] === 'object');
  }

  function getSplitInflaters(split: SplitAction): Inflater[] {
    return split.mapSplits((label, splitExpression) => {
      var simpleInflater = External.getSimpleInflater(splitExpression, label);
      if (simpleInflater) return simpleInflater;

      if (splitExpression instanceof ChainExpression) {
        var lastAction = splitExpression.lastAction();

        if (lastAction instanceof TimeBucketAction) {
          return External.timeRangeInflaterFactory(label, lastAction.duration, lastAction.getTimezone());
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
      var err = new Error("unexpected result (value)");
      (<any>err).result = data; // ToDo: special error type
      throw err;
    }

    return data.length ? data[0][External.VALUE_NAME] : 0;
  }

  function postProcessFactory(inflaters: Inflater[], zeroTotalApplies: ApplyAction[]): PostProcess {
    return (data: any[]): Dataset => {
      if (!correctResult(data)) {
        var err = new Error("unexpected result");
        (<any>err).result = data; // ToDo: special error type
        throw err;
      }

      var n = data.length;
      for (var inflater of inflaters) {
        for (var i = 0; i < n; i++) {
          inflater(data[i], i, data);
        }
      }

      if (n === 0 && zeroTotalApplies) {
        data = [External.makeZeroDatum(zeroTotalApplies)];
      }

      return new Dataset({ data });
    }
  }

  export class SQLExternal extends External {
    static type = 'DATASET';

    static jsToValue(parameters: ExternalJS, requester: Requester.PlywoodRequester<any>): ExternalValue {
      var value: ExternalValue = External.jsToValue(parameters, requester);
      value.table = parameters.table;
      return value;
    }

    public table: string;
    public dialect: SQLDialect;

    constructor(parameters: ExternalValue, dialect: SQLDialect) {
      super(parameters, dummyObject);
      this.table = parameters.table;
      this.dialect = dialect;
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

    public equals(other: SQLExternal): boolean {
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
      const { table, mode, applies, sort, limit, derivedAttributes, dialect } = this;

      var query = ['SELECT'];
      var postProcess: PostProcess = null;
      var inflaters: Inflater[] = [];
      var zeroTotalApplies: ApplyAction[] = null;

      var from = "FROM " + this.dialect.escapeName(table);
      var filter = this.getQueryFilter();
      if (!filter.equals(Expression.TRUE)) {
        from += '\nWHERE ' + filter.getSQL(dialect);
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
                return new ApplyAction({ name, expression: derivedAttributes[name] }).getSQL('', dialect)
              } else {
                return dialect.escapeName(name);
              }
            }).join(', '),
            from
          );
          if (sort) {
            query.push(sort.getSQL('', dialect));
          }
          if (limit) {
            query.push(limit.getSQL('', dialect));
          }
          break;

        case 'value':
          query.push(
            this.toValueApply().getSQL('', dialect),
            from,
            dialect.constantGroupBy()
          );
          postProcess = valuePostProcess;
          break;

        case 'total':
          zeroTotalApplies = applies;
          query.push(
            applies.map(apply => apply.getSQL('', dialect)).join(',\n'),
            from,
            dialect.constantGroupBy()
          );
          break;

        case 'split':
          var split = this.getQuerySplit();
          query.push(
            split.getSelectSQL(dialect)
              .concat(applies.map(apply => apply.getSQL('', dialect)))
              .join(',\n'),
            from,
            split.getShortGroupBySQL()
          );
          if (!(this.havingFilter.equals(Expression.TRUE))) {
            query.push('HAVING ' + this.havingFilter.getSQL(dialect));
          }
          if (sort) {
            query.push(sort.getSQL('', dialect));
          }
          if (limit) {
            query.push(limit.getSQL('', dialect));
          }
          inflaters = getSplitInflaters(split);
          break;

        default:
          throw new Error(`can not get query for mode: ${mode}`);
      }

      return {
        query: query.join('\n'),
        postProcess: postProcess || postProcessFactory(inflaters, zeroTotalApplies)
      };
    }

    public getIntrospectAttributes(): Q.Promise<IntrospectResult> {
      throw new Error('implement me');
    }
  }
}
