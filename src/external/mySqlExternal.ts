module Plywood {
  interface SQLDescribeRow {
    Field: string;
    Type: string;
  }

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
      var err = new Error("unexpected result from MySQL (value)");
      (<any>err).result = data; // ToDo: special error type
      throw err;
    }

    return data.length ? data[0][External.VALUE_NAME] : 0;
  }

  function postProcessFactory(inflaters: Inflater[], zeroTotalApplies: ApplyAction[]): PostProcess {
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

      if (n === 0 && zeroTotalApplies) {
        data = [External.makeZeroDatum(zeroTotalApplies)];
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

  export class MySQLExternal extends SQLExternal {
    static type = 'DATASET';

    static fromJS(parameters: ExternalJS, requester: Requester.PlywoodRequester<any>): MySQLExternal {
      var value: ExternalValue = SQLExternal.jsToValue(parameters, requester);
      return new MySQLExternal(value);
    }

    static getSourceList(requester: Requester.PlywoodRequester<any>): Q.Promise<string[]> {
      return requester({ query: "SHOW TABLES" })
        .then((sources) => {
          if (!Array.isArray(sources)) throw new Error('invalid sources response');
          if (!sources.length) return sources;
          var key = Object.keys(sources[0])[0];
          if (!key) throw new Error('invalid sources response (no key)');
          return sources.map((s: PseudoDatum) => s[key]);
        });
    }

    public table: string;

    constructor(parameters: ExternalValue) {
      super(parameters, new MySQLDialect());
      this._ensureEngine("mysql");
    }

    public getIntrospectAttributes(): Q.Promise<IntrospectResult> {
      return this.requester({ query: `DESCRIBE ${this.dialect.escapeName(this.table)}`, }).then(postProcessIntrospect);
    }
  }

  External.register(MySQLExternal, 'mysql');
}
