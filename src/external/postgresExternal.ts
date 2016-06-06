module Plywood {
  interface SQLDescribeRow {
    name: string;
    sqlType: string;
  }

  function postProcessIntrospect(columns: SQLDescribeRow[]): IntrospectResult {
    var attributes = columns.map((column: SQLDescribeRow) => {
      var name = column.name;
      var sqlType = column.sqlType.toLowerCase();
      if (sqlType.indexOf('timestamp') !== -1) {
        return new AttributeInfo({ name, type: 'TIME' });
      } else if (sqlType === 'character varying') {
        return new AttributeInfo({ name, type: 'STRING' });
      } else if (sqlType === 'integer' || sqlType === 'bigint') {
        // ToDo: make something special for integers
        return new AttributeInfo({ name, type: 'NUMBER' });
      } else if (sqlType === "double precision" || sqlType === "float") {
        return new AttributeInfo({ name, type: 'NUMBER' });
      } else if (sqlType === 'boolean') {
        return new AttributeInfo({ name, type: 'BOOLEAN' });
      }
      return null;
    }).filter(Boolean);

    return {
      version: null,
      attributes
    }
  }

  export class PostgresExternal extends SQLExternal {
    static type = 'DATASET';

    static fromJS(parameters: ExternalJS, requester: Requester.PlywoodRequester<any>): PostgresExternal {
      var value: ExternalValue = SQLExternal.jsToValue(parameters, requester);
      return new PostgresExternal(value);
    }

    static getSourceList(requester: Requester.PlywoodRequester<any>): Q.Promise<string[]> {
      return requester({
        query: `SELECT table_name AS "tab" FROM INFORMATION_SCHEMA.TABLES WHERE table_type = 'BASE TABLE' AND table_schema = 'public'`
      })
        .then((sources) => {
          if (!Array.isArray(sources)) throw new Error('invalid sources response');
          if (!sources.length) return sources;
          return sources.map((s: PseudoDatum) => s['tab']).sort();
        });
    }

    constructor(parameters: ExternalValue) {
      super(parameters, new PostgresDialect());
      this._ensureEngine("postgres");
    }

    public getIntrospectAttributes(): Q.Promise<IntrospectResult> {
      return this.requester({
        query: `SELECT "column_name" AS "name", "data_type" AS "sqlType" FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = ${this.dialect.escapeLiteral(this.table)}`,
      }).then(postProcessIntrospect);
    }
  }

  External.register(PostgresExternal, 'postgres');
}
