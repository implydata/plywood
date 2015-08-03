module Plywood {
  export class LiteralExpression extends Expression {
    static fromJS(parameters: ExpressionJS): Expression {
      var value: ExpressionValue = {
        op: parameters.op,
        type: parameters.type
      };
      var v: any = parameters.value;
      if (isHigherObject(v)) {
        value.value = v;
      } else {
        value.value = valueFromJS(v, parameters.type);
      }
      return new LiteralExpression(value);
    }

    public value: any;

    constructor(parameters: ExpressionValue) {
      super(parameters, dummyObject);
      var value = parameters.value;
      this.value = value;
      this._ensureOp("literal");
      if (typeof this.value === 'undefined') {
        throw new TypeError("must have a `value`")
      }
      this.type = getValueType(value);
      this.simple = true;
    }

    public valueOf(): ExpressionValue {
      var value = super.valueOf();
      value.value = this.value;
      if (this.type) value.type = this.type;
      return value;
    }

    public toJS(): ExpressionJS {
      var js = super.toJS();
      if (this.value && this.value.toJS) {
        js.value = this.value.toJS();
        js.type = (this.type.indexOf('SET/') === 0) ? 'SET' : this.type;
      } else {
        js.value = this.value;
      }
      return js;
    }

    public toString(): string {
      var value = this.value;
      if (value instanceof Dataset && value.basis()) {
        return '$()';
      } else if (this.type === 'STRING') {
        return JSON.stringify(value);
      } else {
        return String(value);
      }
    }

    public getFn(): ComputeFn {
      var value = this.value;
      return () => value;
    }

    public getJS(datumVar: string): string {
      return JSON.stringify(this.value); // ToDo: what to do with higher objects?
    }

    public getSQL(dialect: SQLDialect): string {
      var value = this.value;
      switch (this.type) {
        case 'STRING':
          return JSON.stringify(value);

        case 'BOOLEAN':
          return String(value).toUpperCase();

        case 'NUMBER':
          return String(value);

        case 'NUMBER_RANGE':
          return String(value.start) + '/' + String(value.end);

        case 'TIME':
          return timeToSQL(<Date>value);

        case 'TIME_RANGE':
          return timeToSQL(value.start) + '/' + timeToSQL(value.end);

        case 'SET/STRING':
          return '(' + (<Set>value).elements.map((v: string) => JSON.stringify(v)).join(',') + ')';

        default:
          throw new Error("currently unsupported type: " + this.type);
      }
    }

    public equals(other: LiteralExpression): boolean {
      if (!super.equals(other) || this.type !== other.type) return false;
      if (this.value) {
        if (this.value.equals) {
          return this.value.equals(other.value);
        } else if (this.value.toISOString && other.value.toISOString) {
          return this.value.valueOf() === other.value.valueOf();
        } else {
          return this.value === other.value;
        }
      } else {
        return this.value === other.value;
      }
    }

    public _collectBindSpecs(bindSpecs: BindSpec[], selectionDepth: Lookup<number>, depth: number, applyName: string, data: string, key: string): void {
      if (this.type === 'MARK') {
        if (depth !== 0) throw new Error('can not have a mark that is not in the base context');
        var mark = <Mark>this.value;
        var bindSpec = {
          selectionInput: '__base__',
          selector: mark.selector,
          selectionName: applyName
        };
        fillMethods(mark.prop, bindSpec);
        bindSpecs.push(bindSpec);
        selectionDepth[applyName] = 0;
      }
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      indexer.index++;
      if (this.type == 'DATASET') {
        var newTypeContext = (<Dataset>this.value).getFullType();
        newTypeContext.parent = typeContext;
        return newTypeContext;
      } else {
        return { type: this.type };
      }
    }

    public _computeResolved(): Q.Promise<any> {
      var value = this.value;
      if (value instanceof External) {
        return value.queryValues();
      } else {
        return Q(this.value);
      }
    }

  }

  Expression.ZERO = new LiteralExpression({ value: 0 });
  Expression.ONE = new LiteralExpression({ value: 1 });
  Expression.FALSE = new LiteralExpression({ value: false });
  Expression.TRUE = new LiteralExpression({ value: true });

  Expression.register(LiteralExpression);
}
