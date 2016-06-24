module Plywood {
  export class LiteralExpression extends Expression {
    static fromJS(parameters: ExpressionJS): LiteralExpression {
      var value: ExpressionValue = {
        op: parameters.op,
        type: parameters.type
      };
      if (!hasOwnProperty(parameters, 'value')) throw new Error('literal expression must have value');
      var v: any = parameters.value;
      if (isImmutableClass(v)) {
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
        js.type = isSetType(this.type) ? 'SET' : this.type;
      } else {
        js.value = this.value;
        if (this.type === 'TIME') js.type = 'TIME';
      }
      return js;
    }

    public toString(): string {
      var value = this.value;
      if (value instanceof Dataset && value.basis()) {
        return 'ply()';
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
      if (value === null) return 'NULL';

      switch (this.type) {
        case 'STRING':
          return dialect.escapeLiteral(value);

        case 'BOOLEAN':
          return dialect.booleanToSQL(value);

        case 'NUMBER':
          return dialect.numberToSQL(value);

        case 'NUMBER_RANGE':
          return `${dialect.numberToSQL(value.start)}`;

        case 'TIME':
          return dialect.timeToSQL(<Date>value);

        case 'TIME_RANGE':
          return `${dialect.timeToSQL(value.start)}`;

        case 'SET/STRING':
        case 'SET/NUMBER':
          return '(' + (<Set>value).elements.map((v: any) => typeof v === 'number' ? v : dialect.escapeLiteral(v)).join(',') + ')';

        case 'SET/NUMBER_RANGE':
        case 'SET/TIME_RANGE':
          return 'FALSE'; // ToDo: fix these dummies

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

    public _fillRefSubstitutions(typeContext: DatasetFullType, indexer: Indexer, alterations: Alterations): FullType {
      indexer.index++;
      if (this.type == 'DATASET') {
        var newTypeContext = (<Dataset>this.value).getFullType();
        newTypeContext.parent = typeContext;
        return newTypeContext;
      } else {
        return { type: <PlyTypeSimple>this.type };
      }
    }

    public getLiteralValue(): any {
      return this.value;
    }

    public _computeResolvedSimulate(): PlywoodValue {
      return this.value;
    }

    public _computeResolved(): Q.Promise<PlywoodValue> {
      return Q(this.value);
    }

    public maxPossibleSplitValues(): number {
      const { value } = this;
      return Set.isSet(value) ? value.size() : 1;
    }

    public bumpStringLiteralToTime(): Expression {
      if (this.type !== 'STRING') return this;
      var parse = parseISODate(this.value, defaultParserTimezone);
      if (!parse) throw new Error(`could not parse '${this.value}' as time`);
      return r(parse);
    }

    public bumpStringLiteralToSetString(): Expression {
      if (this.type !== 'STRING') return this;
      return r(Set.fromJS([this.value]));
    }
  }

  Expression.NULL = new LiteralExpression({ value: null });
  Expression.ZERO = new LiteralExpression({ value: 0 });
  Expression.ONE = new LiteralExpression({ value: 1 });
  Expression.FALSE = new LiteralExpression({ value: false });
  Expression.TRUE = new LiteralExpression({ value: true });
  Expression.EMPTY_STRING = new LiteralExpression({ value: '' });
  Expression.EMPTY_SET = new LiteralExpression({ value: Set.fromJS([]) });

  Expression.register(LiteralExpression);
}
