module Plywood {

  export class SplitAction extends Action {
    static fromJS(parameters: ActionJS): SplitAction {
      var value: ActionValue = {
        action: parameters.action
      };
      var splits: SplitsJS;
      if (parameters.expression && parameters.name) {
        splits = { [parameters.name]: parameters.expression };
      } else {
        splits = parameters.splits;
      }
      value.splits = helper.expressionLookupFromJS(splits);
      value.dataName = parameters.dataName;
      return new SplitAction(value);
    }

    public keys: string[];
    public splits: Splits;
    public dataName: string;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      var splits = parameters.splits;
      if (!splits) throw new Error('must have splits');
      this.splits = splits;
      this.keys = Object.keys(splits).sort();
      if (!this.keys.length) throw new Error('must have at least one split');
      this.dataName = parameters.dataName;
      this._ensureAction("split");
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.splits = this.splits;
      value.dataName = this.dataName;
      return value;
    }

    public toJS(): ActionJS {
      var { splits } = this;

      var js = super.toJS();
      if (this.isMultiSplit()) {
        js.splits = helper.expressionLookupToJS(splits);
      } else {
        for (var name in splits) {
          js.name = name;
          js.expression = splits[name].toJS();
        }
      }
      js.dataName = this.dataName;
      return js;
    }

    public equals(other: SplitAction): boolean {
      return super.equals(other) &&
        immutableLookupsEqual(this.splits, other.splits) &&
        this.dataName === other.dataName;
    }

    protected _toStringParameters(expressionString: string): string[] {
      if (this.isMultiSplit()) {
        var { splits } = this;
        var splitStrings: string[] = [];
        for (var name in splits) {
          splitStrings.push(`${name}: ${splits[name]}`);
        }
        return [splitStrings.join(', '), this.dataName];
      } else {
        return [this.firstSplitExpression().toString(), this.firstSplitName(), this.dataName];
      }
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'DATASET');
      return 'DATASET';
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType {
      var newDatasetType: Lookup<FullType> = {};
      this.mapSplits((name, expression) => {
        newDatasetType[name] = expression._fillRefSubstitutions(typeContext, indexer, alterations);
      });
      newDatasetType[this.dataName] = typeContext;

      return {
        parent: typeContext.parent,
        type: 'DATASET',
        datasetType: newDatasetType,
        remote: false
      };
    }

    public getFn(inputFn: ComputeFn): ComputeFn {
      var { dataName } = this;
      var splitFns = this.mapSplitExpressions((ex) => ex.getFn());
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        return inV ? inV.split(splitFns, dataName) : null;
      }
    }

    public getSQL(inputSQL: string, dialect: SQLDialect): string {
      var groupBys = this.mapSplits((name, expression) => expression.getSQL(dialect));
      return `GROUP BY ${groupBys.join(', ')}`;
    }

    public getSelectSQL(dialect: SQLDialect): string[] {
      return this.mapSplits((name, expression) => `${expression.getSQL(dialect)} AS ${dialect.escapeName(name)}`);
    }

    public getShortGroupBySQL(): string {
      return 'GROUP BY ' + Object.keys(this.splits).map((d, i) => i + 1).join(', ');
    }

    public expressionCount(): int {
      var count = 0;
      this.mapSplits((k, expression) => {
        count += expression.expressionCount();
      });
      return count;
    }

    public fullyDefined(): boolean {
      return false; // Do not try to simplify for now
    }

    public simplify(): Action {
      if (this.simple) return this;

      var simpleSplits = this.mapSplitExpressions((ex) => ex.simplify());

      var value = this.valueOf();
      value.splits = simpleSplits;
      value.simple = true;
      return new SplitAction(value);
    }

    public getExpressions(): Expression[] {
      return this.mapSplits((name, ex) => ex);
    }

    public _substituteHelper(substitutionFn: SubstitutionFn, thisArg: any, indexer: Indexer, depth: int, nestDiff: int): Action {
      var nestDiffNext = nestDiff + 1;
      var hasChanged = false;
      var subSplits = this.mapSplitExpressions((ex) => {
        var subExpression = ex._substituteHelper(substitutionFn, thisArg, indexer, depth, nestDiffNext);
        if (subExpression !== ex) hasChanged = true;
        return subExpression;
      });
      if (!hasChanged) return this;
      var value = this.valueOf();
      value.splits = subSplits;
      return new SplitAction(value);
    }

    public isNester(): boolean {
      return true;
    }

    public numSplits(): number {
      return this.keys.length;
    }

    public isMultiSplit(): boolean {
      return this.numSplits() > 1;
    }

    public mapSplits<T>(fn: (name: string, expression?: Expression) => T): T[] {
      var { splits, keys } = this;
      var res: T[] = [];
      for (var k of keys) {
        var v = fn(k, splits[k]);
        if (typeof v !== 'undefined') res.push(v);
      }
      return res;
    }

    public mapSplitExpressions<T>(fn: (expression: Expression, name?: string) => T): Lookup<T> {
      var { splits, keys } = this;
      var ret: Lookup<T> = Object.create(null);
      for (var key of keys) {
        ret[key] = fn(splits[key], key);
      }
      return ret;
    }

    public transformExpressions(fn: (expression: Expression, name?: string) => Expression): SplitAction {
      var { splits, keys } = this;
      var newSplits: Lookup<Expression> = Object.create(null);
      var changed = false;
      for (var key of keys) {
        var ex = splits[key];
        var transformed = fn(ex, key);
        if (transformed !== ex) changed = true;
        newSplits[key] = transformed;
      }
      if (!changed) return this;
      var value = this.valueOf();
      value.splits = newSplits;
      return new SplitAction(value);
    }

    public firstSplitName(): string {
      return this.keys[0];
    }

    public firstSplitExpression(): Expression {
      return this.splits[this.firstSplitName()];
    }

    public filterFromDatum(datum: Datum): Expression {
      return Expression.and(this.mapSplits((name, expression) => {
        if (isSetType(expression.type)) {
          return r(datum[name]).in(expression);
        } else {
          return expression.is(r(datum[name]));
        }
      })).simplify();
    }

    public hasKey(key: string): boolean {
      return hasOwnProperty(this.splits, key);
    }

    public maxBucketNumber(): number {
      var { splits, keys } = this;
      var num = 1;
      for (var key of keys) {
        num *= splits[key].maxPossibleSplitValues();
      }
      return num;
    }

    public isAggregate() {
      return true;
    }
  }

  Action.register(SplitAction);
}
