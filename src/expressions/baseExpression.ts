module Plywood {

  export interface SubstitutionFn {
    (ex: Expression, index?: int, depth?: int, nestDiff?: int): Expression;
  }

  export interface BooleanExpressionIterator {
    (ex: Expression, index?: int, depth?: int, nestDiff?: int): boolean;
  }

  export interface VoidExpressionIterator {
    (ex: Expression, index?: int, depth?: int, nestDiff?: int): void;
  }

  export interface DatasetBreakdown {
    singleDatasetActions: ApplyAction[];
    combineExpression: Expression;
  }

  export interface Digest {
    expression: Expression;
    undigested: ApplyAction;
  }

  export interface Indexer {
    index: int
  }

  export type Alterations = Lookup<Expression>;

  export interface ExpressionValue {
    op?: string;
    type?: string;
    simple?: boolean;
    value?: any;
    name?: string;
    nest?: int;
    dataset?: RemoteDataset;
    expression?: Expression;
    actions?: Action[];

    remote?: string[];
  }

  export interface ExpressionJS {
    op: string;
    type?: string;
    value?: any;
    name?: string;
    nest?: int;
    dataset?: DatasetJS; // ToDo: make RemoteDatasetJS
    action?: ActionJS;
    expression?: ExpressionJS;
    actions?: ActionJS[];
  }

  export interface Separation {
    included: Expression;
    excluded: Expression;
  }

  export var simulatedQueries: any[] = null;

  function getDataName(ex: Expression): string {
    if (ex instanceof RefExpression) {
      return ex.name;
    } else if (ex instanceof ChainExpression) {
      return getDataName(ex.expression);
    } else {
      return null;
    }
  }

  export function mergeRemotes(remotes: string[][]): string[] {
    var lookup: Lookup<boolean> = {};
    for (let remote of remotes) {
      if (!remote) continue;
      for (let r of remote) {
        lookup[r] = true;
      }
    }
    var merged = Object.keys(lookup);
    return merged.length ? merged.sort() : null;
  }

  /**
   * The expression starter function. Performs different operations depending on the type and value of the input
   * $() produces a native dataset with a singleton empty datum inside of it. This is useful to describe the base container
   * $('blah') produces an reference lookup expression on 'blah'
   *
   * @param input The input that can be nothing, a string, or a driver
   * @returns {Expression}
   */
  export function $(input?: any): Expression {
    if (arguments.length) {
      if (typeof input === 'string') {
        return RefExpression.parse(input);
      } else {
        return new LiteralExpression({ op: 'literal', value: input });
      }
    } else {
      return new LiteralExpression({
        op: 'literal',
        value: new NativeDataset({ source: 'native', data: [{}] })
      });
    }
  }

  export function mark(selector: string, prop: Lookup<any> = {}): Mark {
    return new Mark({
      selector,
      prop
    })
  }

  var check: ImmutableClass<ExpressionValue, ExpressionJS>;

  /**
   * Provides a way to express arithmetic operations, aggregations and database operators.
   * This class is the backbone of facet.js
   */
  export class Expression implements ImmutableInstance<ExpressionValue, ExpressionJS> {
    static ZERO: LiteralExpression;
    static ONE: LiteralExpression;
    static FALSE: LiteralExpression;
    static TRUE: LiteralExpression;

    static isExpression(candidate: any): boolean {
      return isInstanceOf(candidate, Expression);
    }

    /**
     * Parses an expression
     *
     * @param str The expression to parse
     * @returns {Expression}
     */
    static parse(str: string): Expression {
      try {
        return expressionParser.parse(str);
      } catch (e) {
        // Re-throw to add the stacktrace
        throw new Error('Expression parse error: ' + e.message + ' on `' + str + '`');
      }
    }

    /**
     * Parses SQL statements into facet expressions
     *
     * @param str The SQL to parse
     * @returns {Expression}
     */
    static parseSQL(str: string): Expression {
      try {
        return sqlParser.parse(str);
      } catch (e) {
        // Re-throw to add the stacktrace
        throw new Error('SQL parse error: ' + e.message + ' on `' + str + '`');
      }
    }

    /**
     * Deserializes or parses an expression
     *
     * @param param The expression to parse
     * @returns {Expression}
     */
    static fromJSLoose(param: any): Expression {
      var expressionJS: ExpressionJS;
      // Quick parse simple expressions
      switch (typeof param) {
        case 'object':
          if (Expression.isExpression(param)) {
            return param
          } else if (isHigherObject(param)) {
            if (param.constructor.type) {
              // Must be a datatype
              expressionJS = { op: 'literal', value: param };
            } else {
              throw new Error("unknown object"); //ToDo: better error
            }
          } else if (param.op) {
            expressionJS = <ExpressionJS>param;
          } else if (param.toISOString) {
            expressionJS = { op: 'literal', value: new Date(param) };
          } else if (Array.isArray(param)) {
            expressionJS = { op: 'literal', value: Set.fromJS(param) };
          } else if (hasOwnProperty(param, 'start') && hasOwnProperty(param, 'end')) {
            expressionJS = { op: 'literal', value: Range.fromJS(param) };
          } else {
            throw new Error('unknown parameter');
          }
          break;

        case 'number':
        case 'boolean':
          expressionJS = { op: 'literal', value: param };
          break;

        case 'string':
          if (/^[\w ]+$/.test(param)) { // ToDo: is [\w ] right?
            expressionJS = { op: 'literal', value: param };
          } else {
            return Expression.parse(param);
          }
          break;

        default:
          throw new Error("unrecognizable expression");
      }

      return Expression.fromJS(expressionJS);
    }

    static classMap: Lookup<typeof Expression> = {};
    static register(ex: typeof Expression): void {
      var op = (<any>ex).name.replace('Expression', '').replace(/^\w/, (s: string) => s.toLowerCase());
      Expression.classMap[op] = ex;
    }

    /**
     * Deserializes the expression JSON
     *
     * @param expressionJS
     * @returns {any}
     */
    static fromJS(expressionJS: ExpressionJS): Expression {
      if (!hasOwnProperty(expressionJS, "op")) {
        throw new Error("op must be defined");
      }
      var op = expressionJS.op;
      if (typeof op !== "string") {
        throw new Error("op must be a string");
      }
      var ClassFn = Expression.classMap[op];
      if (!ClassFn) {
        throw new Error(`unsupported expression op '${op}'`);
      }

      return ClassFn.fromJS(expressionJS);
    }

    public op: string;
    public type: string;
    public simple: boolean;

    constructor(parameters: ExpressionValue, dummy: Dummy = null) {
      this.op = parameters.op;
      if (dummy !== dummyObject) {
        throw new TypeError("can not call `new Expression` directly use Expression.fromJS instead");
      }
      if (parameters.simple) this.simple = true;
    }

    protected _ensureOp(op: string) {
      if (!this.op) {
        this.op = op;
        return;
      }
      if (this.op !== op) {
        throw new TypeError("incorrect expression op '" + this.op + "' (needs to be: '" + op + "')");
      }
    }

    public valueOf(): ExpressionValue {
      var value: ExpressionValue = { op: this.op };
      if (this.simple) value.simple = true;
      return value;
    }

    /**
     * Serializes the expression into a simple JS object that can be passed to JSON.serialize
     *
     * @returns ExpressionJS
     */
    public toJS(): ExpressionJS {
      return {
        op: this.op
      };
    }

    /**
     * Makes it safe to call JSON.serialize on expressions
     *
     * @returns ExpressionJS
     */
    public toJSON(): ExpressionJS {
      return this.toJS();
    }

    /**
     * Validate that two expressions are equal in their meaning
     *
     * @param other
     * @returns {boolean}
     */
    public equals(other: Expression): boolean {
      return Expression.isExpression(other) &&
        this.op === other.op &&
        this.type === other.type;
    }

    /**
     * Check that the expression can potentially have the desired type
     * If wanted type is 'SET' then any SET/* type is matched
     *
     * @param wantedType The type that is wanted
     * @returns {boolean}
     */
    public canHaveType(wantedType: string): boolean {
      if (!this.type) return true;
      if (wantedType === 'SET') {
        return this.type.indexOf('SET/') === 0;
      } else {
        return this.type === wantedType;
      }
    }

    /**
     * Counts the number of expressions contained within this expression
     *
     * @returns {number}
     */
    public expressionCount(): int {
      return 1;
    }

    /**
     * Check if the expression is of the given operation (op)
     *
     * @param op The operation to test
     * @returns {boolean}
     */
    public isOp(op: string): boolean {
      return this.op === op;
    }

    /**
     * Check if the expression contains the given operation (op)
     *
     * @param op The operation to test
     * @returns {boolean}
     */
    public containsOp(op: string): boolean {
      return this.some((ex: Expression) => ex.isOp(op) || null);
    }

    /**
     * Check if the expression contains references to remote datasets
     *
     * @returns {boolean}
     */
    public hasRemote(): boolean {
      return this.some(function(ex: Expression) {
        if (ex instanceof RemoteExpression) return true;
        if (ex instanceof RefExpression) return ex.isRemote();
        return null; // search further
      });
    }

    public getRemoteDatasetIds(): string[] {
      var remoteDatasetIds: string[] = [];
      var push = Array.prototype.push;
      this.forEach(function(ex: Expression) {
        if (ex.type !== 'DATASET') return;
        if (ex instanceof LiteralExpression) {
          push.apply(remoteDatasetIds, (<Dataset>ex.value).getRemoteDatasetIds());
        } else if (ex instanceof RefExpression) {
          push.apply(remoteDatasetIds, ex.remote);
        }
      });
      return deduplicateSort(remoteDatasetIds);
    }

    public getRemoteDatasets(): RemoteDataset[] {
      var remoteDatasets: RemoteDataset[][] = [];
      this.forEach(function(ex: Expression) {
        if (ex instanceof LiteralExpression && ex.type === 'DATASET') {
          remoteDatasets.push((<Dataset>ex.value).getRemoteDatasets());
        }
      });
      return mergeRemoteDatasets(remoteDatasets);
    }

    /**
     * Retrieve all free references by name
     * returns the alphabetically sorted list of the references
     *
     * @returns {string[]}
     */
    public getFreeReferences(): string[] {
      var freeReferences: string[] = [];
      this.forEach((ex: Expression, index: int, depth: int, nestDiff: int) => {
        if (ex instanceof RefExpression && nestDiff <= ex.nest) {
          freeReferences.push(repeat('^', ex.nest - nestDiff) + ex.name);
        }
      });
      return deduplicateSort(freeReferences);
    }

    /**
     * Retrieve all free references by index in the query
     *
     * @returns {number[]}
     */
    public getFreeReferenceIndexes(): number[] {
      var freeReferenceIndexes: number[] = [];
      this.forEach((ex: Expression, index: int, depth: int, nestDiff: int) => {
        if (ex instanceof RefExpression && nestDiff <= ex.nest) {
          freeReferenceIndexes.push(index);
        }
      });
      return freeReferenceIndexes;
    }

    /**
     * Increment the ^ nesting on all the free reference variables within this expression

     * @param by The number of generation to increment by (default: 1)
     * @returns {any}
     */
    public incrementNesting(by: int = 1): Expression {
      var freeReferenceIndexes = this.getFreeReferenceIndexes();
      if (freeReferenceIndexes.length === 0) return this;
      return this.substitute((ex: Expression, index: int) => {
        if (ex instanceof RefExpression && freeReferenceIndexes.indexOf(index) !== -1) {
          return ex.incrementNesting(by);
        }
        return null;
      });
    }

    /**
     * Merge self with the provided expression for AND operation and returns a merged expression.
     *
     * @returns {Expression}
     */
    public mergeAnd(ex: Expression): Expression {
      throw new Error('can not call on base');
    }

    /**
     * Merge self with the provided expression for OR operation and returns a merged expression.
     *
     * @returns {Expression}
     */
    public mergeOr(ex: Expression): Expression {
      throw new Error('can not call on base');
    }

    /**
     * Returns an expression that is equivalent but no more complex
     * If no simplification can be done will return itself.
     *
     * @returns {Expression}
     */
    public simplify(): Expression {
      return this;
    }

    /**
     * Runs iter over all the sub expression and return true if iter returns true for everything
     *
     * @param iter The function to run
     * @param thisArg The this for the substitution function
     * @returns {boolean}
     */
    public every(iter: BooleanExpressionIterator, thisArg?: any): boolean {
      return this._everyHelper(iter, thisArg, { index: 0 }, 0, 0);
    }

    public _everyHelper(iter: BooleanExpressionIterator, thisArg: any, indexer: Indexer, depth: int, nestDiff: int): boolean {
      var pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
      if (pass != null) {
        return pass;
      } else {
        indexer.index++;
      }
      return true;
    }

    /**
     * Runs iter over all the sub expression and return true if iter returns true for anything
     *
     * @param iter The function to run
     * @param thisArg The this for the substitution function
     * @returns {boolean}
     */
    public some(iter: BooleanExpressionIterator, thisArg?: any): boolean {
      return !this.every((ex: Expression, index: int, depth: int, nestDiff: int) => {
        var v = iter.call(this, ex, index, depth, nestDiff);
        return (v == null) ? null : !v;
      }, thisArg);
    }

    /**
     * Runs iter over all the sub expressions
     *
     * @param iter The function to run
     * @param thisArg The this for the substitution function
     * @returns {boolean}
     */
    public forEach(iter: VoidExpressionIterator, thisArg?: any): void {
      this.every((ex: Expression, index: int, depth: int, nestDiff: int) => {
        iter.call(this, ex, index, depth, nestDiff);
        return null;
      }, thisArg);
    }

    /**
     * Performs a substitution by recursively applying the given substitutionFn to every sub-expression
     * if substitutionFn returns an expression than it is replaced; if null is returned this expression is returned
     *
     * @param substitutionFn The function with which to substitute
     * @param thisArg The this for the substitution function
     */
    public substitute(substitutionFn: SubstitutionFn, thisArg?: any): Expression {
      return this._substituteHelper(substitutionFn, thisArg, { index: 0 }, 0, 0);
    }

    public _substituteHelper(substitutionFn: SubstitutionFn, thisArg: any, indexer: Indexer, depth: int, nestDiff: int): Expression {
      var sub = substitutionFn.call(thisArg, this, indexer.index, depth, nestDiff);
      if (sub) {
        indexer.index += this.expressionCount();
        return sub;
      } else {
        indexer.index++;
      }

      return this;
    }


    public getFn(): ComputeFn {
      throw new Error('should never be called directly');
    }

    public getJS(datumVar: string): string {
      throw new Error('should never be called directly');
    }

    public getJSFn(): string {
      return `function(d){return ${this.getJS('d')};}`;
    }

    public getSQL(dialect: SQLDialect): string {
      throw new Error('should never be called directly');
    }

    public separateViaAnd(refName: string): Separation {
      if (typeof refName !== 'string') throw new Error('must have refName');
      if (this.type !== 'BOOLEAN') return null;
      var myRef = this.getFreeReferences();
      if (myRef.length > 1 && myRef.indexOf(refName) !== -1) return null;
      if (myRef[0] === refName) {
        return {
          included: this,
          excluded: Expression.TRUE
        }
      } else {
        return {
          included: Expression.TRUE,
          excluded: this
        }
      }
    }

    public breakdownByDataset(tempNamePrefix: string): DatasetBreakdown {
      var nameIndex = 0;
      var singleDatasetActions: ApplyAction[] = [];

      var remoteDatasets = this.getRemoteDatasetIds();
      if (remoteDatasets.length < 2) {
        throw new Error('not a multiple dataset expression');
      }

      var combine = this.substitute(ex => {
        var remoteDatasets = ex.getRemoteDatasetIds();
        if (remoteDatasets.length !== 1) return null;

        var existingApply = find(singleDatasetActions, (apply) => apply.expression.equals(ex));

        var tempName: string;
        if (existingApply) {
          tempName = existingApply.name;
        } else {
          tempName = tempNamePrefix + (nameIndex++);
          singleDatasetActions.push(new ApplyAction({
            action: 'apply',
            name: tempName,
            expression: ex
          }));
        }

        return new RefExpression({
          op: 'ref',
          name: tempName,
          nest: 0
        })
      });
      return {
        combineExpression: combine,
        singleDatasetActions: singleDatasetActions
      }
    }

    // ------------------------------------------------------------------------
    // API behaviour

    // Action constructors
    public _performAction(action: Action): ChainExpression {
      return new ChainExpression({
        op: 'chain',
        expression: this,
        actions: [action]
      });
    }

    /**
     * Evaluate some expression on every datum in the dataset. Record the result as `name`
     *
     * @param name The name of where to store the results
     * @param ex The expression to evaluate
     * @returns {ChainExpression}
     */
    public apply(name: string, ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new ApplyAction({ name: name, expression: ex }));
    }

    /**
     * Filter the dataset with a boolean expression
     * Only works on expressions that return DATASET
     *
     * @param ex A boolean expression to filter on
     * @returns {ChainExpression}
     */
    public filter(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new FilterAction({ expression: ex }));
    }

    /**
     *
     * @param ex
     * @param direction
     * @returns {ChainExpression}
     */
    public sort(ex: any, direction: string): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new SortAction({ expression: ex, direction: direction }));
    }

    public limit(limit: int): ChainExpression {
      return this._performAction(new LimitAction({ limit: limit }));
    }

    public not(): ChainExpression {
      return this._performAction(new NotAction({}));
    }

    public match(re: string): ChainExpression {
      return this._performAction(new MatchAction({ regexp: re }));
    }

    public numberBucket(size: number, offset: number = 0): ChainExpression {
      return this._performAction(new NumberBucketAction({ size: size, offset: offset }));
    }

    public timeBucket(duration: any, timezone: any): ChainExpression {
      if (!Duration.isDuration(duration)) duration = Duration.fromJS(duration);
      if (!Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(timezone);
      return this._performAction(new TimeBucketAction({ duration: duration, timezone: timezone }));
    }

    public timePart(part: any, timezone: any): ChainExpression {
      if (!Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(timezone);
      return this._performAction(new TimePartAction({ part: part, timezone: timezone }));
    }

    public substr(position: number, length: number): ChainExpression {
      return this._performAction(new SubstrAction({ position: position, length: length }));
    }

    public count(): ChainExpression {
      return this._performAction(new CountAction({}));
    }

    public sum(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new SumAction({ expression: ex }));
    }

    public min(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new MinAction({ expression: ex }));
    }

    public max(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new MaxAction({ expression: ex }));
    }

    public average(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new AverageAction({ expression: ex }));
    }

    public countDistinct(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new CountDistinctAction({ expression: ex }));
    }

    public quantile(ex: any, quantile: number): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new QuantileAction({ expression: ex, quantile }));
    }

    public split(ex: any, name: string, newDataName: string = null): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      var dataName = getDataName(this);
      if (!dataName && !newDataName) {
        throw new Error("could not guess data name in `split`, please provide one explicitly");
      }
      return this._performAction(new SplitAction({ expression: ex, name, dataName: newDataName || dataName }));
    }

    public is(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new IsAction({ expression: ex }));
    }

    public isnt(ex: any): ChainExpression {
      return this.is(ex).not();
    }

    public lessThan(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new LessThanAction({ expression: ex }));
    }

    public lessThanOrEqual(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new LessThanOrEqualAction({ expression: ex }));
    }

    public greaterThan(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new GreaterThanAction({ expression: ex }));
    }

    public greaterThanOrEqual(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new GreaterThanOrEqualAction({ expression: ex }));
    }

    public contains(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new ContainsAction({ expression: ex }));
    }

    public in(start: Date, end: Date): ChainExpression;
    public in(start: number, end: number): ChainExpression;
    public in(ex: any): ChainExpression;
    public in(ex: any, snd?: any): ChainExpression {
      if (arguments.length === 2) {
        if (typeof ex === 'number' && typeof snd === 'number') {
          ex = new NumberRange({ start: ex, end: snd });
        } else if (ex.toISOString && snd.toISOString) {
          ex = new TimeRange({ start: ex, end: snd });
        } else {
          throw new Error('uninterpretable IN parameters');
        }
      }
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new InAction({ expression: ex }));
    }

    public join(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this._performAction(new JoinAction({ expression: ex }));
    }

    public attach(selector: string, prop: Lookup<any>) {
      return this._performAction(new AttachAction({
        selector,
        prop
      }));
    }

    private _performMultiAction(action: string, exs: any[]) {
      if (!exs.length) throw new Error(`${action} action must have at least one argument`);
      var ret = <ChainExpression>this; // A slight type hack but it works because we know that we will go through the loop
      for (var ex of exs) {
        if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
        ret = ret._performAction(new Action.classMap[action]({ expression: ex }));
      }
      return ret;
    }

    public add(...exs: any[]): ChainExpression {
      return this._performMultiAction('add', exs);
    }

    public subtract(...exs: any[]): ChainExpression {
      return this._performMultiAction('subtract', exs);
    }

    public negate(): ChainExpression {
      return Expression.ZERO.subtract(this);
    }

    public multiply(...exs: any[]): ChainExpression {
      return this._performMultiAction('multiply', exs);
    }

    public divide(...exs: any[]): ChainExpression {
      return this._performMultiAction('divide', exs);
    }

    public reciprocate(): ChainExpression {
      return Expression.ONE.divide(this);
    }

    public and(...exs: any[]): ChainExpression {
      return this._performMultiAction('and', exs);
    }

    public or(...exs: any[]): ChainExpression {
      return this._performMultiAction('or', exs);
    }

    public concat(...exs: any[]): ChainExpression {
      return this._performMultiAction('concat', exs);
    }

    /**
     * Checks for references and returns the list of alterations that need to be made to the expression
     *
     * @param typeContext the context inherited from the parent
     * @param indexer the index along the tree to maintain
     * @param alterations the accumulation of the alterations to be made (output)
     * @returns the resolved type of the expression
     * @private
     */
    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      indexer.index++;
      return typeContext;
    }

    /**
     * Rewrites the expression with all the references typed correctly and resolved to the correct parental level
     *
     * @param context The datum within which the check is happening
     * @returns {ChainExpression}
     */
    public referenceCheck(context: Datum) {
      var datasetType: Lookup<FullType> = {};
      for (var k in context) {
        if (!hasOwnProperty(context, k)) continue;
        datasetType[k] = getFullType(context[k]);
      }
      var typeContext: FullType = {
        type: 'DATASET',
        datasetType: datasetType
      };
      
      var alterations: Alterations = {};
      this._fillRefSubstitutions(typeContext, { index: 0 }, alterations); // This return the final type
      if (!Object.keys(alterations).length) return this;
      return this.substitute((ex: Expression, index: int): Expression => alterations[index] || null);
    }

    /**
     * Resolves one level of dependencies that refer outside of this expression.
     *
     * @param context The context containing the values to resolve to
     * @param ifNotFound If the reference is not in the context what to do? "throw", "leave", "null"
     * @return The resolved expression
     */
    public resolve(context: Datum, ifNotFound: string = 'throw'): Expression {
      return this.substitute((ex: Expression, index: int, depth: int, nestDiff: int) => {
        if (ex instanceof RefExpression) {
          var nest = ex.nest;
          if (nestDiff === nest) {
            var foundValue: any = null;
            var valueFound: boolean = false;
            if (hasOwnProperty(context, ex.name)) {
              foundValue = context[ex.name];
              valueFound = true;
            } else {
              valueFound = false;
            }

            if (valueFound) {
              return new LiteralExpression({ value: foundValue });
            } else if (ifNotFound === 'throw') {
              throw new Error('could not resolve ' + ex.toString() + ' because is was not in the context');
            } else if (ifNotFound === 'null') {
              return new LiteralExpression({ value: null });
            } else if (ifNotFound === 'leave') {
              return this;
            }
          } else if (nestDiff < nest) {
            throw new Error('went too deep during resolve on: ' + ex.toString());
          }
        }
        return null;
      });
    }

    public resolved(): boolean {
      return this.every((ex: Expression) => {
        return (ex instanceof RefExpression) ? ex.nest === 0 : null; // Search within
      })
    }

    /**
     * Decompose instances of $data.average($x) into $data.sum($x) / $data.count()
     */
    public decomposeAverage(): Expression {
      return this.substitute(ex => {
        return ex.isOp('aggregate') ? ex.decomposeAverage() : null;
      })
    }

    /**
     * Apply the distributive law wherever possible to aggregates
     * Turns $data.sum($x - 2 * $y) into $data.sum($x) - 2 * $data.sum($y)
     */
    public distributeAggregates(): Expression {
      return this.substitute(ex => {
        return ex.isOp('aggregate') ? ex.distributeAggregates() : null;
      })
    }


    public _collectBindSpecs(bindSpecs: BindSpec[], selectionDepth: Lookup<number>, depth: number, applyName: string, data: string, key: string): void {
    }

    public getBindSpecs(): BindSpec[] {
      var bindSpecs: BindSpec[] = [];
      this._collectBindSpecs(bindSpecs, {}, 0, null, null, null);
      return bindSpecs;
    }

    // ---------------------------------------------------------
    // Evaluation

    public _computeResolved(): Q.Promise<any> {
      throw new Error("can not call this directly");
    }

    public simulateQueryPlan(context: Datum = {}): any[] {
      simulatedQueries = [];
      this.referenceCheck(context).getFn()(context, null);
      return simulatedQueries;
    }

    /**
     * Computes an expression synchronously if possible
     *
     * @param context The context within which to compute the expression
     * @returns {any}
     */
    public computeNative(context: Datum = {}): any {
      return this.getFn()(context, null);
    }

    /**
     * Computes a general asynchronous expression
     *
     * @param context The context within which to compute the expression
     * @param selector The selector where to attach the visualization
     * @returns {Q.Promise<any>}
     */
    public compute(context: Datum = {}, selector: string = null): Q.Promise<any> {
      if (!datumHasRemote(context) && !this.hasRemote()) {
        var referenceChecked = this.referenceCheck(context);
        var value = referenceChecked.computeNative(context);
        if (selector && value instanceof NativeDataset) {
          var selection = d3.select(selector);
          binder(selection, value, referenceChecked.getBindSpecs());
        }
        return Q(value);
      }
      var ex = this;
      return introspectDatum(context).then(introspectedContext => {
        return ex.referenceCheck(introspectedContext).resolve(introspectedContext).simplify()._computeResolved();
      });
    }
  }
  check = Expression;
}
