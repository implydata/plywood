module Plywood {
  export interface BooleanExpressionIterator {
    (ex: Expression, index?: int, depth?: int, nestDiff?: int): boolean;
  }

  export interface VoidExpressionIterator {
    (ex: Expression, index?: int, depth?: int, nestDiff?: int): void;
  }

  export interface SubstitutionFn {
    (ex: Expression, index?: int, depth?: int, nestDiff?: int): Expression;
  }

  export interface ActionMatchFn {
    (action: Action): boolean;
  }

  export interface ActionSubstitutionFn {
    (preEx: Expression, action: Action): Expression;
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

  export interface SQLParse {
    verb: string;
    expression: Expression;
    table: string;
  }

  export interface ExpressionValue {
    op?: string;
    type?: string;
    simple?: boolean;
    value?: any;
    name?: string;
    nest?: int;
    external?: External;
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
    external?: ExternalJS;
    expression?: ExpressionJS;
    action?: ActionJS;
    actions?: ActionJS[];
  }

  export interface Separation {
    included: Expression;
    excluded: Expression;
  }

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

  function getValue(param: any): any {
    if (param instanceof LiteralExpression) return param.value;
    return param;
  }

  function getString(param: string | Expression): string {
    if (typeof param === 'string') return param;
    if (param instanceof LiteralExpression && param.type === 'STRING') {
      return param.value;
    }
    throw new Error('could not extract a string out of ' + String(param));
  }

  function getNumber(param: number | Expression): number {
    if (typeof param === 'number') return param;
    if (param instanceof LiteralExpression && param.type === 'NUMBER') {
      return param.value;
    }
    throw new Error('could not extract a number out of ' + String(param));
  }

  // -----------------------------

  /**
   * The expression starter function. It produces a native dataset with a singleton empty datum inside of it.
   * This is useful to describe the base container
   */
  export function ply(dataset?: Dataset): LiteralExpression {
    if (!dataset) dataset = new Dataset({ data: [{}] });
    return r(dataset);
  }

  /**
   * $('blah') produces an reference lookup expression on 'blah'
   *
   * @param name The name of the column
   * @param nest (optional) the amount of nesting to add default: 0
   * @param type (optional) force the type of the reference
   */
  export function $(name: string, nest?: number, type?: string): RefExpression;
  export function $(name: string, type?: string): RefExpression;
  export function $(name: string, nest?: any, type?: string): RefExpression {
    if (typeof name !== 'string') throw new TypeError('name must be a string');
    if (typeof nest === 'string') {
      type = nest;
      nest = 0;
    }
    return new RefExpression({
      name,
      nest: nest != null ? nest : 0,
      type
    });
  }

  export function r(value: any): LiteralExpression {
    if (External.isExternal(value)) throw new TypeError('r can not accept externals');
    if (Array.isArray(value)) value = Set.fromJS(value);
    return LiteralExpression.fromJS({ op: 'literal', value: value });
  }

  function chainVia(op: string, expressions: Expression[], zero: Expression): Expression {
    switch (expressions.length) {
      case 0: return zero;
      case 1: return expressions[0];
      default:
        var acc = expressions[0];
        for (var i = 1; i < expressions.length; i++) {
          acc = (<any>acc)[op](expressions[i]);
        }
        return acc;
    }
  }

  var check: Class<ExpressionValue, ExpressionJS>;

  /**
   * Provides a way to express arithmetic operations, aggregations and database operators.
   * This class is the backbone of plywood
   */
  export class Expression implements Instance<ExpressionValue, ExpressionJS> {
    static NULL: LiteralExpression;
    static ZERO: LiteralExpression;
    static ONE: LiteralExpression;
    static FALSE: LiteralExpression;
    static TRUE: LiteralExpression;
    static EMPTY_STRING: LiteralExpression;

    static isExpression(candidate: any): boolean {
      return isInstanceOf(candidate, Expression);
    }

    /**
     * Parses an expression
     * @param str The expression to parse
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
     * Parses SQL statements into a plywood expressions
     * @param str The SQL to parse
     */
    static parseSQL(str: string): SQLParse {
      try {
        return sqlParser.parse(str);
      } catch (e) {
        // Re-throw to add the stacktrace
        throw new Error('SQL parse error: ' + e.message + ' on `' + str + '`');
      }
    }

    /**
     * Deserializes or parses an expression
     * @param param The expression to parse
     */
    static fromJSLoose(param: any): Expression {
      var expressionJS: ExpressionJS;
      // Quick parse simple expressions
      switch (typeof param) {
        case 'object':
          if (param === null) {
            return Expression.NULL;
          } else if (Expression.isExpression(param)) {
            return param
          } else if (isImmutableClass(param)) {
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
          return Expression.parse(param);

        default:
          throw new Error("unrecognizable expression");
      }

      return Expression.fromJS(expressionJS);
    }

    static inOrIs(lhs: Expression, value: any): Expression {
      var literal = new LiteralExpression({
        op: 'literal',
        value: value
      });

      var literalType = literal.type;
      var returnExpression: Expression = null;
      if (literalType === 'NUMBER_RANGE' || literalType === 'TIME_RANGE' || literalType.indexOf('SET/') === 0) {
        returnExpression = lhs.in(literal);
      } else {
        returnExpression = lhs.is(literal);
      }
      return returnExpression.simplify();
    }

    /**
     * Composes the given expressions with an AND
     * @param expressions the array of expressions to compose
     */
    static and(expressions: Expression[]): Expression {
      return chainVia('and', expressions, Expression.TRUE);
    }

    /**
     * Composes the given expressions as E0 or E1 or ... or En
     * @param expressions the array of expressions to compose
     */
    static or(expressions: Expression[]): Expression {
      return chainVia('or', expressions, Expression.FALSE);
    }

    /**
     * Composes the given expressions as E0 + E1+ ... + En
     * @param expressions the array of expressions to compose
     */
    static add(expressions: Expression[]): Expression {
      return chainVia('add', expressions, Expression.ZERO);
    }

    /**
     * Composes the given expressions as E0 - E1- ... - En
     * @param expressions the array of expressions to compose
     */
    static subtract(expressions: Expression[]): Expression {
      return chainVia('subtract', expressions, Expression.ZERO);
    }

    static multiply(expressions: Expression[]): Expression {
      return chainVia('multiply', expressions, Expression.ONE);
    }

    static concat(expressions: Expression[]): Expression {
      return chainVia('concat', expressions, Expression.EMPTY_STRING);
    }

    static classMap: Lookup<typeof Expression> = {};
    static register(ex: typeof Expression): void {
      var op = (<any>ex).name.replace('Expression', '').replace(/^\w/, (s: string) => s.toLowerCase());
      Expression.classMap[op] = ex;
    }

    /**
     * Deserializes the expression JSON
     * @param expressionJS
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
        throw new TypeError(`incorrect expression op '${this.op}' (needs to be: '${op}')`);
      }
    }

    public valueOf(): ExpressionValue {
      var value: ExpressionValue = { op: this.op };
      if (this.simple) value.simple = true;
      return value;
    }

    /**
     * Serializes the expression into a simple JS object that can be passed to JSON.serialize
     */
    public toJS(): ExpressionJS {
      return {
        op: this.op
      };
    }

    /**
     * Makes it safe to call JSON.serialize on expressions
     */
    public toJSON(): ExpressionJS {
      return this.toJS();
    }

    public toString(indent?: int): string {
      return 'BaseExpression';
    }

    /**
     * Validate that two expressions are equal in their meaning
     * @param other
     */
    public equals(other: Expression): boolean {
      return Expression.isExpression(other) &&
        this.op === other.op &&
        this.type === other.type;
    }

    /**
     * Check that the expression can potentially have the desired type
     * If wanted type is 'SET' then any SET/* type is matched
     * @param wantedType The type that is wanted
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
     */
    public expressionCount(): int {
      return 1;
    }

    /**
     * Check if the expression is of the given operation (op)
     * @param op The operation to test
     */
    public isOp(op: string): boolean {
      return this.op === op;
    }

    /**
     * Check if the expression contains the given operation (op)
     * @param op The operation to test
     */
    public containsOp(op: string): boolean {
      return this.some((ex: Expression) => ex.isOp(op) || null);
    }

    /**
     * Check if the expression contains references to remote datasets
     */
    public hasExternal(): boolean {
      return this.some(function(ex: Expression) {
        if (ex instanceof ExternalExpression) return true;
        if (ex instanceof RefExpression) return ex.isRemote();
        return null; // search further
      });
    }

    public getExternalIds(): string[] {
      var externalIds: string[] = [];
      var push = Array.prototype.push;
      this.forEach(function(ex: Expression) {
        if (ex.type !== 'DATASET') return;
        if (ex instanceof LiteralExpression) {
          push.apply(externalIds, (<Dataset>ex.value).getExternalIds());
        } else if (ex instanceof RefExpression) {
          push.apply(externalIds, ex.remote);
        }
      });
      return deduplicateSort(externalIds);
    }

    public getExternals(): External[] {
      var externals: External[] = [];
      this.forEach(function(ex: Expression) {
        if (ex instanceof ExternalExpression) externals.push(ex.external);
      });
      return mergeExternals([externals]);
    }

    /**
     * Retrieve all free references by name
     * returns the alphabetically sorted list of the references
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
     * Returns an expression that is equivalent but no more complex
     * If no simplification can be done will return itself.
     */
    public simplify(): Expression {
      return this;
    }

    /**
     * Runs iter over all the sub expression and return true if iter returns true for everything
     * @param iter The function to run
     * @param thisArg The this for the substitution function
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
     * @param iter The function to run
     * @param thisArg The this for the substitution function
     */
    public some(iter: BooleanExpressionIterator, thisArg?: any): boolean {
      return !this.every((ex: Expression, index: int, depth: int, nestDiff: int) => {
        var v = iter.call(this, ex, index, depth, nestDiff);
        return (v == null) ? null : !v;
      }, thisArg);
    }

    /**
     * Runs iter over all the sub expressions
     * @param iter The function to run
     * @param thisArg The this for the substitution function
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


    public substituteAction(actionMatchFn: ActionMatchFn, actionSubstitutionFn: ActionSubstitutionFn, thisArg?: any): Expression {
      return this.substitute((ex: Expression) => {
        if (ex instanceof ChainExpression) {
          var actions = ex.actions;
          for (var i = 0; i < actions.length; i++) {
            var action = actions[i];
            if (actionMatchFn.call(this, action)) {
              var preEx = ex.expression;
              if (i) {
                preEx = new ChainExpression({
                  expression: preEx,
                  actions: actions.slice(0, i)
                })
              }
              var newEx = actionSubstitutionFn.call(this, preEx, action);
              for (var j = i + 1; j < actions.length; j++) newEx = newEx.performAction(actions[j]);
              return newEx.substituteAction(actionMatchFn, actionSubstitutionFn, this);
            }
          }
        }
        return null;
      }, thisArg);
    }

    public getFn(): ComputeFn {
      throw new Error('should never be called directly');
    }

    public getJS(datumVar: string): string {
      throw new Error('should never be called directly');
    }

    public getJSFn(datumVar: string = 'd[]'): string {
      return `function(${datumVar.replace('[]', '')}){return ${this.getJS(datumVar)};}`;
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

      var externals = this.getExternalIds();
      if (externals.length < 2) {
        throw new Error('not a multiple dataset expression');
      }

      var combine = this.substitute(ex => {
        var externals = ex.getExternalIds();
        if (externals.length !== 1) return null;

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

    public actionize(containingAction: string): Action[] {
      return null;
    }

    public getExpressionPattern(actionType: string): Expression[] {
      var actions = this.actionize(actionType);
      return actions ? actions.map((action) => action.expression) : null;
    }

    /**
     * Returns the first action
     * Returns null there are no actions
     */
    public firstAction(): Action {
      return null;
    }

    /**
     * Returns the last action
     * Returns null there are no actions
     */
    public lastAction(): Action {
      return null;
    }

    /**
     * Returns an expression without the last action.
     * Returns null if an action can not be poped
     */
    public popAction(): Expression {
      return null;
    }

    public getLiteralValue(): any {
      return null;
    }

    // ------------------------------------------------------------------------
    // API behaviour

    // Action constructors
    public performAction(action: Action, markSimple?: boolean): ChainExpression {
      if (!action) throw new Error('must have action');
      return new ChainExpression({
        op: 'chain',
        expression: this,
        actions: [action],
        simple: Boolean(markSimple)
      });
    }

    private _performMultiAction(action: string, exs: any[]): ChainExpression {
      if (!exs.length) throw new Error(`${action} action must have at least one argument`);
      var ret: any = this; // A slight type hack but it works because we know that we will go through the loop
      for (var ex of exs) {
        if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
        ret = ret.performAction(new Action.classMap[action]({ expression: ex }));
      }
      return ret;
    }

    // Basic arithmetic

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

    // Boolean predicates

    public is(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new IsAction({ expression: ex }));
    }

    public isnt(ex: any): ChainExpression {
      return this.is(ex).not();
    }

    public lessThan(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new LessThanAction({ expression: ex }));
    }

    public lessThanOrEqual(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new LessThanOrEqualAction({ expression: ex }));
    }

    public greaterThan(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new GreaterThanAction({ expression: ex }));
    }

    public greaterThanOrEqual(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new GreaterThanOrEqualAction({ expression: ex }));
    }

    public contains(ex: any, compare?: string): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      if (compare) compare = getString(compare);
      return this.performAction(new ContainsAction({ expression: ex, compare }));
    }

    public match(re: string): ChainExpression {
      return this.performAction(new MatchAction({ regexp: getString(re) }));
    }

    public in(start: Date, end: Date): ChainExpression;
    public in(start: number, end: number): ChainExpression;
    public in(start: string, end: string): ChainExpression;
    public in(ex: any): ChainExpression;
    public in(ex: any, snd?: any): ChainExpression {
      if (arguments.length === 2) {
        ex = getValue(ex);
        snd = getValue(snd);

        if (typeof ex === 'string') {
          ex = new Date(ex);
          if (isNaN(ex.valueOf())) throw new Error('can not convert start to date');
        }

        if (typeof snd === 'string') {
          snd = new Date(snd);
          if (isNaN(snd.valueOf())) throw new Error('can not convert end to date');
        }

        if (typeof ex === 'number' && typeof snd === 'number') {
          ex = new NumberRange({ start: ex, end: snd });
        } else if (ex.toISOString && snd.toISOString) {
          ex = new TimeRange({ start: ex, end: snd });
        } else {
          throw new Error('uninterpretable IN parameters');
        }
      }
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new InAction({ expression: ex }));
    }

    public not(): ChainExpression {
      return this.performAction(new NotAction({}));
    }

    public and(...exs: any[]): ChainExpression {
      return this._performMultiAction('and', exs);
    }

    public or(...exs: any[]): ChainExpression {
      return this._performMultiAction('or', exs);
    }

    // String manipulation

    public substr(position: number, length: number): ChainExpression {
      return this.performAction(new SubstrAction({ position: getNumber(position), length: getNumber(length) }));
    }

    public extract(re: string): ChainExpression {
      return this.performAction(new ExtractAction({ regexp: getString(re) }));
    }

    public concat(...exs: any[]): ChainExpression {
      return this._performMultiAction('concat', exs);
    }

    public lookup(lookup: string): ChainExpression {
      return this.performAction(new LookupAction({ lookup: getString(lookup) }));
    }

    // Bucketing

    public numberBucket(size: number, offset: number = 0): ChainExpression {
      return this.performAction(new NumberBucketAction({ size: getNumber(size), offset: getNumber(offset) }));
    }

    public timeBucket(duration: any, timezone: any = Timezone.UTC): ChainExpression {
      if (!Duration.isDuration(duration)) duration = Duration.fromJS(getString(duration));
      if (!Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(getString(timezone));
      return this.performAction(new TimeBucketAction({ duration: duration, timezone: timezone }));
    }

    public timePart(part: string, timezone: any): ChainExpression {
      if (!Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(getString(timezone));
      return this.performAction(new TimePartAction({ part: getString(part), timezone: timezone }));
    }

    public timeOffset(duration: any, timezone: any): ChainExpression {
      if (!Duration.isDuration(duration)) duration = Duration.fromJS(getString(duration));
      if (!Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(getString(timezone));
      return this.performAction(new TimeOffsetAction({ duration: duration, timezone: timezone }));
    }

    // Split Apply Combine based transformations

    /**
     * Filter the dataset with a boolean expression
     * Only works on expressions that return DATASET
     * @param ex A boolean expression to filter on
     */
    public filter(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new FilterAction({ expression: ex }));
    }

    public split(splits: any, dataName?: string): ChainExpression;
    public split(ex: any, name: string, dataName?: string): ChainExpression;
    public split(splits: any, name?: string, dataName?: string): ChainExpression {
      // Determine if use case #2
      if (arguments.length === 3 ||
        (arguments.length === 2 && splits && (typeof splits === 'string' || typeof splits.op === 'string'))) {
        name = getString(name);
        var realSplits = Object.create(null);
        realSplits[name] = splits;
        splits = realSplits;
      } else {
        dataName = name;
      }

      var parsedSplits: Splits = Object.create(null);
      for (var k in splits) {
        if (!hasOwnProperty(splits, k)) continue;
        var ex = splits[k];
        parsedSplits[k] = Expression.isExpression(ex) ? ex : Expression.fromJSLoose(ex);
      }

      dataName = dataName ? getString(dataName) : getDataName(this);
      if (!dataName) throw new Error("could not guess data name in `split`, please provide one explicitly");
      return this.performAction(new SplitAction({ splits: parsedSplits, dataName: dataName }));
    }

    /**
     * Evaluate some expression on every datum in the dataset. Record the result as `name`
     * @param name The name of where to store the results
     * @param ex The expression to evaluate
     */
    public apply(name: string, ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new ApplyAction({ name: getString(name), expression: ex }));
    }

    public sort(ex: any, direction: string): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new SortAction({ expression: ex, direction: getString(direction) }));
    }

    public limit(limit: number): ChainExpression {
      return this.performAction(new LimitAction({ limit: getNumber(limit) }));
    }

    public fallback(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new FallbackAction({ expression: ex }));
    }

    // Aggregate expressions

    public count(): ChainExpression {
      return this.performAction(new CountAction({}));
    }

    public sum(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new SumAction({ expression: ex }));
    }

    public min(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new MinAction({ expression: ex }));
    }

    public max(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new MaxAction({ expression: ex }));
    }

    public average(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new AverageAction({ expression: ex }));
    }

    public countDistinct(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new CountDistinctAction({ expression: ex }));
    }

    public quantile(ex: any, quantile: number): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new QuantileAction({ expression: ex, quantile: getNumber(quantile) }));
    }

    public custom(custom: string): ChainExpression {
      return this.performAction(new CustomAction({ custom: getString(custom) }));
    }

    // Undocumented (for now)

    public join(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new JoinAction({ expression: ex }));
    }

    public attach(selector: string, prop: Lookup<any>) {
      return this.performAction(new AttachAction({
        selector,
        prop
      }));
    }

    /**
     * Checks for references and returns the list of alterations that need to be made to the expression
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
     * @param context The datum within which the check is happening
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

    /**!
     * Resolves one level of dependencies that refer outside of this expression.
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
              return External.isExternal(foundValue) ?
                new ExternalExpression({ external: foundValue }) :
                new LiteralExpression({ value: foundValue });
            } else if (ifNotFound === 'throw') {
              throw new Error('could not resolve ' + ex.toString() + ' because is was not in the context');
            } else if (ifNotFound === 'null') {
              return new LiteralExpression({ value: null });
            } else if (ifNotFound === 'leave') {
              return ex;
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
     * @param countEx and optional expression to use in a sum instead of a count
     */
    public decomposeAverage(countEx?: Expression): Expression {
      return this.substituteAction(
        (action) => {
          return action.action === 'average';
        },
        (preEx: Expression, action: Action) => {
          var expression = action.expression;
          return preEx.sum(expression).divide(countEx ? preEx.sum(countEx) : preEx.count())
        }
      );
    }

    /**
     * Apply the distributive law wherever possible to aggregates
     * Turns $data.sum($x - 2 * $y) into $data.sum($x) - 2 * $data.sum($y)
     */
    public distribute(): Expression {
      return this.substituteAction(
        (action) => {
          return action.canDistribute();
        },
        (preEx: Expression, action: Action) => {
          var distributed = action.distribute(preEx);
          if (!distributed) throw new Error('distribute returned null');
          return distributed;
        }
      );
    }

    // ---------------------------------------------------------
    // Evaluation

    public _computeResolvedSimulate(simulatedQueries: any[]): any {
      throw new Error("can not call this directly");
    }

    public simulateQueryPlan(context: Datum = {}): any[] {
      if (!datumHasExternal(context) && !this.hasExternal()) {
        return [];
      }

      var simulatedQueries: any[] = [];
      var readyExpression = this.referenceCheck(context).resolve(context).simplify();
      if (readyExpression instanceof ExternalExpression) {
        // Top level externals need to be unsuppressed
        readyExpression = (<ExternalExpression>readyExpression).unsuppress()
      }
      readyExpression._computeResolvedSimulate(simulatedQueries);
      return simulatedQueries;
    }

    public _computeResolved(): Q.Promise<any> {
      throw new Error("can not call this directly");
    }

    /**
     * Computes a general asynchronous expression
     * @param context The context within which to compute the expression
     */
    public compute(context: Datum = {}): Q.Promise<any> {
      if (!datumHasExternal(context) && !this.hasExternal()) {
        return Q.fcall(() => {
          var referenceChecked = this.referenceCheck(context);
          return referenceChecked.getFn()(context, null);
        });
      }
      var ex = this;
      return introspectDatum(context).then(introspectedContext => {
        var readyExpression = ex.referenceCheck(introspectedContext).resolve(introspectedContext).simplify();
        if (readyExpression instanceof ExternalExpression) {
          // Top level externals need to be unsuppressed
          readyExpression = (<ExternalExpression>readyExpression).unsuppress()
        }
        return readyExpression._computeResolved();
      });
    }
  }
  check = Expression;
}
