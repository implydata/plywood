module Plywood {
  export interface BooleanExpressionIterator {
    (ex?: Expression, index?: int, depth?: int, nestDiff?: int): boolean;
  }

  export interface VoidExpressionIterator {
    (ex?: Expression, index?: int, depth?: int, nestDiff?: int): void;
  }

  export interface SubstitutionFn {
    (ex?: Expression, index?: int, depth?: int, nestDiff?: int): Expression;
  }

  export interface ExpressionMatchFn {
    (ex?: Expression): boolean;
  }

  export interface ActionMatchFn {
    (action?: Action): boolean;
  }

  export interface ActionSubstitutionFn {
    (preEx?: Expression, action?: Action): Expression;
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
    rewrite?: string;
    expression?: Expression;
    table?: string;
    database?: string;
    rest?: string;
  }

  export interface ExpressionValue {
    op?: string;
    type?: PlyType;
    simple?: boolean;
    value?: any;
    name?: string;
    nest?: int;
    external?: External;
    expression?: Expression;
    actions?: Action[];

    remote?: boolean;
  }

  export interface ExpressionJS {
    op: string;
    type?: PlyType;
    value?: any;
    name?: string;
    nest?: int;
    external?: ExternalJS;
    expression?: ExpressionJS;
    action?: ActionJS;
    actions?: ActionJS[];
  }

  export interface ExtractAndRest {
    extract: Expression;
    rest: Expression;
  }

  export type IfNotFound = "throw" | "leave" | "null";

  export interface SubstituteActionOptions {
    onceInChain?: boolean;
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

  function getValue(param: any): any {
    if (param instanceof LiteralExpression) return param.value;
    return param;
  }

  function getString(param: string | Expression): string {
    if (typeof param === 'string') return param;
    if (param instanceof LiteralExpression && param.type === 'STRING') {
      return param.value;
    }
    if (param instanceof RefExpression && param.nest === 0) {
      return param.name;
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
  export function $(name: string, nest?: number, type?: PlyType): RefExpression;
  export function $(name: string, type?: PlyType): RefExpression;
  export function $(name: string, nest?: any, type?: PlyType): RefExpression {
    if (typeof name !== 'string') throw new TypeError('$() argument must be a string');
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

  export function toJS(thing: any): any {
    return (thing && typeof thing.toJS === 'function') ? thing.toJS() : thing;
  }

  function chainVia(op: string, expressions: Expression[], zero: Expression): Expression {
    var n = expressions.length;
    if (!n) return zero;
    var acc = expressions[0];
    if (!Expression.isExpression(acc)) acc = Expression.fromJSLoose(acc);
    for (var i = 1; i < n; i++) acc = (<any>acc)[op](expressions[i]);
    return acc;
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
    static EMPTY_SET: LiteralExpression;

    static isExpression(candidate: any): candidate is Expression {
      return isInstanceOf(candidate, Expression);
    }

    /**
     * Parses an expression
     * @param str The expression to parse
     * @param timezone The timezone within which to evaluate any untimezoned date strings
     */
    static parse(str: string, timezone?: Timezone): Expression {
      var original = defaultParserTimezone;
      if (timezone) defaultParserTimezone = timezone;
      try {
        return expressionParser.parse(str);
      } catch (e) {
        // Re-throw to add the stacktrace
        throw new Error(`Expression parse error: ${e.message} on '${str}'`);
      } finally {
        defaultParserTimezone = original;
      }
    }

    /**
     * Parses SQL statements into a plywood expressions
     * @param str The SQL to parse
     * @param timezone The timezone within which to evaluate any untimezoned date strings
     */
    static parseSQL(str: string, timezone?: Timezone): SQLParse {
      var original = defaultParserTimezone;
      if (timezone) defaultParserTimezone = timezone;
      try {
        return plyqlParser.parse(str);
      } catch (e) {
        // Re-throw to add the stacktrace
        throw new Error(`SQL parse error: ${e.message} on '${str}'`);
      } finally {
        defaultParserTimezone = original;
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
        case 'undefined':
          throw new Error('must have an expression');

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
      if (literalType === 'NUMBER_RANGE' || literalType === 'TIME_RANGE' || isSetType(literalType)) {
        returnExpression = lhs.in(literal);
      } else {
        returnExpression = lhs.is(literal);
      }
      return returnExpression.simplify();
    }
    
    static jsNullSafety(lhs: string, rhs: string, combine: (lhs: string, rhs: string) => string, lhsCantBeNull?: boolean, rhsCantBeNull?: boolean): string {
      if (lhsCantBeNull) {
        if (rhsCantBeNull) {
          return `(${combine(lhs, rhs)})`;
        } else {
          return `(_=${rhs},(_==null)?null:(${combine(lhs, '_')}))`;
        }
      } else {
        if (rhsCantBeNull) {
          return `(_=${lhs},(_==null)?null:(${combine('_', rhs)}))`;
        } else {
          return `(_1=${rhs},_2=${lhs},(_1==null||_2==null)?null:(${combine('_1', '_2')})`;
        }
      }
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

    static power(expressions: Expression[]): Expression {
      return chainVia('power', expressions, Expression.ZERO);
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
    public type: PlyType;
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
      var { type } =  this;
      if (!type) return true;
      if (wantedType === 'SET') {
        return isSetType(type);
      } else {
        return type === wantedType;
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
     * Check if the expression contains externals
     */
    public hasExternal(): boolean {
      return this.some(function(ex: Expression) {
        if (ex instanceof ExternalExpression) return true;
        if (ex instanceof RefExpression) return ex.isRemote();
        return null; // search further
      });
    }

    public getBaseExternals(): External[] {
      var externals: External[] = [];
      this.forEach(function(ex: Expression) {
        if (ex instanceof ExternalExpression) externals.push(ex.external.getBase());
      });
      return External.deduplicateExternals(externals);
    }

    public getRawExternals(): External[] {
      var externals: External[] = [];
      this.forEach(function(ex: Expression) {
        if (ex instanceof ExternalExpression) externals.push(ex.external.getRaw());
      });
      return External.deduplicateExternals(externals);
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
      return helper.deduplicateSort(freeReferences);
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

    public substituteAction(actionMatchFn: ActionMatchFn, actionSubstitutionFn: ActionSubstitutionFn, options: SubstituteActionOptions = {}, thisArg?: any): Expression {
      return this.substitute((ex: Expression) => {
        if (ex instanceof ChainExpression) {
          var actions = ex.actions;
          for (var i = 0; i < actions.length; i++) {
            var action = actions[i];
            if (actionMatchFn.call(this, action)) {
              var newEx = actionSubstitutionFn.call(this, ex.headActions(i), action);
              for (var j = i + 1; j < actions.length; j++) newEx = newEx.performAction(actions[j]);
              if (options.onceInChain) return newEx;
              return newEx.substituteAction(actionMatchFn, actionSubstitutionFn, options, this);
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
      const { type } = this;
      var jsEx = this.getJS(datumVar);
      var body: string;
      if (type === 'NUMBER' || type === 'NUMBER_RANGE') {
        body = `_=${jsEx};return isNaN(_)?null:_`;
      } else {
        body = `return ${jsEx};`;
      }
      return `function(${datumVar.replace('[]', '')}){${body}}`;
    }

    public getSQL(dialect: SQLDialect): string {
      throw new Error('should never be called directly');
    }

    public extractFromAnd(matchFn: ExpressionMatchFn): ExtractAndRest {
      if (this.type !== 'BOOLEAN') return null;
      if (matchFn(this)) {
        return {
          extract: this,
          rest: Expression.TRUE
        }
      } else {
        return {
          extract: Expression.TRUE,
          rest: this
        }
      }
    }

    public breakdownByDataset(tempNamePrefix: string): DatasetBreakdown {
      var nameIndex = 0;
      var singleDatasetActions: ApplyAction[] = [];

      var externals = this.getBaseExternals();
      if (externals.length < 2) {
        throw new Error('not a multiple dataset expression');
      }

      var combine = this.substitute(ex => {
        var externals = ex.getBaseExternals();
        if (externals.length !== 1) return null;

        var existingApply = helper.find(singleDatasetActions, (apply) => apply.expression.equals(ex));

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
     * Returns an expression containing up to `n` actions
     */
    public headActions(n: int): Expression {
      return this;
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

    public bumpStringLiteralToTime(): Expression {
      return this;
    }

    public bumpStringLiteralToSetString(): Expression {
      return this;
    }

    // ------------------------------------------------------------------------
    // API behaviour

    // Action constructors
    public performAction(action: Action, markSimple?: boolean): ChainExpression {
      return <ChainExpression>this.performActions([action], markSimple);
    }

    public performActions(actions: Action[], markSimple?: boolean): Expression {
      if (!actions.length) return this;
      return new ChainExpression({
        expression: this,
        actions: actions,
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

    public sqrt(): ChainExpression {
      return this.power(0.5);
    }

    public power(...exs: any[]): ChainExpression {
      return this._performMultiAction('power', exs);
    }

    public fallback(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new FallbackAction({ expression: ex }));
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
      return this.bumpStringLiteralToTime().performAction(new LessThanAction({ expression: ex.bumpStringLiteralToTime() }));
    }

    public lessThanOrEqual(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.bumpStringLiteralToTime().performAction(new LessThanOrEqualAction({ expression: ex.bumpStringLiteralToTime() }));
    }

    public greaterThan(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.bumpStringLiteralToTime().performAction(new GreaterThanAction({ expression: ex.bumpStringLiteralToTime() }));
    }

    public greaterThanOrEqual(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.bumpStringLiteralToTime().performAction(new GreaterThanOrEqualAction({ expression: ex.bumpStringLiteralToTime() }));
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
          ex = parseISODate(ex, defaultParserTimezone);
          if (!ex) throw new Error('can not convert start to date');
        }

        if (typeof snd === 'string') {
          snd = parseISODate(snd, defaultParserTimezone);
          if (!snd) throw new Error('can not convert end to date');
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

    public overlap(ex: any): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.bumpStringLiteralToSetString().performAction(new OverlapAction({ expression: ex.bumpStringLiteralToSetString() }));
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

    // Number manipulation

    public numberBucket(size: number, offset: number = 0): ChainExpression {
      return this.performAction(new NumberBucketAction({ size: getNumber(size), offset: getNumber(offset) }));
    }

    public absolute(): ChainExpression {
      return this.performAction(new AbsoluteAction({}));
    }

    // Time manipulation

    public timeBucket(duration: any, timezone?: any): ChainExpression {
      if (!Duration.isDuration(duration)) duration = Duration.fromJS(getString(duration));
      if (timezone && !Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(getString(timezone));
      return this.bumpStringLiteralToTime().performAction(new TimeBucketAction({ duration, timezone }));
    }

    public timeFloor(duration: any, timezone?: any): ChainExpression {
      if (!Duration.isDuration(duration)) duration = Duration.fromJS(getString(duration));
      if (timezone && !Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(getString(timezone));
      return this.bumpStringLiteralToTime().performAction(new TimeFloorAction({ duration, timezone }));
    }

    public timeShift(duration: any, step: number, timezone?: any): ChainExpression {
      if (!Duration.isDuration(duration)) duration = Duration.fromJS(getString(duration));
      if (timezone && !Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(getString(timezone));
      return this.bumpStringLiteralToTime().performAction(new TimeShiftAction({ duration, step: getNumber(step), timezone }));
    }

    public timeRange(duration: any, step: number, timezone?: any): ChainExpression {
      if (!Duration.isDuration(duration)) duration = Duration.fromJS(getString(duration));
      if (timezone && !Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(getString(timezone));
      return this.bumpStringLiteralToTime().performAction(new TimeRangeAction({ duration, step: getNumber(step), timezone }));
    }

    public timePart(part: string, timezone?: any): ChainExpression {
      if (timezone && !Timezone.isTimezone(timezone)) timezone = Timezone.fromJS(getString(timezone));
      return this.bumpStringLiteralToTime().performAction(new TimePartAction({ part: getString(part), timezone }));
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
      if (arguments.length < 2) throw new Error('invalid arguments to .apply, did you forget to specify a name?');
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new ApplyAction({ name: getString(name), expression: ex }));
    }

    public sort(ex: any, direction: string = 'ascending'): ChainExpression {
      if (!Expression.isExpression(ex)) ex = Expression.fromJSLoose(ex);
      return this.performAction(new SortAction({ expression: ex, direction: getString(direction) }));
    }

    public limit(limit: number): ChainExpression {
      return this.performAction(new LimitAction({ limit: getNumber(limit) }));
    }

    public select(...attributes: string[]): ChainExpression {
      attributes = attributes.map(getString);
      return this.performAction(new SelectAction({ attributes }));
    }


    // Aggregate expressions

    public count(): ChainExpression {
      if (arguments.length) throw new Error('.count() should not have arguments, did you want to .filter().count()?');
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

    /**
     * Rewrites the expression with all the references typed correctly and resolved to the correct parental level
     * @param environment The environment that will be defined
     */
    public defineEnvironment(environment: Environment): Expression {
      if (!environment.timezone) environment = { timezone: Timezone.UTC };

      // Allow strings as well
      if (typeof environment.timezone === 'string') environment = { timezone: Timezone.fromJS(environment.timezone as any) };

      return this.substituteAction(
        (action) => action.needsEnvironment(),
        (preEx, action) => preEx.performAction(action.defineEnvironment(environment))
      );
    }

    /**
     * Rewrites the expression with all the references typed correctly and resolved to the correct parental level
     * @param context The datum within which the check is happening
     */
    public referenceCheck(context: Datum): Expression {
      return this.referenceCheckInTypeContext(getFullTypeFromDatum(context));
    }

    /**
     * Check if the expression is defined in the given type context
     * @param typeContext The FullType context within which to resolve
     */
    public definedInTypeContext(typeContext: DatasetFullType): boolean {
      try {
        var alterations: Alterations = {};
        this._fillRefSubstitutions(typeContext, { index: 0 }, alterations); // This returns the final type
      } catch (e) {
        return false;
      }
      return true;
    }

    /**
     * Rewrites the expression with all the references typed correctly and resolved to the correct parental level
     * @param typeContext The FullType context within which to resolve
     */
    public referenceCheckInTypeContext(typeContext: DatasetFullType): Expression {
      var alterations: Alterations = {};
      this._fillRefSubstitutions(typeContext, { index: 0 }, alterations); // This returns the final type
      if (helper.emptyLookup(alterations)) return this;
      return this.substitute((ex: Expression, index: int): Expression => alterations[index] || null);
    }

    /**
     * Checks for references and returns the list of alterations that need to be made to the expression
     * @param typeContext the context inherited from the parent
     * @param indexer the index along the tree to maintain
     * @param alterations the accumulation of the alterations to be made (output)
     * @returns the resolved type of the expression
     */
    public _fillRefSubstitutions(typeContext: DatasetFullType, indexer: Indexer, alterations: Alterations): FullType {
      indexer.index++;
      return typeContext;
    }


    /**
     * Resolves one level of dependencies that refer outside of this expression.
     * @param context The context containing the values to resolve to
     * @param ifNotFound If the reference is not in the context what to do? "throw", "leave", "null"
     * @return The resolved expression
     */
    public resolve(context: Datum, ifNotFound: IfNotFound = 'throw'): Expression {
      var expressions: Lookup<Expression> = Object.create(null);
      for (var k in context) {
        if (!hasOwnProperty(context, k)) continue;
        var value = context[k];
        expressions[k] = External.isExternal(value) ?
          new ExternalExpression({ external: <External>value }) :
          new LiteralExpression({ value });
      }

      return this.resolveWithExpressions(expressions, ifNotFound);
    }

    public resolveWithExpressions(expressions: Lookup<Expression>, ifNotFound: IfNotFound = 'throw'): Expression {
      return this.substitute((ex: Expression, index: int, depth: int, nestDiff: int) => {
        if (ex instanceof RefExpression) {
          var nest = ex.nest;
          if (nestDiff === nest) {
            var foundExpression: Expression = null;
            var valueFound: boolean = false;
            if (hasOwnProperty(expressions, ex.name)) {
              foundExpression = expressions[ex.name];
              valueFound = true;
            } else {
              valueFound = false;
            }

            if (valueFound) {
              return foundExpression;
            } else if (ifNotFound === 'throw') {
              throw new Error(`could not resolve ${ex} because is was not in the context`);
            } else if (ifNotFound === 'null') {
              return Expression.NULL;
            } else if (ifNotFound === 'leave') {
              return ex;
            }
          } else if (nestDiff < nest) {
            throw new Error(`went too deep during resolve on: ${ex}`);
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

    public contained(): boolean {
      return this.every((ex: Expression, index: int, depth: int, nestDiff: int) => {
        if (ex instanceof RefExpression) {
          var nest = ex.nest;
          return nestDiff >= nest
        }
        return null;
      });
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

    /**
     * Returns the maximum number of possible values this expression can return in a split context
     */
    public maxPossibleSplitValues(): number {
      throw new Error('must be implemented by sub class');
    }

    // ---------------------------------------------------------
    // Evaluation

    private _initialPrepare(context: Datum, environment: Environment): Expression {
      return this.defineEnvironment(environment)
        .referenceCheck(context)
        .resolve(context)
        .simplify();
    }


    /**
     * Simulates computing the expression and returns the results with the right shape (but fake data)
     * @param context The context within which to compute the expression
     * @param environment The environment for the default of the expression
     */
    public simulate(context: Datum = {}, environment: Environment = {}): PlywoodValue {
      var readyExpression = this._initialPrepare(context, environment);
      if (readyExpression instanceof ExternalExpression) {
        // Top level externals need to be unsuppressed
        readyExpression = (<ExternalExpression>readyExpression).unsuppress();
      }

      return readyExpression._computeResolvedSimulate(true, []);
    }


    /**
     * Simulates computing the expression and returns the quereis that would have been made
     * @param context The context within which to compute the expression
     * @param environment The environment for the default of the expression
     */
    public simulateQueryPlan(context: Datum = {}, environment: Environment = {}): any[] {
      if (!datumHasExternal(context) && !this.hasExternal()) return [];


      var readyExpression = this._initialPrepare(context, environment);
      if (readyExpression instanceof ExternalExpression) {
        // Top level externals need to be unsuppressed
        readyExpression = (<ExternalExpression>readyExpression).unsuppress();
      }

      var simulatedQueries: any[] = [];
      readyExpression._computeResolvedSimulate(true, simulatedQueries);
      return simulatedQueries;
    }

    public _computeResolvedSimulate(lastNode: boolean, simulatedQueries: any[]): PlywoodValue {
      throw new Error("can not call this directly");
    }


    /**
     * Computes a general asynchronous expression
     * @param context The context within which to compute the expression
     * @param environment The environment for the default of the expression
     */
    public compute(context: Datum = {}, environment: Environment = {}): Q.Promise<PlywoodValue> {
      if (!datumHasExternal(context) && !this.hasExternal()) {
        return Q.fcall(() => {
          var referenceChecked = this.defineEnvironment(environment).referenceCheck(context);
          return referenceChecked.getFn()(context, null);
        });
      }
      return introspectDatum(context)
        .then(introspectedContext => {
          var readyExpression = this._initialPrepare(introspectedContext, environment);
          if (readyExpression instanceof ExternalExpression) {
            // Top level externals need to be unsuppressed
            readyExpression = (<ExternalExpression>readyExpression).unsuppress()
          }
          return readyExpression._computeResolved(true);
        });
    }

    public _computeResolved(lastNode: boolean): Q.Promise<PlywoodValue> {
      throw new Error("can not call this directly");
    }
  }
  check = Expression;
}
