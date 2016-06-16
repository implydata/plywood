/// <reference path="../datatypes/dataset.ts" />
/// <reference path="../expressions/baseExpression.ts" />

module Plywood {
  export interface Splits {
    [name: string]: Expression;
  }

  export interface SplitsJS {
    [name: string]: ExpressionJS;
  }

  export interface ActionValue {
    action?: string;
    name?: string;
    dataName?: string;
    expression?: Expression;
    splits?: Splits;
    direction?: string;
    limit?: int;
    size?: number;
    offset?: number;
    duration?: Duration;
    timezone?: Timezone;
    part?: string;
    step?: number;
    position?: int;
    length?: int;
    regexp?: string;
    quantile?: number;
    selector?: string;
    prop?: Lookup<any>;
    custom?: string;
    compare?: string;
    lookup?: string;
    attributes?: string[];
    simple?: boolean;
  }

  export interface ActionJS {
    action?: string;
    name?: string;
    dataName?: string;
    expression?: ExpressionJS;
    splits?: SplitsJS;
    direction?: string;
    limit?: int;
    size?: number;
    offset?: number;
    duration?: string;
    timezone?: string;
    part?: string;
    step?: number;
    position?: int;
    length?: int;
    regexp?: string;
    quantile?: number;
    selector?: string;
    prop?: Lookup<any>;
    custom?: string;
    compare?: string;
    lookup?: string;
    attributes?: string[];
  }

  export interface Environment {
    timezone?: Timezone;
  }

// =====================================================================================
// =====================================================================================

  export abstract class Action implements Instance<ActionValue, ActionJS> {

    static jsToValue(parameters: ActionJS): ActionValue {
      var value: ActionValue = {
        action: parameters.action
      };
      if (parameters.expression) {
        value.expression = Expression.fromJS(parameters.expression);
      }
      return value;
    }

    static actionsDependOn(actions: Action[], name: string): boolean {
      for (let action of actions) {
        var freeReferences = action.getFreeReferences();
        if (freeReferences.indexOf(name) !== -1) return true;
        if ((<ApplyAction>action).name === name) return false;
      }
      return false;
    }

    static isAction(candidate: any): candidate is Action {
      return isInstanceOf(candidate, Action);
    }

    static classMap: Lookup<typeof Action> = {};

    static register(act: typeof Action): void {
      var action = (<any>act).name.replace('Action', '').replace(/^\w/, (s: string) => s.toLowerCase());
      Action.classMap[action] = act;
    }

    static fromJS(actionJS: ActionJS): Action {
      if (!hasOwnProperty(actionJS, "action")) {
        throw new Error("action must be defined");
      }
      var action = actionJS.action;
      if (typeof action !== "string") {
        throw new Error("action must be a string");
      }
      var ClassFn = Action.classMap[action];
      if (!ClassFn) {
        throw new Error(`unsupported action '${action}'`);
      }

      return ClassFn.fromJS(actionJS);
    }

    static fromValue(value: ActionValue): Action {
      var ClassFn = Action.classMap[value.action] as any;
      return new ClassFn(value);
    }

    public action: string;
    public expression: Expression;
    public simple: boolean;

    constructor(parameters: ActionValue, dummy: Dummy = null) {
      if (dummy !== dummyObject) {
        throw new TypeError("can not call `new Action` directly use Action.fromJS instead");
      }
      this.action = parameters.action;
      this.expression = parameters.expression;
      this.simple = parameters.simple;
    }

    protected _ensureAction(action: string) {
      if (!this.action) {
        this.action = action;
        return;
      }
      if (this.action !== action) {
        throw new TypeError(`incorrect action '${this.action}' (needs to be: '${action}')`);
      }
    }

    protected _toStringParameters(expressionString: string): string[] {
      return expressionString ? [expressionString] : [];
    }

    public toString(indent?: int): string {
      var expression = this.expression;
      var spacer = '';
      var joinStr = indent != null ? ', ' : ',';
      var nextIndent: int = null;
      if (indent != null && expression && expression.type === 'DATASET') {
        var space = repeat(' ', indent);
        spacer = '\n' + space;
        joinStr = ',\n' + space;
        nextIndent = indent + 2;
      }
      return [
        this.action,
        '(',
        spacer,
        this._toStringParameters(expression ? expression.toString(nextIndent) : null).join(joinStr),
        spacer,
        ')'
      ].join('');
    }

    public valueOf(): ActionValue {
      var value: ActionValue = {
        action: this.action
      };
      if (this.expression) value.expression = this.expression;
      if (this.simple) value.simple = true;
      return value;
    }

    public toJS(): ActionJS {
      var js: ActionJS = {
        action: this.action
      };
      if (this.expression) {
        js.expression = this.expression.toJS();
      }
      return js;
    }

    public toJSON(): ActionJS {
      return this.toJS();
    }

    public equals(other: Action): boolean {
      return Action.isAction(other) &&
        this.action === other.action &&
        Boolean(this.expression) === Boolean(other.expression) &&
        (!this.expression || this.expression.equals(other.expression))
    }

    public isAggregate(): boolean {
      return false;
    }

    protected _checkInputTypes(inputType: string, ...neededTypes: string[]) {
      if (inputType && inputType !== 'NULL' && neededTypes.indexOf(inputType) === -1) {
        if (neededTypes.length === 1) {
          throw new Error(`${this.action} must have input of type ${neededTypes[0]} (is ${inputType})`);
        } else {
          throw new Error(`${this.action} must have input of type ${neededTypes.join(' or ')} (is ${inputType})`);
        }
      }
    }

    protected _checkNoExpression() {
      if (this.expression) {
        throw new Error(`${this.action} must no have an expression (is ${this.expression})`);
      }
    }

    protected _checkExpressionTypes(...neededTypes: string[]) {
      var expressionType = this.expression.type;
      if (expressionType && expressionType !== 'NULL' && neededTypes.indexOf(expressionType) === -1) {
        if (neededTypes.length === 1) {
          throw new Error(`${this.action} must have expression of type ${neededTypes[0]} (is ${expressionType})`);
        } else {
          throw new Error(`${this.action} must have expression of type ${neededTypes.join(' or ')} (is ${expressionType})`);
        }
      }
    }

    public abstract getOutputType(inputType: PlyType): PlyType

    protected _stringTransformOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'STRING', 'SET/STRING');
      return inputType;
    }

    public abstract _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType, indexer: Indexer, alterations: Alterations): FullType

    protected _getFnHelper(inputFn: ComputeFn, expressionFn: ComputeFn): ComputeFn {
      var action = this.action;
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        return inV ? inV[action](expressionFn, foldContext(d, c)) : null;
      }
    }

    public getFn(inputFn: ComputeFn): ComputeFn {
      var expression = this.expression;
      var expressionFn = expression ? expression.getFn() : null;
      return this._getFnHelper(inputFn, expressionFn);
    }


    protected _getJSHelper(inputJS: string, expressionJS: string): string {
      throw new Error('can not call this directly');
    }

    public getJS(inputJS: string, datumVar: string): string {
      var expression = this.expression;
      var expressionJS = expression ? expression.getJS(datumVar) : null;
      return this._getJSHelper(inputJS, expressionJS);
    }


    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      throw new Error('can not call this directly');
    }

    public getSQL(inputSQL: string, dialect: SQLDialect): string {
      var expression = this.expression;
      var expressionSQL = expression ? expression.getSQL(dialect) : null;
      return this._getSQLHelper(dialect, inputSQL, expressionSQL);
    }


    public expressionCount(): int {
      return this.expression ? this.expression.expressionCount() : 0;
    }

    public fullyDefined(): boolean {
      var { expression } = this;
      return !expression || expression.isOp('literal');
    }

    // Simplification
    protected _specialSimplify(simpleExpression: Expression): Action {
      return null;
    }

    public simplify(): Action {
      if (this.simple) return this;
      var expression = this.expression;
      var simpleExpression = expression ? expression.simplify() : null;

      var special = this._specialSimplify(simpleExpression);
      if (special) return special;

      var value = this.valueOf();
      if (simpleExpression) {
        value.expression = simpleExpression;
      }
      value.simple = true;
      return Action.fromValue(value);
    }

    /**
     * Remove action if possible
     * For example +0 and *1
     */
    protected _removeAction(): boolean {
      return false;
    }

    /**
     * Wipe out all if possible
     * For example *0
     */
    protected _nukeExpression(precedingExpression: Expression): Expression {
      return null;
    }

    /**
     * Distribute this action over the inner expression if needed
     * For example +(x +y +z) => +x +y +z
     */
    protected _distributeAction(): Action[] {
      return null;
    }

    /**
     * Special logic to perform this action on a literal
     * @param literalExpression the expression on which to perform
     */
    protected _performOnLiteral(literalExpression: LiteralExpression): Expression {
      return null;
    }

    /**
     * Special logic to perform this action on a reference
     * @param refExpression the expression on which to perform
     */
    protected _performOnRef(refExpression: RefExpression): Expression {
      return null;
    }

    /**
     * Special logic to combine the previous action and this action together.
     * @param prevAction the previous action
     */
    protected _foldWithPrevAction(prevAction: Action): Action {
      return null;
    }

    /**
     * Special logic to move this action before the previous one.
     * @param lastAction the previous action
     */
    protected _putBeforeLastAction(lastAction: Action): Action {
      return null;
    }

    /**
     * Special logic to perform this action on a chain
     * @param chainExpression the expression on which to perform
     */
    protected _performOnSimpleChain(chainExpression: ChainExpression): Expression {
      return null;
    }

    public performOnSimple(simpleExpression: Expression): Expression {
      if (!this.simple) return this.simplify().performOnSimple(simpleExpression);
      if (!simpleExpression.simple) throw new Error('must get a simple expression');

      if (this._removeAction()) return simpleExpression;

      var nukedExpression = this._nukeExpression(simpleExpression);
      if (nukedExpression) return nukedExpression;

      var distributedActions = this._distributeAction();
      if (distributedActions) {
        for (var distributedAction of distributedActions) {
          simpleExpression = distributedAction.performOnSimple(simpleExpression);
        }
        return simpleExpression;
      }

      if (simpleExpression instanceof LiteralExpression) {
        if (this.fullyDefined()) {
          return new LiteralExpression({
            value: this.getFn(simpleExpression.getFn())(null, null)
          });
        }

        var special = this._performOnLiteral(simpleExpression);
        if (special) return special;

      } else if (simpleExpression instanceof RefExpression) {
        var special = this._performOnRef(simpleExpression);
        if (special) return special;

      } else if (simpleExpression instanceof ChainExpression) {
        var actions = simpleExpression.actions;
        var lastAction = actions[actions.length - 1];

        var foldedAction = this._foldWithPrevAction(lastAction);
        if (foldedAction) {
          return foldedAction.performOnSimple(simpleExpression.popAction());
        }

        var beforeAction = this._putBeforeLastAction(lastAction);
        if (beforeAction) {
          return lastAction.performOnSimple(beforeAction.performOnSimple(simpleExpression.popAction()));
        }

        var special = this._performOnSimpleChain(simpleExpression);
        if (special) return special;

      }

      return simpleExpression.performAction(this, true);
    }

    public getExpressions(): Expression[] {
      return this.expression ? [this.expression] : [];
    }

    public getFreeReferences(): string[] {
      var freeReferences: string[] = [];
      this.getExpressions().forEach((ex) => {
        freeReferences = freeReferences.concat(ex.getFreeReferences());
      });
      return helper.deduplicateSort(freeReferences);
    }

    public _everyHelper(iter: BooleanExpressionIterator, thisArg: any, indexer: Indexer, depth: int, nestDiff: int): boolean {
      var nestDiffNext = nestDiff + Number(this.isNester());
      return this.getExpressions().every((ex) => ex._everyHelper(iter, thisArg, indexer, depth, nestDiffNext));
    }

    /**
     * Performs a substitution by recursively applying the given substitutionFn to the expression
     * if substitutionFn returns an expression than it is replaced and a new actions is returned;
     * if null is returned this action will return
     *
     * @param substitutionFn The function with which to substitute
     * @param thisArg The this for the substitution function
     */
    public substitute(substitutionFn: SubstitutionFn, thisArg?: any): Action {
      return this._substituteHelper(substitutionFn, thisArg, { index: 0 }, 0, 0);
    }

    public _substituteHelper(substitutionFn: SubstitutionFn, thisArg: any, indexer: Indexer, depth: int, nestDiff: int): Action {
      var expression = this.expression;
      if (!expression) return this;
      var subExpression = expression._substituteHelper(substitutionFn, thisArg, indexer, depth, nestDiff + Number(this.isNester()));
      if (expression === subExpression) return this;
      var value = this.valueOf();
      value.simple = false;
      value.expression = subExpression;
      return Action.fromValue(value);
    }

    public canDistribute(): boolean {
      return false;
    }

    public distribute(preEx: Expression): Expression {
      return null;
    }

    public changeExpression(newExpression: Expression): Action {
      var expression = this.expression;
      if (!expression || expression === newExpression) return this;
      var value = this.valueOf();
      value.expression = newExpression;
      return Action.fromValue(value);
    }

    public isNester(): boolean {
      return false;
    }

    public getLiteralValue(): any {
      var expression = this.expression;
      if (expression instanceof LiteralExpression) {
        return expression.value;
      }
      return null;
    }

    public maxPossibleSplitValues(): number {
      return Infinity;
    }

    // Environment methods

    public needsEnvironment(): boolean {
      return false;
    }

    public defineEnvironment(environment: Environment): Action {
      return this;
    }

    public getTimezone(): Timezone {
      return Timezone.UTC;
    }

    public alignsWith(actions: Action[]): boolean {
      return true;
    }
  }

}
