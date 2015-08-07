/// <reference path="../datatypes/dataset.ts" />
/// <reference path="../expressions/baseExpression.ts" />

module Plywood {
  export interface ActionValue {
    action?: string;
    name?: string;
    dataName?: string;
    expression?: Expression;
    direction?: string;
    limit?: int;
    size?: number;
    offset?: number;
    lowerLimit?: number;
    upperLimit?: number;
    duration?: Duration;
    timezone?: Timezone;
    part?: string;
    position?: int;
    length?: int;
    regexp?: string;
    quantile?: number;
    selector?: string;
    prop?: Lookup<any>;
  }

  export interface ActionJS {
    action?: string;
    name?: string;
    dataName?: string;
    expression?: ExpressionJS;
    direction?: string;
    limit?: int;
    size?: number;
    offset?: number;
    lowerLimit?: number;
    upperLimit?: number;
    duration?: string;
    timezone?: string;
    part?: string;
    position?: int;
    length?: int;
    regexp?: string;
    quantile?: number;
    selector?: string;
    prop?: Lookup<any>;
  }

  export interface ExpressionTransformation {
    (ex: Expression): Expression;
  }

  export enum Simplification { Remove, Replace, Wipe }
  export interface ActionSimplification {
    simplification: Simplification;
    actions?: Action[];
    expression?: Expression;
  }

// =====================================================================================
// =====================================================================================

  var checkAction: ImmutableClass<ActionValue, ActionJS>;
  export class Action implements ImmutableInstance<ActionValue, ActionJS> {

    static jsToValue(parameters: ActionJS): ActionValue {
      var value: ActionValue = {
        action: parameters.action
      };
      if (parameters.expression) {
        value.expression = Expression.fromJS(parameters.expression);
      }
      return value;
    }

    /*
    static actionsDependOn(actions: Action[], name: string): boolean {
      for (let action of actions) {
        var freeReferences = action.getFreeReferences();
        if (freeReferences.indexOf(name) !== -1) return true;
        if ((<ApplyAction>action).name === name) return false;
      }
      return false;
    }
    */

    static isAction(candidate: any): boolean {
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
        throw new Error("unsupported action '" + action + "'");
      }

      return ClassFn.fromJS(actionJS);
    }

    public action: string;
    public expression: Expression;

    constructor(parameters: ActionValue, dummy: Dummy = null) {
      this.action = parameters.action;
      this.expression = parameters.expression;
      if (dummy !== dummyObject) {
        throw new TypeError("can not call `new Action` directly use Action.fromJS instead");
      }
    }

    protected _ensureAction(action: string) {
      if (!this.action) {
        this.action = action;
        return;
      }
      if (this.action !== action) {
        throw new TypeError("incorrect action '" + this.action + "' (needs to be: '" + action + "')");
      }
    }

    protected _toStringParameters(expressionString: string): string[] {
      return expressionString ? [expressionString] : [];
    }

    public toString(indent?: int): string {
      var expression = this.expression;
      var spacer = '';
      var joinStr = ', ';
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
      if (this.expression) {
        value.expression = this.expression;
      }
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

    protected _checkInputType(inputType: string, neededType: string) {
      if (inputType && inputType !== 'NULL' && neededType && inputType !== neededType) {
        throw new Error(`${this.action} must have input of type ${neededType} (is ${inputType})`);
      }
    }

    protected _checkInputTypes(inputType: string, ...neededTypes: string[]) {
      if (inputType && inputType !== 'NULL' && neededTypes.indexOf(inputType) === -1) {
        throw new Error(`${this.action} must have input of type ${neededTypes.join(' or ')} (is ${inputType})`);
      }
    }

    protected _checkExpressionType(neededType: string) {
      var expressionType = this.expression.type;
      if (expressionType && expressionType !== 'NULL' && expressionType !== neededType) {
        throw new Error(`${this.action} must have expression of type ${neededType} (is ${expressionType})`);
      }
    }

    protected _checkExpressionTypes(...neededTypes: string[]) {
      var expressionType = this.expression.type;
      if (expressionType && expressionType !== 'NULL' && neededTypes.indexOf(expressionType) === -1) {
        throw new Error(`${this.action} must have expression of type ${neededTypes.join(' or ')} (is ${expressionType})`);
      }
    }

    public getOutputType(inputType: string): string {
      throw new Error('must implement type checker');
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      var expression = this.expression;
      if (expression) expression._fillRefSubstitutions(typeContext, indexer, alterations);
      return typeContext;
    }

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
      return inputJS + '.' + this.action + '(' + (expressionJS || '') + ')';
    }

    public getJS(inputJS: string, datumVar: string): string {
      var expression = this.expression;
      var expressionJS = expression ? expression.getJS(datumVar) : null;
      return this._getJSHelper(inputJS, expressionJS);
    }


    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      throw new Error('can not call this directly');
    }

    public getSQL(dialect: SQLDialect, inputSQL: string): string {
      var expression = this.expression;
      var expressionSQL = expression ? expression.getSQL(dialect) : null;
      return this._getJSHelper(inputSQL, expressionSQL);
    }


    public expressionCount(): int {
      return this.expression ? this.expression.expressionCount() : 0;
    }


    protected _specialSimplify(simpleExpression: Expression): ActionSimplification {
      return null;
    }

    public simplify(): ActionSimplification {
      var expression = this.expression;
      if (!expression) return this._specialSimplify(null);

      var simpleExpression = expression.simplify();
      var special = this._specialSimplify(simpleExpression);
      if (special) return special;
      if (simpleExpression === expression) return null;

      var value = this.valueOf();
      value.expression = simpleExpression;
      return {
        simplification: Simplification.Replace,
        actions: [new Action.classMap[this.action](value)]
      };
    }


    protected _specialFoldLiteral(literalInput: LiteralExpression): Expression {
      return null;
    }

    public foldLiteral(literalInput: LiteralExpression): Expression {
      var special = this._specialFoldLiteral(literalInput);
      if (special) return special;

      var expression = this.expression;
      if (expression && !(expression instanceof LiteralExpression)) return null;
      return new LiteralExpression({
        value: this.getFn(literalInput.getFn())(null, null)
      });
    }


    public getFreeReferences(): string[] {
      return this.expression ? this.expression.getFreeReferences() : [];
    }

    public _everyHelper(iter: BooleanExpressionIterator, thisArg: any, indexer: Indexer, depth: int, nestDiff: int): boolean {
      return this.expression ? this.expression._everyHelper(iter, thisArg, indexer, depth, nestDiff) : true;
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
      var subExpression = expression._substituteHelper(substitutionFn, thisArg, indexer, depth, nestDiff);
      if (expression === subExpression) return this;
      var value = this.valueOf();
      value.expression = subExpression;
      return new (Action.classMap[this.action])(value);
    }

    public applyToExpression(transformation: ExpressionTransformation): Action {
      var expression = this.expression;
      if (!expression) return this;
      var newExpression = transformation(expression);
      if (newExpression === expression) return this;
      var value = this.valueOf();
      value.expression = newExpression;
      return new (Action.classMap[this.action])(value);
    }

    public contextDiff(): int {
      return 0;
    }

    public getLiteralValue(): any {
      var expression = this.expression;
      if (expression instanceof LiteralExpression) {
        return expression.value;
      }
      return null;
    }
  }
  checkAction = Action;
}
