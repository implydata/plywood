module Plywood {
  export class ChainExpression extends Expression {
    static fromJS(parameters: ExpressionJS): ChainExpression {
      var value: ExpressionValue = {
        op: parameters.op
      };
      value.expression = Expression.fromJS(parameters.expression);
      if (hasOwnProperty(parameters, 'action')) {
        value.actions = [Action.fromJS(parameters.action)];
      } else {
        if (!Array.isArray(parameters.actions)) throw new Error('chain `actions` must be an array');
        value.actions = parameters.actions.map(Action.fromJS);
      }

      return new ChainExpression(value);
    }

    public expression: Expression;
    public actions: Action[];

    constructor(parameters: ExpressionValue) {
      super(parameters, dummyObject);
      var expression = parameters.expression;
      this.expression = expression;
      var actions = parameters.actions;
      if (!actions.length) throw new Error('can not have empty actions');
      this.actions = actions;
      this._ensureOp('chain');

      var type = expression.type;
      for (var action of actions) {
        type = action.getOutputType(type);
      }
      this.type = type;
    }

    public valueOf(): ExpressionValue {
      var value = super.valueOf();
      value.expression = this.expression;
      value.actions = this.actions;
      return value;
    }

    public toJS(): ExpressionJS {
      var js = super.toJS();
      js.expression = this.expression.toJS();

      var { actions } = this;
      if (actions.length === 1) {
        js.action = actions[0].toJS();
      } else {
        js.actions = actions.map(action => action.toJS());
      }
      return js;
    }

    public toString(indent?: int): string {
      var expression = this.expression;
      var actions = this.actions;
      var joinStr = '.';
      var nextIndent: int = null;
      if (indent != null && (actions.length > 1 || expression.type === 'DATASET')) {
        joinStr = '\n' + repeat(' ', indent) + joinStr;
        nextIndent = indent + 2;
      }
      return [expression.toString()]
        .concat(actions.map(action => action.toString(nextIndent)))
        .join(joinStr);
    }

    public equals(other: ChainExpression): boolean {
      return super.equals(other) &&
             this.expression.equals(other.expression) &&
             higherArraysEqual(this.actions, other.actions);
    }

    public expressionCount(): int {
      var expressionCount = 1 + this.expression.expressionCount();
      var actions = this.actions;
      for (let action of actions) {
        expressionCount += action.expressionCount();
      }
      return expressionCount;
    }

    public getFn(): ComputeFn {
      var fn = this.expression.getFn();
      var actions = this.actions;
      for (let action of actions) {
        fn = action.getFn(fn);
      }
      return fn;
    }

    public getJS(datumVar: string): string {
      var expression = this.expression;
      var actions = this.actions;
      var js = expression.getJS(datumVar);
      for (let action of actions) {
        js = action.getJS(js, datumVar);
      }
      return js;
    }

    public getSQL(dialect: SQLDialect): string {
      var expression = this.expression;
      var actions = this.actions;
      var sql = expression.getSQL(dialect);
      for (let action of actions) {
        sql = action.getSQL(sql, dialect);
      }
      return sql;
    }

    /**
     * Returns the single action of the chain, if there are multiple actions null is returned
     * @param neededAction and optional type can be passed in to retrun only an action of this type
     * @returns Action
     */
    public getSingleAction(neededAction?: string): Action {
      var actions = this.actions;
      if (actions.length !== 1) return null;
      var singleAction = actions[0];
      if (neededAction && singleAction.action !== neededAction) return null;
      return singleAction;
    }

    public foldIntoExternal(): Expression {
      var externalExpression = this.expression;
      var actions = this.actions;

      if (externalExpression instanceof ExternalExpression) {
        var undigestedActions: Action[] = [];
        for (var action of actions) {
          var newExternal = (<ExternalExpression>externalExpression).addAction(action);
          if (newExternal) {
            externalExpression = newExternal;
          } else {
            undigestedActions.push(action);
          }
        }

        var emptyLiteral = (<ExternalExpression>externalExpression).getEmptyLiteral();
        if (emptyLiteral) {
          externalExpression = emptyLiteral;
        }

        if (undigestedActions.length) {
          return new ChainExpression({
            expression: externalExpression,
            actions: undigestedActions,
            simple: true
          });
        } else {
          return externalExpression;
        }
      }
      return this;
    }

    public simplify(): Expression {
      if (this.simple) return this;

      var simpleExpression = this.expression.simplify();
      var actions = this.actions;
      if (simpleExpression instanceof ChainExpression) {
        return new ChainExpression({
          expression: simpleExpression.expression,
          actions: simpleExpression.actions.concat(actions)
        }).simplify();
      }

      for (let action of actions) {
        simpleExpression = action.performOnSimple(simpleExpression);
      }

      if (simpleExpression instanceof ChainExpression) {
        return simpleExpression.foldIntoExternal();
      } else {
        return simpleExpression;
      }
    }

    public _everyHelper(iter: BooleanExpressionIterator, thisArg: any, indexer: Indexer, depth: int, nestDiff: int): boolean {
      var pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
      if (pass != null) {
        return pass;
      } else {
        indexer.index++;
      }
      depth++;

      var expression = this.expression;
      if (!expression._everyHelper(iter, thisArg, indexer, depth, nestDiff)) return false;

      var actions = this.actions;
      var every: boolean = true;
      for (let action of actions) {
        if (every) {
          every = action._everyHelper(iter, thisArg, indexer, depth, nestDiff);
        } else {
          indexer.index += action.expressionCount();
        }
      }
      return every;
    }

    public _substituteHelper(substitutionFn: SubstitutionFn, thisArg: any, indexer: Indexer, depth: int, nestDiff: int): Expression {
      var sub = substitutionFn.call(thisArg, this, indexer.index, depth, nestDiff);
      if (sub) {
        indexer.index += this.expressionCount();
        return sub;
      } else {
        indexer.index++;
      }
      depth++;

      var expression = this.expression;
      var subExpression = expression._substituteHelper(substitutionFn, thisArg, indexer, depth, nestDiff);

      var actions = this.actions;
      var subActions = actions.map(action => {
        var subbedAction = action._substituteHelper(substitutionFn, thisArg, indexer, depth, nestDiff);
        return subbedAction;
      });
      if (expression === subExpression && arraysEqual(actions, subActions)) return this;

      var value = this.valueOf();
      value.expression = subExpression;
      value.actions = subActions;
      delete value.simple;
      return new ChainExpression(value);
    }

    public performAction(action: Action, markSimple?: boolean): ChainExpression {
      if (!action) throw new Error('must have action');
      return new ChainExpression({
        expression: this.expression,
        actions: this.actions.concat(action),
        simple: Boolean(markSimple)
      });
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      indexer.index++;

      // Some explanation of what is going on here is in order as this is the heart of the variable resolution code
      // The _fillRefSubstitutions function is chained across all the expressions.
      // If an expression returns a DATASET type it is treated as the new context otherwise the original context is
      // used for the next expression (currentContext)
      var currentContext = typeContext;
      var outputContext = this.expression._fillRefSubstitutions(currentContext, indexer, alterations);
      currentContext = outputContext.type === 'DATASET' ? outputContext : typeContext;

      var actions = this.actions;
      for (let action of actions) {
        outputContext = action._fillRefSubstitutions(currentContext, indexer, alterations);
        currentContext = outputContext.type === 'DATASET' ? outputContext : typeContext;
      }

      return outputContext;
    }

    public actionize(containingAction: string): Action[] {
      var actions = this.actions;

      var k = actions.length - 1;
      for (; k >= 0; k--) {
        if (actions[k].action !== containingAction) break;
      }
      k++; // k now represents the number of actions that remain in the chain
      if (k === actions.length) return null; // nothing to do

      var newExpression: Expression;
      if (k === 0) {
        newExpression = this.expression;
      } else {
        var value = this.valueOf();
        value.actions = actions.slice(0, k);
        newExpression = new ChainExpression(value);
      }

      return [
        new Action.classMap[containingAction]({
          expression: newExpression
        })
      ].concat(actions.slice(k));
    }

    public firstAction(): Action {
      return this.actions[0] || null;
    }

    public lastAction(): Action {
      var { actions } = this;
      return actions[actions.length - 1] || null;
    }

    public popAction(): Expression {
      if (arguments.length) {
        console.error('popAction no longer takes any arguments, check lastAction instead');
      }
      var actions = this.actions;
      if (!actions.length) return null;
      actions = actions.slice(0, -1);
      if (!actions.length) return this.expression;
      var value = this.valueOf();
      value.actions = actions;
      return new ChainExpression(value);
    }

    public _computeResolvedSimulate(simulatedQueries: any[]): any {
      var actions = this.actions;

      function execAction(i: int, dataset: Dataset): Dataset {
        var action = actions[i];
        var actionExpression = action.expression;

        if (action instanceof FilterAction) {
          return dataset.filter(action.expression.getFn(), null);

        } else if (action instanceof ApplyAction) {
          if (actionExpression.hasExternal()) {
            return dataset.apply(action.name, (d: Datum) => {
              var simpleActionExpression = actionExpression.resolve(d);
              simpleActionExpression = simpleActionExpression.simplify();
              return simpleActionExpression._computeResolvedSimulate(simulatedQueries);
            }, null);
          } else {
            return dataset.apply(action.name, actionExpression.getFn(), null);
          }

        } else if (action instanceof SortAction) {
          return dataset.sort(actionExpression.getFn(), action.direction, null);

        } else if (action instanceof LimitAction) {
          return dataset.limit(action.limit);

        }
      }

      var value = this.expression._computeResolvedSimulate(simulatedQueries);
      for (var i = 0; i < actions.length; i++) {
        value = execAction(i, value);
      }
      return value;
    }

    public _computeResolved(): Q.Promise<Dataset> {
      var actions = this.actions;

      function execAction(i: int) {
        return (dataset: Dataset): Dataset | Q.Promise<Dataset> => {
          var action = actions[i];
          var actionExpression = action.expression;

          if (action instanceof FilterAction) {
            return dataset.filter(action.expression.getFn(), null);

          } else if (action instanceof ApplyAction) {
            if (actionExpression.hasExternal()) {
              return dataset.applyPromise(action.name, (d: Datum) => {
                return actionExpression.resolve(d).simplify()._computeResolved();
              }, null);
            } else {
              return dataset.apply(action.name, actionExpression.getFn(), null);
            }

          } else if (action instanceof SortAction) {
            return dataset.sort(actionExpression.getFn(), action.direction, null);

          } else if (action instanceof LimitAction) {
            return dataset.limit(action.limit);

          }
        }
      }

      var promise = this.expression._computeResolved();
      for (var i = 0; i < actions.length; i++) {
        promise = promise.then(execAction(i));
      }
      return promise;
    }

    public separateViaAnd(refName: string): Separation {
      if (typeof refName !== 'string') throw new Error('must have refName');
      if (!this.simple) return this.simplify().separateViaAnd(refName);

      var andExpressions = this.getExpressionPattern('and');
      if (!andExpressions) {
        return super.separateViaAnd(refName);
      }

      var includedExpressions: Expression[] = [];
      var excludedExpressions: Expression[] = [];
      for (let operand of andExpressions) {
        var sep = operand.separateViaAnd(refName);
        if (sep === null) return null;
        includedExpressions.push(sep.included);
        excludedExpressions.push(sep.excluded);
      }

      return {
        included: Expression.and(includedExpressions).simplify(),
        excluded: Expression.and(excludedExpressions).simplify()
      };
    }
  }

  Expression.register(ChainExpression);
}
