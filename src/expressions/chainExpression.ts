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
             immutableArraysEqual(this.actions, other.actions);
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
     * @param neededAction and optional type can be passed in to return only an action of this type
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
      const { expression, actions } = this;
      var baseExternals = this.getBaseExternals();
      if (baseExternals.length === 0) return this;

      // Looks like: External().blah().blah().blah()
      if (expression instanceof ExternalExpression) {
        var myExternal = expression;
        var undigestedActions: Action[] = [];
        for (var action of actions) {
          var newExternal = myExternal.addAction(action);
          if (newExternal) {
            myExternal = newExternal;
          } else {
            undigestedActions.push(action);
          }
        }

        if (undigestedActions.length) {
          return new ChainExpression({
            expression: myExternal,
            actions: undigestedActions,
            simple: true
          });
        } else {
          return myExternal;
        }
      }

      // Looks like: ply().apply(ValueExternal()).apply(ValueExternal()).apply(ValueExternal())
      var dataset = expression.getLiteralValue();
      if (Dataset.isDataset(dataset) && dataset.basis()) {
        if (baseExternals.length > 1) {
          throw new Error('multiple externals not supported for now'); // ToDo: would need to do a join at this point
        }

        var dataDefinitions: Lookup<ExternalExpression> = Object.create(null);
        var hasExternalValueApply = false;
        var applies: ApplyAction[] = [];
        var undigestedActions: Action[] = [];
        var allActions: Action[] = [];

        function addExternalApply(action: ApplyAction) {
          var externalMode = (<ExternalExpression>action.expression).external.mode;
          if (externalMode === 'raw') {
            dataDefinitions[action.name] = <ExternalExpression>action.expression;
          } else if (externalMode === 'value') {
            applies.push(action);
            allActions.push(action);
            hasExternalValueApply = true;
          } else {
            undigestedActions.push(action);
            allActions.push(action);
          }
        }

        for (let action of actions) {
          if (action instanceof ApplyAction) {
            var substitutedAction = <ApplyAction>action.substitute((ex, index, depth, nestDiff) => {
              if (ex instanceof RefExpression && ex.type === 'DATASET' && nestDiff === 1) {
                return dataDefinitions[ex.name] || null;
              }
              return null;
            }).simplify();

            if (substitutedAction.expression instanceof ExternalExpression) {
              addExternalApply(substitutedAction);
            } else if (substitutedAction.expression.type !== 'DATASET') {
              applies.push(substitutedAction);
              allActions.push(substitutedAction);
            } else {
              undigestedActions.push(substitutedAction);
              allActions.push(substitutedAction);
            }
          } else {
            undigestedActions.push(action);
            allActions.push(action);
          }
        }

        var newExpression: Expression;
        if (hasExternalValueApply) {
          var combinedExternal = baseExternals[0].makeTotal(applies);
          if (!combinedExternal) throw new Error('something went wrong');
          newExpression = new ExternalExpression({ external: combinedExternal });
          if (undigestedActions.length) newExpression = newExpression.performActions(undigestedActions, true);
          return newExpression;
        } else {
          return ply().performActions(allActions);
        }
      }

      // Looks like: $().blah().blah(ValueExternal()).blah()
      return this.substituteAction(
        (action) => {
          var expression = action.expression;
          return (expression instanceof ExternalExpression) && expression.external.mode === 'value';
        },
        (preEx: Expression, action: Action) => {
          var external = (action.expression as ExternalExpression).external;
          return new ExternalExpression({
            external: external.prePack(preEx, action)
          });
        },
        {
          onceInChain: true
        }
      ).simplify();
    }

    public simplify(): Expression {
      if (this.simple) return this;
      var simpleExpression = this.expression.simplify();
      var actions = this.actions;

      // In the unlikely event that there is a chain of a chain => merge them
      if (simpleExpression instanceof ChainExpression) {
        return new ChainExpression({
          expression: simpleExpression.expression,
          actions: simpleExpression.actions.concat(actions)
        }).simplify();
      }

      // Let the actions simplify (and re-arrange themselves)
      for (let action of actions) {
        simpleExpression = action.performOnSimple(simpleExpression);
      }

      // Return now if already as simple as can be
      if (!simpleExpression.isOp('chain')) return simpleExpression;

      return (simpleExpression as ChainExpression).foldIntoExternal();
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
      var subActions = actions.map(action => action._substituteHelper(substitutionFn, thisArg, indexer, depth, nestDiff));
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

    public _fillRefSubstitutions(typeContext: DatasetFullType, indexer: Indexer, alterations: Alterations): FullType {
      indexer.index++;

      // Some explanation of what is going on here is in order as this is the heart of the variable resolution code
      // The _fillRefSubstitutions function is chained across all the expressions.
      // If an expression returns a DATASET type it is treated as the new context otherwise the original context is
      // used for the next expression (currentContext)
      var currentContext: DatasetFullType = typeContext;
      var outputContext = this.expression._fillRefSubstitutions(currentContext, indexer, alterations);
      currentContext = outputContext.type === 'DATASET' ? <DatasetFullType>outputContext : typeContext;

      var actions = this.actions;
      for (let action of actions) {
        outputContext = action._fillRefSubstitutions(currentContext, outputContext, indexer, alterations);
        currentContext = outputContext.type === 'DATASET' ? <DatasetFullType>outputContext : typeContext;
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

    public headActions(n: int): Expression {
      var { actions } = this;
      if (actions.length <= n) return this;
      if (n <= 0) return this.expression;

      var value = this.valueOf();
      value.actions = actions.slice(0, n);
      return new ChainExpression(value);
    }

    public popAction(): Expression {
      var actions = this.actions;
      if (!actions.length) return null;
      actions = actions.slice(0, -1);
      if (!actions.length) return this.expression;
      var value = this.valueOf();
      value.actions = actions;
      return new ChainExpression(value);
    }

    public _computeResolvedSimulate(lastNode: boolean, simulatedQueries: any[]): PlywoodValue {
      var { expression, actions } = this;

      if (expression.isOp('external')) {
        var exV = expression._computeResolvedSimulate(false, simulatedQueries);
        var newExpression = r(exV).performActions(actions).simplify();
        if (newExpression.hasExternal()) {
          return newExpression._computeResolvedSimulate(true, simulatedQueries);
        } else {
          return newExpression.getFn()(null, null);
        }
      }

      function execAction(i: int, dataset: Dataset): Dataset {
        var action = actions[i];
        var actionExpression = action.expression;

        if (action instanceof FilterAction) {
          return dataset.filter(actionExpression.getFn(), null);

        } else if (action instanceof ApplyAction) {
          if (actionExpression.hasExternal()) {
            return dataset.apply(action.name, (d: Datum) => {
              var simpleExpression = actionExpression.resolve(d).simplify();
              return simpleExpression._computeResolvedSimulate(simpleExpression.isOp('external'), simulatedQueries);
            }, actionExpression.type, null);
          } else {
            return dataset.apply(action.name, actionExpression.getFn(), actionExpression.type, null);
          }

        } else if (action instanceof SortAction) {
          return dataset.sort(actionExpression.getFn(), action.direction, null);

        } else if (action instanceof LimitAction) {
          return dataset.limit(action.limit);

        } else if (action instanceof SelectAction) {
          return dataset.select(action.attributes);

        }

        throw new Error(`could not execute action ${action}`);
      }

      var value = expression._computeResolvedSimulate(false, simulatedQueries);
      for (var i = 0; i < actions.length; i++) {
        value = execAction(i, value as Dataset);
      }
      return value;
    }

    public _computeResolved(): Q.Promise<PlywoodValue> {
      var { expression, actions } = this;

      if (expression.isOp('external')) {
        return expression._computeResolved(false).then((exV) => {
          var newExpression = r(exV).performActions(actions).simplify();
          if (newExpression.hasExternal()) {
            return newExpression._computeResolved(true);
          } else {
            return newExpression.getFn()(null, null);
          }
        });
      }

      function execAction(i: int) {
        return (dataset: Dataset): Dataset | Q.Promise<Dataset> => {
          var action = actions[i];
          var actionExpression = action.expression;

          if (action instanceof FilterAction) {
            return dataset.filter(actionExpression.getFn(), null);

          } else if (action instanceof ApplyAction) {
            if (actionExpression.hasExternal()) {
              return dataset.applyPromise(action.name, (d: Datum) => {
                var simpleExpression = actionExpression.resolve(d).simplify();
                return simpleExpression._computeResolved(simpleExpression.isOp('external'));
              }, actionExpression.type, null);
            } else {
              return dataset.apply(action.name, actionExpression.getFn(), actionExpression.type, null);
            }

          } else if (action instanceof SortAction) {
            return dataset.sort(actionExpression.getFn(), action.direction, null);

          } else if (action instanceof LimitAction) {
            return dataset.limit(action.limit);

          } else if (action instanceof SelectAction) {
            return dataset.select(action.attributes);

          }

          throw new Error(`could not execute action ${action}`);
        }
      }

      var promise = expression._computeResolved(false);
      for (var i = 0; i < actions.length; i++) {
        promise = promise.then(execAction(i));
      }
      return promise;
    }

    public extractFromAnd(matchFn: ExpressionMatchFn): ExtractAndRest {
      if (!this.simple) return this.simplify().extractFromAnd(matchFn);

      var andExpressions = this.getExpressionPattern('and');
      if (!andExpressions) return super.extractFromAnd(matchFn);

      var includedExpressions: Expression[] = [];
      var excludedExpressions: Expression[] = [];
      for (let ex of andExpressions) {
        if (matchFn(ex)) {
          includedExpressions.push(ex);
        } else {
          excludedExpressions.push(ex);
        }
      }

      return {
        extract: Expression.and(includedExpressions).simplify(),
        rest: Expression.and(excludedExpressions).simplify()
      };
    }

    public maxPossibleSplitValues(): number {
      return this.type === 'BOOLEAN' ? 3 : this.lastAction().maxPossibleSplitValues();
    }
  }

  Expression.register(ChainExpression);
}
