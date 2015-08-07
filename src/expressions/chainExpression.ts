module Plywood {
  export class ChainExpression extends Expression {
    static fromJS(parameters: ExpressionJS): ChainExpression {
      var value: ExpressionValue = {
        op: parameters.op
      };
      value.expression = Expression.fromJS(parameters.expression);
      value.actions = parameters.actions.map(Action.fromJS);
      return new ChainExpression(value);
    }

    public expression: Expression;
    public actions: Action[];

    constructor(parameters: ExpressionValue) {
      super(parameters, dummyObject);
      var expression = parameters.expression;
      this.expression = expression;
      var actions = parameters.actions;
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
      js.actions = this.actions.map(action => action.toJS());
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
      throw new Error("can not call getJS on actions");
    }

    public getSQL(dialect: SQLDialect, minimal: boolean = false): string {
      throw new Error("can not call getSQL on actions");
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

    public simplify(): Expression {
      if (this.simple) return this;

      var simpleExpression = this.expression.simplify();
      if (simpleExpression instanceof ChainExpression) {
        return new ChainExpression({
          expression: simpleExpression.expression,
          actions: simpleExpression.actions.concat(this.actions)
        }).simplify();
      }

      // Simplify all actions
      var actions = this.actions;
      var simpleActions: Action[] = [];
      for (let action of actions) {
        let actionSimplification = action.simplify();
        if (actionSimplification) {
          switch (actionSimplification.simplification) {
            case Simplification.Replace:
              simpleActions = simpleActions.concat(actionSimplification.actions);
              break;

            case Simplification.Wipe:
              simpleActions = [];
              simpleExpression = actionSimplification.expression.simplify();
              break;

            default: // Simplification.Remove
              break;
          }
        } else {
          simpleActions.push(action);
        }
      }

      // In case of literal fold accordingly
      while (simpleExpression instanceof LiteralExpression && simpleActions.length) {
        let foldedExpression = simpleActions[0].foldLiteral(<LiteralExpression>simpleExpression);
        if (!foldedExpression) break;
        simpleActions.shift();
        simpleExpression = foldedExpression.simplify();
      }

      // ToDo: try to merge actions here

      if (simpleExpression instanceof LiteralExpression && simpleExpression.type === 'DATASET' && simpleActions.length) {
        var dataset: Dataset = (<LiteralExpression>simpleExpression).value;
        var externalAction = simpleActions[0];
        var externalExpression = externalAction.expression;
        if (dataset.basis() && externalAction.action === 'apply') {
          if (externalExpression instanceof ExternalExpression) {
            var newTotalExpression = externalExpression.makeTotal();
            if (newTotalExpression) {
              simpleExpression = newTotalExpression;
              simpleActions.shift();
            }
          } else {
            var externals = this.getExternals();
            if (externals.length === 1) {
              simpleExpression = new ExternalExpression({
                external: externals[0].makeTotal()
              });
            } else {
              throw new Error('not done yet');
            }
          }
        }
      }

      if (simpleExpression instanceof ExternalExpression) {
        var undigestedActions: Action[] = [];
        for (let simpleAction of simpleActions) {
          if (undigestedActions.length && simpleAction.getFreeReferences().length > 1) { // ToDo: fix the > 1
            undigestedActions.push(simpleAction);
            continue;
          }

          let newSimpleExpression = (<ExternalExpression>simpleExpression).addAction(simpleAction);
          if (newSimpleExpression) {
            simpleExpression = newSimpleExpression;
          } else {
            undigestedActions.push(simpleAction);
          }
        }
        simpleActions = undigestedActions;
      }

      /*
      function isRemoteSimpleApply(action: Action): boolean {
        return action instanceof ApplyAction && action.expression.hasExternal() && action.expression.type !== 'DATASET';
      }

      // These are actions on a remote dataset
      var externals = this.getExternals();
      var external: External;
      var digestedOperand = simpleExpression;
      if (externals.length && (digestedOperand instanceof LiteralExpression || digestedOperand instanceof JoinExpression)) {
        external = externals[0];
        if (digestedOperand instanceof LiteralExpression && !digestedOperand.isRemote() && simpleActions.some(isRemoteSimpleApply)) {
          if (externals.length === 1) {
            digestedOperand = new LiteralExpression({
              value: external.makeTotal()
            });
          } else {
            throw new Error('not done yet')
          }
        }

        var undigestedActions: Action[] = [];
        for (var i = 0; i < simpleActions.length; i++) {
          var action: Action = simpleActions[i];
          var digest = external.digest(digestedOperand, action);
          if (digest) {
            digestedOperand = digest.expression;
            if (digest.undigested) undigestedActions.push(digest.undigested);

          } else {
            undigestedActions.push(action);
          }
        }
        if (simpleExpression !== digestedOperand) {
          simpleExpression = digestedOperand;
          simpleActions = defsToAddBack.concat(undigestedActions);
        }
      }
      */

      if (simpleActions.length === 0) return simpleExpression;

      var simpleValue = this.valueOf();
      simpleValue.expression = simpleExpression;
      simpleValue.actions = simpleActions;
      simpleValue.simple = true;
      return new ChainExpression(simpleValue);
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
      var actionNestDiff = nestDiff + (expression.type === 'DATASET' ? 1 : 0);

      var actions = this.actions;
      var every: boolean = true;
      for (let action of actions) {
        if (every) {
          every = action._everyHelper(iter, thisArg, indexer, depth, actionNestDiff);
          actionNestDiff += action.contextDiff();
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
      var actionNestDiff = nestDiff + (expression.type === 'DATASET' ? 1 : 0);

      var actions = this.actions;
      var subActions = actions.map(action => {
        var subbedAction = action._substituteHelper(substitutionFn, thisArg, indexer, depth, actionNestDiff);
        actionNestDiff += action.contextDiff();
        return subbedAction;
      });
      if (expression === subExpression && arraysEqual(actions, subActions)) return this;

      var value = this.valueOf();
      value.expression = subExpression;
      value.actions = subActions;
      delete value.simple;
      return new ChainExpression(value);
    }

    public _performAction(action: Action): ChainExpression {
      return new ChainExpression({
        expression: this.expression,
        actions: this.actions.concat(action)
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

    public getExpressionPattern(actionType: string): Expression[] {
      var actions = this.actionize(actionType);
      if (!actions || actions.length < 2) return null;
      return actions.map((action) => action.expression);
    }

    public getTailPattern(actionType: string): Expression {
      var actions = this.actions;
      var lastAction = actions[actions.length - 1];
      if (lastAction.action !== actionType) return null;
      var value = this.valueOf();
      value.actions = actions.slice(0, -1);
      return new ChainExpression(value);
    }

    public _collectBindSpecs(bindSpecs: BindSpec[], selectionDepth: Lookup<number>, depth: number, applyName: string, data: string, key: string): void {
      var expression = this.expression;
      var actions = this.actions;
      switch (expression.type) {
        case 'DATASET':
          var nextData: string = null;
          var nextKey: string = null;
          for (let action of actions) {
            if (action instanceof SplitAction) {
              nextData = applyName;
              nextKey = action.name;
              depth++;
            } else if (action instanceof ApplyAction) {
              action.expression._collectBindSpecs(bindSpecs, selectionDepth, depth, action.name, nextData, nextKey);
            }
          }
          break;

        case 'MARK':
          var selectionInput = (<RefExpression>expression).name;
          for (let action of actions) {
            if (action instanceof AttachAction) {
              var bindSpec: BindSpec = {
                selectionInput,
                selector: action.selector,
                selectionName: applyName
              };
              if (!hasOwnProperty(selectionDepth, selectionInput)) throw new Error('something terrible has happened');
              if (data && depth > selectionDepth[selectionInput]) {
                bindSpec.data = data;
                bindSpec.key = key;
              }
              fillMethods(action.prop, bindSpec);
              bindSpecs.push(bindSpec);
              selectionDepth[applyName] = depth;
            } else {
              throw new Error('unknown action ' + action.action);
            }
          }

          break;
      }
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
              var simpleActionExpression = actionExpression.resolve(d).simplify();
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

      return null;
    }
  }

  Expression.register(ChainExpression);
}
