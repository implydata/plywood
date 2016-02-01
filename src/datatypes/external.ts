module Plywood {
  export interface PostProcess {
    (result: any): Dataset;
  }

  export interface QueryAndPostProcess<T> {
    query: T;
    postProcess: PostProcess;
  }

  export interface Inflater {
    (d: Datum, i: number, data: Datum[]): void;
  }

  export function mergeExternals(externalGroups: External[][]): External[] {
    var seen: Lookup<External> = {};
    externalGroups.forEach(externalGroup => {
      externalGroup.forEach(external => {
        var id = external.getId();
        if (seen[id]) return;
        seen[id] = external;
      })
    });
    return Object.keys(seen).sort().map(k => seen[k]);
  }

  function getSampleValue(valueType: string, ex: Expression): any {
    switch (valueType) {
      case 'BOOLEAN':
        return true;

      case 'NUMBER':
        return 4;

      case 'NUMBER_RANGE':
        var numberBucketAction: NumberBucketAction;
        if (ex instanceof ChainExpression && (numberBucketAction = <NumberBucketAction>ex.getSingleAction('numberBucket'))) {
          return new NumberRange({
            start: numberBucketAction.offset,
            end: numberBucketAction.offset + numberBucketAction.size
          });
        } else {
          return new NumberRange({ start: 0, end: 1 });
        }

      case 'TIME':
        return new Date('2015-03-14T00:00:00');

      case 'TIME_RANGE':
        var timeBucketAction: TimeBucketAction;
        if (ex instanceof ChainExpression && (timeBucketAction = <TimeBucketAction>ex.getSingleAction('timeBucket'))) {
          var timezone = timeBucketAction.timezone || Timezone.UTC;
          var start = timeBucketAction.duration.floor(new Date('2015-03-14T00:00:00'), timezone);
          return new TimeRange({
            start,
            end: timeBucketAction.duration.move(start, timezone, 1)
          });
        } else {
          return new TimeRange({ start: new Date('2015-03-14T00:00:00'), end: new Date('2015-03-15T00:00:00') });
        }

      case 'STRING':
        if (ex instanceof RefExpression) {
          return 'some_' + ex.name;
        } else {
          return 'something';
        }

      case 'SET/STRING':
        if (ex instanceof RefExpression) {
          return Set.fromJS([ex.name + '1']);
        } else {
          return Set.fromJS(['something']);
        }

      default:
        throw new Error("unsupported simulation on: " + valueType);
    }
  }

  function immutableAdd<T>(obj: Lookup<T>, key: string, value: T): Lookup<T> {
    var newObj = Object.create(null);
    for (var k in obj) newObj[k] = obj[k];
    newObj[key] = value;
    return newObj;
  }

  export interface ExternalValue {
    engine?: string;
    suppress?: boolean;
    attributes?: Attributes;
    attributeOverrides?: Attributes;
    mode?: string;
    dataName?: string;

    filter?: Expression;
    rawAttributes?: Attributes;
    derivedAttributes?: Lookup<Expression>;
    split?: SplitAction;
    applies?: ApplyAction[];
    sort?: SortAction;
    limit?: LimitAction;
    havingFilter?: Expression;

    // MySQL
    table?: string;

    // Druid
    dataSource?: string | string[];
    timeAttribute?: string;
    customAggregations?: CustomDruidAggregations;
    allowEternity?: boolean;
    allowSelectQueries?: boolean;
    introspectionStrategy?: string;
    exactResultsOnly?: boolean;
    context?: Lookup<any>;
    druidVersion?: string;

    requester?: Requester.PlywoodRequester<any>;
  }

  export interface ExternalJS {
    engine: string;
    attributes?: AttributeJSs;
    attributeOverrides?: AttributeJSs;

    filter?: ExpressionJS;
    rawAttributes?: AttributeJSs;

    // MySQL
    table?: string;

    // Druid
    dataSource?: string | string[];
    timeAttribute?: string;
    customAggregations?: CustomDruidAggregations;
    allowEternity?: boolean;
    allowSelectQueries?: boolean;
    introspectionStrategy?: string;
    exactResultsOnly?: boolean;
    context?: Lookup<any>;
    druidVersion?: string;

    requester?: Requester.PlywoodRequester<any>;
  }

  export class External {
    static type = 'EXTERNAL';

    static isExternal(candidate: any): boolean {
      return isInstanceOf(candidate, External);
    }

    // ==== Inflaters

    static getSimpleInflater(splitExpression: Expression, label: string): Inflater {
      switch (splitExpression.type) {
        case 'BOOLEAN': return External.booleanInflaterFactory(label);
        case 'NUMBER': return External.numberInflaterFactory(label);
        case 'TIME': return External.timeInflaterFactory(label);
        default: return null;
      }
    }

    static booleanInflaterFactory(label: string): Inflater {
      return (d: any) => {
        var v = '' + d[label];
        switch (v) {
          case 'null':
            d[label] = null;
            break;

          case 'false':
            d[label] = false;
            break;

          case 'true':
            d[label] = true;
            break;

          default:
            throw new Error("got strange result from boolean: " + v);
        }
      };
    }

    static timeRangeInflaterFactory(label: string, duration: Duration, timezone: Timezone): Inflater {
      return (d: any) => {
        var v = d[label];
        if ('' + v === "null") {
          d[label] = null;
          return;
        }

        var start = new Date(v);
        d[label] = new TimeRange({ start, end: duration.move(start, timezone) })
      };
    }

    static consecutiveTimeRangeInflaterFactory(label: string, duration: Duration, timezone: Timezone): Inflater {
      var canonicalDurationLengthAndThenSome = duration.getCanonicalLength() * 1.5;
      return (d: any, i: int, data: Datum[]) => {
        var v = d[label];
        if ('' + v === "null") {
          d[label] = null;
          return;
        }

        var start = new Date(v);
        var next = data[i + 1];
        var nextTimestamp: Date;
        if (next) {
          nextTimestamp = new Date(next[label]);
        }

        var end = (
          nextTimestamp &&
          start.valueOf() < nextTimestamp.valueOf() &&
          nextTimestamp.valueOf() - start.valueOf() < canonicalDurationLengthAndThenSome
        ) ? nextTimestamp
          : duration.move(start, timezone, 1);

        d[label] = new TimeRange({ start, end });
      };
    }

    static numberRangeInflaterFactory(label: string, rangeSize: number): Inflater  {
      return (d: any) => {
        var v = d[label];
        if ('' + v === "null") {
          d[label] = null;
          return;
        }

        var start = Number(v);
        d[label] = new NumberRange({
          start: start,
          end: safeAdd(start, rangeSize)
        });
      }
    }

    static numberInflaterFactory(label: string): Inflater  {
      return (d: any) => {
        var v = d[label];
        if ('' + v === "null") {
          d[label] = null;
          return;
        }

        d[label] = Number(v);
      }
    }

    static timeInflaterFactory(label: string): Inflater  {
      return (d: any) => {
        var v = d[label];
        if ('' + v === "null") {
          d[label] = null;
          return;
        }

        d[label] = new Date(v);
      }
    }

    static jsToValue(parameters: ExternalJS): ExternalValue {
      var value: ExternalValue = {
        engine: parameters.engine,
        suppress: true
      };
      if (parameters.attributes) {
        value.attributes = AttributeInfo.fromJSs(parameters.attributes);
      }
      if (parameters.attributeOverrides) {
        value.attributeOverrides = AttributeInfo.fromJSs(parameters.attributeOverrides);
      }
      if (parameters.requester) value.requester = parameters.requester;
      value.filter = parameters.filter ? Expression.fromJS(parameters.filter) : Expression.TRUE;

      return value;
    }

    static classMap: Lookup<typeof External> = {};
    static register(ex: typeof External, id: string = null): void {
      if (!id) id = (<any>ex).name.replace('External', '').replace(/^\w/, (s: string) => s.toLowerCase());
      External.classMap[id] = ex;
    }

    static fromJS(parameters: ExternalJS): External {
      if (!hasOwnProperty(parameters, "engine")) {
        throw new Error("external `engine` must be defined");
      }
      var engine: string = parameters.engine;
      if (typeof engine !== "string") {
        throw new Error("dataset must be a string");
      }
      var ClassFn = External.classMap[engine];
      if (!ClassFn) {
        throw new Error(`unsupported engine '${engine}'`);
      }
      return ClassFn.fromJS(parameters);
    }

    public engine: string;
    public suppress: boolean;
    public attributes: Attributes = null;
    public attributeOverrides: Attributes = null;

    public rawAttributes: Attributes = null;
    public requester: Requester.PlywoodRequester<any>;
    public mode: string; // raw, total, split (potential aggregate mode)
    public derivedAttributes: Lookup<Expression>;
    public filter: Expression;
    public dataName: string;
    public split: SplitAction;
    public applies: ApplyAction[];
    public sort: SortAction;
    public limit: LimitAction;
    public havingFilter: Expression;

    constructor(parameters: ExternalValue, dummy: Dummy = null) {
      if (dummy !== dummyObject) {
        throw new TypeError("can not call `new External` directly use External.fromJS instead");
      }
      this.engine = parameters.engine;
      this.suppress = parameters.suppress === true;
      if (parameters.attributes) {
        this.attributes = parameters.attributes;
      }
      if (parameters.attributeOverrides) {
        this.attributeOverrides = parameters.attributeOverrides;
      }
      this.rawAttributes = parameters.rawAttributes;
      this.requester = parameters.requester;
      this.mode = parameters.mode || 'raw';
      this.derivedAttributes = parameters.derivedAttributes || {};
      this.filter = parameters.filter || Expression.TRUE;
      this.split = parameters.split;
      this.dataName = parameters.dataName;
      this.applies = parameters.applies;
      this.sort = parameters.sort;
      this.limit = parameters.limit;
      this.havingFilter = parameters.havingFilter;

      if (this.mode !== 'raw') {
        this.applies = this.applies || [];

        if (this.mode === 'split') {
          if (!this.split) throw new Error('must have split action in split mode');
          this.havingFilter = this.havingFilter || Expression.TRUE;
        }
      }
    }

    protected _ensureEngine(engine: string) {
      if (!this.engine) {
        this.engine = engine;
        return;
      }
      if (this.engine !== engine) {
        throw new TypeError(`incorrect engine '${this.engine}' (needs to be: '${engine}')`);
      }
    }

    public valueOf(): ExternalValue {
      var value: ExternalValue = {
        engine: this.engine
      };
      if (this.suppress) value.suppress = this.suppress;
      if (this.attributes) value.attributes = this.attributes;
      if (this.attributeOverrides) value.attributeOverrides = this.attributeOverrides;

      if (this.rawAttributes) {
        value.rawAttributes = this.rawAttributes;
      }
      if (this.requester) {
        value.requester = this.requester;
      }
      value.mode = this.mode;
      if (this.dataName) {
        value.dataName = this.dataName;
      }
      value.derivedAttributes = this.derivedAttributes;
      value.filter = this.filter;
      if (this.split) {
        value.split = this.split;
      }
      if (this.applies) {
        value.applies = this.applies;
      }
      if (this.sort) {
        value.sort = this.sort;
      }
      if (this.limit) {
        value.limit = this.limit;
      }
      if (this.havingFilter) {
        value.havingFilter = this.havingFilter;
      }
      return value;
    }

    public toJS(): ExternalJS {
      var js: ExternalJS = {
        engine: this.engine
      };
      if (this.attributes) js.attributes = AttributeInfo.toJSs(this.attributes);
      if (this.attributeOverrides) js.attributeOverrides = AttributeInfo.toJSs(this.attributeOverrides);

      if (this.rawAttributes) js.rawAttributes = AttributeInfo.toJSs(this.rawAttributes);
      if (this.requester) {
        js.requester = this.requester;
      }
      if (!this.filter.equals(Expression.TRUE)) {
        js.filter = this.filter.toJS();
      }
      return js;
    }

    public toJSON(): ExternalJS {
      return this.toJS();
    }

    public toString(): string {
      switch (this.mode) {
        case 'raw':
          return `ExternalRaw(${this.filter.toString()})`;

        case 'total':
          return `ExternalTotal(${this.applies.length})`;

        case 'split':
          return `ExternalSplit(${this.applies.length})`;

        default :
          return 'External()';
      }

    }

    public equals(other: External): boolean {
      return External.isExternal(other) &&
        this.engine === other.engine &&
        this.mode === other.mode &&
        this.filter.equals(other.filter);
    }

    public getId(): string {
      return this.engine + ':' + this.filter.toString();
    }

    public hasExternal(): boolean {
      return true;
    }

    public getExternals(): External[] {
      return [this];
    }

    public getExternalIds(): string[] {
      return [this.getId()]
    }

    public getAttributesInfo(attributeName: string) {
      var attributes = this.rawAttributes || this.attributes;
      for (var attribute of attributes) {
        if (attribute.name === attributeName) return attribute;
      }
      return null;
    }

    public updateAttribute(newAttribute: AttributeInfo): External {
      if (!this.attributes) return this;
      var newAttributeName = newAttribute.name;
      var added = false;

      var value = this.valueOf();

      value.attributes = value.attributes.map((attribute) => {
        if (attribute.name === newAttributeName) {
          added = true;
          return newAttribute;
        } else {
          return attribute;
        }
      });

      if (!added) {
        // At this point map already made a copy of the list
        value.attributes.push(newAttribute);
      }

      return new (External.classMap[this.engine])(value);
    }

    public show(): External {
      var value = this.valueOf();
      value.suppress = false;
      return new (External.classMap[this.engine])(value);
    }

    // -----------------

    public canHandleFilter(ex: Expression): boolean {
      throw new Error("must implement canHandleFilter");
    }

    public canHandleTotal(): boolean {
      throw new Error("must implement canHandleTotal");
    }

    public canHandleSplit(ex: Expression): boolean {
      throw new Error("must implement canHandleSplit");
    }

    public canHandleApply(ex: Expression): boolean {
      throw new Error("must implement canHandleApply");
    }

    public canHandleSort(sortAction: SortAction): boolean {
      throw new Error("must implement canHandleSort");
    }

    public canHandleLimit(limitAction: LimitAction): boolean {
      throw new Error("must implement canHandleLimit");
    }

    public canHandleHavingFilter(ex: Expression): boolean {
      throw new Error("must implement canHandleHavingFilter");
    }

    // -----------------

    // ToDo: make this better
    public getRaw(): External {
      if (this.mode === 'raw') return this;

      var value = this.valueOf();
      value.suppress = true;
      value.mode = 'raw';
      value.dataName = null;
      value.attributes = value.rawAttributes;
      value.rawAttributes = null;
      value.applies = [];
      value.split = null;
      value.sort = null;
      value.limit = null;

      return <External>(new (External.classMap[this.engine])(value));
    }

    public makeTotal(dataName: string): External {
      if (this.mode !== 'raw') return null; // Can only split on 'raw' datasets
      if (!this.canHandleTotal()) return null;

      var value = this.valueOf();
      value.suppress = false;
      value.mode = 'total';
      value.dataName = dataName;
      value.rawAttributes = value.attributes;
      value.attributes = [];

      return <External>(new (External.classMap[this.engine])(value));
    }

    public addAction(action: Action): External {
      if (action instanceof FilterAction) {
        return this._addFilterAction(action);
      }
      if (action instanceof SplitAction) {
        return this._addSplitAction(action);
      }
      if (action instanceof ApplyAction) {
        return this._addApplyAction(action);
      }
      if (action instanceof SortAction) {
        return this._addSortAction(action);
      }
      if (action instanceof LimitAction) {
        return this._addLimitAction(action);
      }
      return null;
    }

    private _addFilterAction(action: FilterAction): External {
      return this.addFilter(action.expression);
    }

    public addFilter(expression: Expression): External {
      if (!expression.resolved()) return null;

      var value = this.valueOf();
      switch (this.mode) {
        case 'raw':
          if (!this.canHandleFilter(expression)) return null;
          value.filter = value.filter.and(expression).simplify();
          break;

        case 'split':
          if (!this.canHandleHavingFilter(expression)) return null;
          value.havingFilter = value.havingFilter.and(expression).simplify();
          break;

        default:
          return null; // can not add filter in total mode
      }

      return <External>(new (External.classMap[this.engine])(value));
    }

    private _addSplitAction(splitAction: SplitAction): External {
      if (this.mode !== 'raw') return null; // Can only split on 'raw' datasets
      //if (!this.canHandleSplit(expression)) return null;

      var value = this.valueOf();
      value.suppress = false;
      value.mode = 'split';
      value.dataName = splitAction.dataName;
      value.split = splitAction;
      value.rawAttributes = value.attributes;
      value.attributes = splitAction.mapSplits((name, expression) => new AttributeInfo({ name, type: expression.type }));

      return <External>(new (External.classMap[this.engine])(value));
    }

    private _addApplyAction(action: ApplyAction): External {
      var expression = action.expression;
      if (expression.type !== 'NUMBER' && expression.type !== 'TIME') return null;
      if (!this.canHandleApply(action.expression)) return null;

      var value = this.valueOf();
      if (this.mode === 'raw') {
        value.derivedAttributes = immutableAdd(
          value.derivedAttributes, action.name, action.expression
        );
        value.attributes = value.attributes.concat(new AttributeInfo({ name: action.name, type: action.expression.type }));
      } else {
        // Can not redefine index for now.
        if (this.split && this.split.hasKey(action.name)) return null;

        var basicActions = this.processApply(action);
        for (let basicAction of basicActions) {
          if (basicAction instanceof ApplyAction) {
            value.applies = value.applies.concat(basicAction);
            value.attributes = value.attributes.concat(new AttributeInfo({ name: basicAction.name, type: basicAction.expression.type }));
          } else {
            throw new Error('got something strange from breakUpApply');
          }
        }
      }
      return <External>(new (External.classMap[this.engine])(value));
    }

    private _addSortAction(action: SortAction): External {
      if (this.limit) return null; // Can not sort after limit
      if (!this.canHandleSort(action)) return null;

      var value = this.valueOf();
      value.sort = action;
      return <External>(new (External.classMap[this.engine])(value));
    }

    private _addLimitAction(action: LimitAction): External {
      if (!this.canHandleLimit(action)) return null;

      var value = this.valueOf();
      if (!value.limit || action.limit < value.limit.limit) {
        value.limit = action;
      }
      return <External>(new (External.classMap[this.engine])(value));
    }

    // ----------------------

    public getExistingApplyForExpression(expression: Expression): ApplyAction {
      var applies = this.applies;
      for (let apply of applies) {
        if (apply.expression.equals(expression)) return apply;
      }
      return null;
    }

    public isKnownName(name: string): boolean {
      var attributes = this.attributes;
      for (var attribute of attributes) {
        if (attribute.name === name) return true;
      }
      return false;
    }

    public getTempName(namesTaken: string[] = []): string {
      for (let i = 0; i < 1e6; i++) {
        var name = '_sd_' + i;
        if (namesTaken.indexOf(name) === -1 && !this.isKnownName(name)) return name;
      }
      throw new Error('could not find available name');
    }

    public sortOnLabel(): boolean {
      var sort = this.sort;
      if (!sort) return false;

      var sortOn = (<RefExpression>sort.expression).name;
      if (!this.split || !this.split.hasKey(sortOn)) return false;

      var applies = this.applies;
      for (let apply of applies) {
        if (apply.name === sortOn) return false;
      }

      return true;
    }

    public separateAggregates(apply: ApplyAction): ApplyAction[] {
      var applyExpression = apply.expression;
      if (applyExpression instanceof ChainExpression) {
        var actions = applyExpression.actions;
        if (actions[actions.length - 1].isAggregate()) {
          // This is a vanilla aggregate, just return it.
          return [apply];
        }
      }

      var applies: ApplyAction[] = [];
      var namesUsed: string[] = [];

      var newExpression = applyExpression.substituteAction(
        (action) => {
          return action.isAggregate();
        },
        (preEx: Expression, action: Action) => {
          var aggregateChain = preEx.performAction(action);
          var existingApply = this.getExistingApplyForExpression(aggregateChain);
          if (existingApply) {
            return new RefExpression({
              name: existingApply.name,
              nest: 0,
              type: existingApply.expression.type
            });
          } else {
            var name = this.getTempName(namesUsed);
            namesUsed.push(name);
            applies.push(new ApplyAction({
              action: 'apply',
              name: name,
              expression: aggregateChain
            }));
            return new RefExpression({
              name: name,
              nest: 0,
              type: aggregateChain.type
            });
          }
        },
        this
      );

      applies.push(new ApplyAction({
        action: 'apply',
        name: apply.name,
        expression: newExpression
      }));

      return applies;
    }

    public inlineDerivedAttributes(expression: Expression): Expression {
      var derivedAttributes = this.derivedAttributes;
      return expression.substitute(ex => {
        return null;
        /*
        if (ex instanceof AggregateExpression) {
          return ex.substitute(refEx => {
            if (refEx instanceof RefExpression) {
              var refName = refEx.name;
              return hasOwnProperty(derivedAttributes, refName) ? derivedAttributes[refName] : null;
            } else {
              return null;
            }
          });
        } else {
          return null;
        }
        */
      })
    }

    public processApply(action: ApplyAction): Action[] {
      return [action];
    }

    // -----------------

    public getEmptyTotalDataset(): Dataset {
      if (this.mode !== 'total' || this.applies.length) return null;
      var dataName = this.dataName;
      return new Dataset({ data: [{}] }).apply(dataName, () => {
        return this.getRaw();
      }, null);
    }

    public addNextExternal(dataset: Dataset): Dataset {
      var dataName = this.dataName;
      switch (this.mode) {
        case 'total':
          return dataset.apply(dataName, () => {
            return this.getRaw();
          }, null);

        case 'split':
          var split = this.split;
          return dataset.apply(dataName, (d: Datum) => {
            return this.getRaw().addFilter(split.filterFromDatum(d));
          }, null);

        default:
          return dataset;
      }
    }

    public simulate(): Dataset {
      var datum: Datum = {};

      if (this.mode === 'raw') {
        var attributes = this.attributes;
        for (let attribute of attributes) {
          datum[attribute.name] = getSampleValue(attribute.type, null);
        }
      } else {
        if (this.mode === 'split') {
          this.split.mapSplits((name, expression) => {
            datum[name] = getSampleValue(expression.type, expression);
          });
        }

        var applies = this.applies;
        for (let apply of applies) {
          datum[apply.name] = getSampleValue(apply.expression.type, apply.expression);
        }
      }

      var dataset = new Dataset({ data: [datum] });
      dataset = this.addNextExternal(dataset);
      return dataset;
    }

    public getQueryAndPostProcess(): QueryAndPostProcess<any> {
      throw new Error("can not call getQueryAndPostProcess directly");
    }

    public queryValues(): Q.Promise<Dataset> {
      if (!this.requester) {
        return <Q.Promise<Dataset>>Q.reject(new Error('must have a requester to make queries'));
      }
      try {
        var queryAndPostProcess = this.getQueryAndPostProcess();
      } catch (e) {
        return <Q.Promise<Dataset>>Q.reject(e);
      }
      if (!hasOwnProperty(queryAndPostProcess, 'query') || typeof queryAndPostProcess.postProcess !== 'function') {
        return <Q.Promise<Dataset>>Q.reject(new Error('no error query or postProcess'));
      }
      var result = this.requester({ query: queryAndPostProcess.query })
        .then(queryAndPostProcess.postProcess);

      if (this.mode !== 'raw') {
        result = <Q.Promise<Dataset>>result.then(this.addNextExternal.bind(this));
      }

      return result;
    }

    // -------------------------

    public needsIntrospect(): boolean {
      return !this.attributes;
    }

    public getIntrospectAttributes(): Q.Promise<Attributes> {
      throw new Error("can not call getIntrospectAttributes directly");
    }

    public introspect(): Q.Promise<External> {
      if (this.attributes) {
        return Q(this);
      }

      if (!this.requester) {
        return <Q.Promise<External>>Q.reject(new Error('must have a requester to introspect'));
      }

      var value = this.valueOf();
      var ClassFn = External.classMap[this.engine];
      return this.getIntrospectAttributes()
        .then((attributes: Attributes) => {
          if (value.attributeOverrides) {
            attributes = AttributeInfo.applyOverrides(attributes, value.attributeOverrides);
          }
          value.attributes = attributes;
          // Once attributes are set attributeOverrides will be ignored
          return <External>(new ClassFn(value));
        })
    }

    public getFullType(): FullType {
      var attributes = this.attributes;
      if (!attributes) throw new Error("dataset has not been introspected");

      var remote = [this.engine];

      var myDatasetType: Lookup<FullType> = {};
      for (var attribute of attributes) {
        var attrName = attribute.name;
        myDatasetType[attrName] = {
          type: attribute.type,
          remote
        };
      }
      var myFullType: FullType = {
        type: 'DATASET',
        datasetType: myDatasetType,
        remote
      };
      return myFullType;
    }

    // ------------------------

    /*
    private _joinDigestHelper(joinExpression: JoinExpression, action: Action): JoinExpression {
      var ids = action.expression.getExternalIds();
      if (ids.length !== 1) throw new Error('must be single dataset');
      if (ids[0] === (<External>(<LiteralExpression>joinExpression.lhs).value).getId()) {
        var lhsDigest = this.digest(joinExpression.lhs, action);
        if (!lhsDigest) return null;
        return new JoinExpression({
          op: 'join',
          lhs: lhsDigest.expression,
          rhs: joinExpression.rhs
        });
      } else {
        var rhsDigest = this.digest(joinExpression.rhs, action);
        if (!rhsDigest) return null;
        return new JoinExpression({
          op: 'join',
          lhs: joinExpression.lhs,
          rhs: rhsDigest.expression
        });
      }
    }
    */

    public digest(expression: Expression, action: Action): Digest {
      if (expression instanceof LiteralExpression) {
        var external = expression.value;
        if (external instanceof External) {
          var newExternal = external.addAction(action);
          if (!newExternal) return null;
          return {
            undigested: null,
            expression: new LiteralExpression({
              op: 'literal',
              value: newExternal
            })
          };
        } else {
          return null;
        }

      /*
      } else if (expression instanceof JoinExpression) {
        var lhs = expression.lhs;
        var rhs = expression.rhs;
        if (lhs instanceof LiteralExpression && rhs instanceof LiteralExpression) {
          var lhsValue = lhs.value;
          var rhsValue = rhs.value;
          if (lhsValue instanceof External && rhsValue instanceof External) {
            var actionExpression = action.expression;

            if (action instanceof DefAction) {
              var actionDatasets = actionExpression.getExternalIds();
              if (actionDatasets.length !== 1) return null;
              newJoin = this._joinDigestHelper(expression, action);
              if (!newJoin) return null;
              return {
                expression: newJoin,
                undigested: null
              };

            } else if (action instanceof ApplyAction) {
              var actionDatasets = actionExpression.getExternalIds();
              if (!actionDatasets.length) return null;
              var newJoin: JoinExpression = null;
              if (actionDatasets.length === 1) {
                newJoin = this._joinDigestHelper(expression, action);
                if (!newJoin) return null;
                return {
                  expression: newJoin,
                  undigested: null
                };
              } else {
                var breakdown = actionExpression.breakdownByDataset('_br_');
                var singleDatasetActions = breakdown.singleDatasetActions;
                newJoin = expression;
                for (let i = 0; i < singleDatasetActions.length && newJoin; i++) {
                  newJoin = this._joinDigestHelper(newJoin, singleDatasetActions[i]);
                }
                if (!newJoin) return null;
                return {
                  expression: newJoin,
                  undigested: new ApplyAction({
                    action: 'apply',
                    name: (<ApplyAction>action).name,
                    expression: breakdown.combineExpression
                  })
                };
              }

            } else {
              return null;
            }
          } else {
            return null;
          }
        } else {
          return null;
        }
        */

      } else {
        throw new Error(`can not digest ${expression.op}`);
      }
    }

  }
}
