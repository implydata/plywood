module Plywood {
  export interface PostProcess {
    (result: any): PlywoodValue;
  }

  export interface NextFn<Q, R> {
    (prevQuery: Q, prevResult: R): Q
  }

  export interface QueryAndPostProcess<T> {
    query: T;
    postProcess: PostProcess;
    next?: NextFn<T, any>;
  }

  export interface Inflater {
    (d: Datum, i: number, data: Datum[]): void;
  }

  export type QueryMode = "raw" | "value" | "total" | "split";

  function nullMap<T, Q>(xs: T[], fn: (x: T) => Q): Q[] {
    if (!xs) return null;
    var res: Q[] = [];
    for (var x of xs) {
      var y = fn(x);
      if (y) res.push(y);
    }
    return res.length ? res : null;
  }

  function filterToAnds(filter: Expression): Expression[] {
    if (filter.equals(Expression.TRUE)) return [];
    return filter.getExpressionPattern('and') || [filter];
  }

  function filterDiff(strongerFilter: Expression, weakerFilter: Expression): Expression {
    var strongerFilterAnds = filterToAnds(strongerFilter);
    var weakerFilterAnds = filterToAnds(weakerFilter);
    if (weakerFilterAnds.length > strongerFilterAnds.length) return null;
    for (var i = 0; i < weakerFilterAnds.length; i++) {
      if (!(weakerFilterAnds[i].equals(strongerFilterAnds[i]))) return null;
    }
    return Expression.and(strongerFilterAnds.slice(weakerFilterAnds.length));
  }

  function getCommonFilter(filter1: Expression, filter2: Expression): Expression {
    var filter1Ands = filterToAnds(filter1);
    var filter2Ands = filterToAnds(filter2);
    var minLength = Math.min(filter1Ands.length, filter2Ands.length);
    var commonExpressions: Expression[] = [];
    for (var i = 0; i < minLength; i++) {
      if (!filter1Ands[i].equals(filter2Ands[i])) break;
      commonExpressions.push(filter1Ands[i]);
    }
    return Expression.and(commonExpressions);
  }

  function mergeDerivedAttributes(derivedAttributes1: Lookup<Expression>, derivedAttributes2: Lookup<Expression>): Lookup<Expression> {
    var derivedAttributes: Lookup<Expression> = Object.create(null);
    for (var k in derivedAttributes1) {
      derivedAttributes[k] = derivedAttributes1[k];
    }
    for (var k in derivedAttributes2) {
      if (hasOwnProperty(derivedAttributes, k) && !derivedAttributes[k].equals(derivedAttributes2[k])) {
        throw new Error(`can not currently redefine conflicting ${k}`);
      }
      derivedAttributes[k] = derivedAttributes2[k];
    }
    return derivedAttributes;
  }

  function getSampleValue(valueType: string, ex: Expression): PlywoodValue {
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
            end: timeBucketAction.duration.shift(start, timezone, 1)
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

  function findApplyByExpression(applies: ApplyAction[], expression: Expression): ApplyAction {
    for (let apply of applies) {
      if (apply.expression.equals(expression)) return apply;
    }
    return null;
  }

  export interface ExternalValue {
    engine?: string;
    version?: string;
    suppress?: boolean;
    rollup?: boolean;
    attributes?: Attributes;
    attributeOverrides?: Attributes;
    derivedAttributes?: Lookup<Expression>;
    delegates?: External[];
    concealBuckets?: boolean;
    mode?: QueryMode;
    dataName?: string;
    rawAttributes?: Attributes;
    filter?: Expression;
    valueExpression?: ChainExpression;
    select?: SelectAction;
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
    finalizers? : Druid.PostAggregation[];

    requester?: Requester.PlywoodRequester<any>;
  }

  export interface ExternalJS {
    engine: string;
    version?: string;
    rollup?: boolean;
    attributes?: AttributeJSs;
    attributeOverrides?: AttributeJSs;
    derivedAttributes?: Lookup<ExpressionJS>;
    filter?: ExpressionJS;
    rawAttributes?: AttributeJSs;
    concealBuckets?: boolean;

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
  }

  export interface ApplySegregation {
    aggregateApplies: ApplyAction[];
    postAggregateApplies: ApplyAction[];
  }

  export interface AttributesAndApplies {
    attributes?: Attributes;
    applies?: ApplyAction[];
  }

  export interface IntrospectResult {
    version: string;
    attributes: Attributes;
  }

  export class External {
    static type = 'EXTERNAL';

    static SEGMENT_NAME = '__SEGMENT__';
    static VALUE_NAME = '__VALUE__';

    static isExternal(candidate: any): candidate is External {
      return isInstanceOf(candidate, External);
    }

    static extractVersion(v: string): string {
      if (!v) return null;
      var m = v.match(/^\d+\.\d+\.\d+(?:-\w+)?/);
      return m ? m[0] : null;
    }

    static versionLessThan(va: string, vb: string): boolean {
      var pa = va.split('-')[0].split('.');
      var pb = vb.split('-')[0].split('.');
      if (pa[0] !== pb[0]) return pa[0] < pb[0];
      if (pa[1] !== pb[1]) return pa[1] < pb[1];
      return pa[2] < pb[2];
    }

    static deduplicateExternals(externals: External[]): External[] {
      if (externals.length < 2) return externals;
      var uniqueExternals = [externals[0]];

      function addToUniqueExternals(external: External) {
        for (let uniqueExternal of uniqueExternals) {
          if (uniqueExternal.equalBase(external)) return;
        }
        uniqueExternals.push(external);
      }

      for (let i = 1; i < externals.length; i++) addToUniqueExternals(externals[i]);
      return uniqueExternals;
    }

    static makeZeroDatum(applies: ApplyAction[]): Datum {
      var newDatum = Object.create(null);
      for (var apply of applies) {
        var applyName = apply.name;
        if (applyName[0] === '_') continue;
        newDatum[applyName] = 0;
      }
      return newDatum;
    }

    static normalizeAndAddApply(attributesAndApplies: AttributesAndApplies, apply: ApplyAction): AttributesAndApplies {
      var { attributes, applies } = attributesAndApplies;

      var expressions: Lookup<Expression> = Object.create(null);
      for (let existingApply of applies) expressions[existingApply.name] = existingApply.expression;
      apply = <ApplyAction>apply.changeExpression(apply.expression.resolveWithExpressions(expressions, 'leave').simplify());

      return {
        attributes: helper.overrideByName(attributes, new AttributeInfo({ name: apply.name, type: apply.expression.type })),
        applies: helper.overrideByName(applies, apply)
      }
    }

    static segregationAggregateApplies(applies: ApplyAction[]): ApplySegregation {
      var aggregateApplies: ApplyAction[] = [];
      var postAggregateApplies: ApplyAction[] = [];
      var nameIndex = 0;

      // First extract all the simple cases
      var appliesToSegregate: ApplyAction[] = [];
      for (let apply of applies) {
        var applyExpression = apply.expression;
        if (applyExpression instanceof ChainExpression) {
          var actions = applyExpression.actions;
          if (actions[actions.length - 1].isAggregate()) {
            // This is a vanilla aggregate, just push it in.
            aggregateApplies.push(apply);
            continue;
          }
        }
        appliesToSegregate.push(apply);
      }

      // Now do all the segregation
      for (let apply of appliesToSegregate) {
        var newExpression = apply.expression.substituteAction(
          (action) => {
            return action.isAggregate();
          },
          (preEx: Expression, action: Action) => {
            var aggregateChain = preEx.performAction(action);
            var existingApply = findApplyByExpression(aggregateApplies, aggregateChain);
            if (existingApply) {
              return $(existingApply.name, existingApply.expression.type);
            } else {
              var name = '!T_' + (nameIndex++);
              aggregateApplies.push(new ApplyAction({
                action: 'apply',
                name: name,
                expression: aggregateChain
              }));
              return $(name, aggregateChain.type);
            }
          }
        );

        postAggregateApplies.push(<ApplyAction>apply.changeExpression(newExpression));
      }

      return {
        aggregateApplies,
        postAggregateApplies
      };
    }

    static getCommonFilterFromExternals(externals: External[]): Expression {
      if (!externals.length) throw new Error('must have externals');
      var commonFilter = externals[0].filter;
      for (let i = 1; i < externals.length; i++) {
        commonFilter = getCommonFilter(commonFilter, externals[i].filter);
      }
      return commonFilter;
    }

    static getMergedDerivedAttributesFromExternals(externals: External[]): Lookup<Expression> {
      if (!externals.length) throw new Error('must have externals');
      var derivedAttributes = externals[0].derivedAttributes;
      for (let i = 1; i < externals.length; i++) {
        derivedAttributes = mergeDerivedAttributes(derivedAttributes, externals[i].derivedAttributes);
      }
      return derivedAttributes;
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

          case '0':
          case 'false':
            d[label] = false;
            break;

          case '1':
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
        d[label] = new TimeRange({ start, end: duration.shift(start, timezone) })
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

    static setStringInflaterFactory(label: string): Inflater  {
      return (d: any) => {
        var v = d[label];
        if ('' + v === "null") {
          d[label] = null;
          return;
        }

        if (typeof v === 'string') v = [v];
        d[label] = Set.fromJS({
          setType: 'STRING',
          elements: v
        });
      }
    }

    static jsToValue(parameters: ExternalJS, requester: Requester.PlywoodRequester<any>): ExternalValue {
      var value: ExternalValue = {
        engine: parameters.engine,
        version: parameters.version,
        suppress: true,
        rollup: parameters.rollup,
        concealBuckets: Boolean(parameters.concealBuckets),
        requester
      };
      if (parameters.attributes) {
        value.attributes = AttributeInfo.fromJSs(parameters.attributes);
      }
      if (parameters.attributeOverrides) {
        value.attributeOverrides = AttributeInfo.fromJSs(parameters.attributeOverrides);
      }
      if (parameters.derivedAttributes) {
        value.derivedAttributes = helper.expressionLookupFromJS(parameters.derivedAttributes);
      }

      value.filter = parameters.filter ? Expression.fromJS(parameters.filter) : Expression.TRUE;

      return value;
    }

    static classMap: Lookup<typeof External> = {};
    static register(ex: typeof External, id: string = null): void {
      if (!id) id = (<any>ex).name.replace('External', '').replace(/^\w/, (s: string) => s.toLowerCase());
      External.classMap[id] = ex;
    }

    static fromJS(parameters: ExternalJS, requester: Requester.PlywoodRequester<any> = null): External {
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

      // Back compat
      if (!requester && hasOwnProperty(parameters, 'requester')) {
        console.warn("'requester' parameter should be passed as context (2nd argument)");
        requester = (parameters as any).requester;
      }

      return ClassFn.fromJS(parameters, requester);
    }

    static fromValue(parameters: ExternalValue): External {
      const { engine } = parameters;
      const ClassFn = External.classMap[engine];
      if (!ClassFn) throw new Error(`unsupported engine '${engine}'`);
      return <External>(new ClassFn(parameters));
    }

    public engine: string;
    public version: string;
    public suppress: boolean;
    public rollup: boolean;
    public attributes: Attributes = null;
    public attributeOverrides: Attributes = null;
    public derivedAttributes: Lookup<Expression>;
    public delegates: External[];
    public concealBuckets: boolean;

    public rawAttributes: Attributes = null;
    public requester: Requester.PlywoodRequester<any>;
    public mode: QueryMode;
    public filter: Expression;
    public valueExpression: ChainExpression;
    public select: SelectAction;
    public split: SplitAction;
    public dataName: string;
    public applies: ApplyAction[];
    public sort: SortAction;
    public limit: LimitAction;
    public havingFilter: Expression;

    constructor(parameters: ExternalValue, dummy: Dummy = null) {
      if (dummy !== dummyObject) {
        throw new TypeError("can not call `new External` directly use External.fromJS instead");
      }
      this.engine = parameters.engine;

      var version: string = null;
      if (parameters.version) {
        version = External.extractVersion(parameters.version);
        if (!version) throw new Error(`invalid version ${parameters.version}`);
      }
      this.version = version;

      this.suppress = Boolean(parameters.suppress);
      this.rollup = Boolean(parameters.rollup);
      if (parameters.attributes) {
        this.attributes = parameters.attributes;
      }
      if (parameters.attributeOverrides) {
        this.attributeOverrides = parameters.attributeOverrides;
      }
      this.derivedAttributes = parameters.derivedAttributes || {};
      if (parameters.delegates) {
        this.delegates = parameters.delegates;
      }
      this.concealBuckets = parameters.concealBuckets;

      this.rawAttributes = parameters.rawAttributes;
      this.requester = parameters.requester;

      this.mode = parameters.mode || 'raw';
      this.filter = parameters.filter || Expression.TRUE;

      switch (this.mode) {
        case 'raw':
          this.select = parameters.select;
          this.sort = parameters.sort;
          this.limit = parameters.limit;
          break;

        case 'value':
          this.valueExpression = parameters.valueExpression;
          break;

        case 'total':
          this.applies = parameters.applies || [];
          break;

        case 'split':
          this.dataName = parameters.dataName;
          this.split = parameters.split;
          if (!this.split) throw new Error('must have split action in split mode');
          this.applies = parameters.applies || [];
          this.sort = parameters.sort;
          this.limit = parameters.limit;
          this.havingFilter = parameters.havingFilter || Expression.TRUE;
          break;
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

    protected _ensureMinVersion(minVersion: string) {
      if (this.version && External.versionLessThan(this.version, minVersion)) {
        throw new Error(`only ${this.engine} versions >= ${minVersion} are supported`);
      }
    }

    public valueOf(): ExternalValue {
      var value: ExternalValue = {
        engine: this.engine,
        version: this.version,
        rollup: this.rollup,
        mode: this.mode
      };
      if (this.suppress) value.suppress = this.suppress;
      if (this.attributes) value.attributes = this.attributes;
      if (this.attributeOverrides) value.attributeOverrides = this.attributeOverrides;
      if (helper.nonEmptyLookup(this.derivedAttributes)) value.derivedAttributes = this.derivedAttributes;
      if (this.delegates) value.delegates = this.delegates;
      value.concealBuckets = this.concealBuckets;

      if (this.rawAttributes) {
        value.rawAttributes = this.rawAttributes;
      }
      if (this.requester) {
        value.requester = this.requester;
      }

      if (this.dataName) {
        value.dataName = this.dataName;
      }
      value.filter = this.filter;
      if (this.valueExpression) {
        value.valueExpression = this.valueExpression;
      }
      if (this.select) {
        value.select = this.select;
      }
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
      if (this.version) js.version = this.version;
      if (this.rollup) js.rollup = true;
      if (this.attributes) js.attributes = AttributeInfo.toJSs(this.attributes);
      if (this.attributeOverrides) js.attributeOverrides = AttributeInfo.toJSs(this.attributeOverrides);
      if (helper.nonEmptyLookup(this.derivedAttributes)) js.derivedAttributes = helper.expressionLookupToJS(this.derivedAttributes);
      if (this.concealBuckets) js.concealBuckets = true;

      if (this.rawAttributes) js.rawAttributes = AttributeInfo.toJSs(this.rawAttributes);
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
          return `ExternalRaw(${this.filter})`;

        case 'value':
          return `ExternalValue(${this.valueExpression})`;

        case 'total':
          return `ExternalTotal(${this.applies.length})`;

        case 'split':
          return `ExternalSplit(${this.split}, ${this.applies.length})`;

        default:
          throw new Error(`unknown mode: ${this.mode}`);
      }

    }

    public equals(other: External): boolean {
      return this.equalBase(other) &&
        immutableLookupsEqual(this.derivedAttributes, other.derivedAttributes) &&
        immutableArraysEqual(this.delegates, other.delegates) &&
        this.concealBuckets === other.concealBuckets;
    }

    public equalBase(other: External): boolean {
      return External.isExternal(other) &&
        this.engine === other.engine &&
        this.version === other.version &&
        this.rollup === other.rollup &&
        this.mode === other.mode &&
        this.filter.equals(other.filter);
    }

    public attachRequester(requester: Requester.PlywoodRequester<any>): External {
      var value = this.valueOf();
      value.requester = requester;
      return External.fromValue(value);
    }

    public versionBefore(neededVersion: string): boolean {
      const { version } = this;
      return version && External.versionLessThan(version, neededVersion);
    }

    public getAttributesInfo(attributeName: string) {
      var attributes = this.rawAttributes || this.attributes;
      return helper.findByName(attributes, attributeName);
    }

    public updateAttribute(newAttribute: AttributeInfo): External {
      if (!this.attributes) return this;
      var value = this.valueOf();
      value.attributes = AttributeInfo.override(value.attributes, [newAttribute]);
      return External.fromValue(value);
    }

    public show(): External {
      var value = this.valueOf();
      value.suppress = false;
      return External.fromValue(value);
    }

    public hasAttribute(name: string): boolean {
      const { attributes, rawAttributes, derivedAttributes } = this;
      if (helper.find(rawAttributes || attributes, (a) => a.name === name)) return true;
      return hasOwnProperty(derivedAttributes, name);
    }

    public expressionDefined(ex: Expression): boolean {
      return ex.definedInTypeContext(this.getFullType());
    }

    public bucketsConcealed(ex: Expression) {
      return ex.every((ex, index, depth, nestDiff) => {
        if (nestDiff) return true;
        if (ex instanceof RefExpression) {
          var refAttributeInfo = this.getAttributesInfo(ex.name);
          if (refAttributeInfo && refAttributeInfo.makerAction) {
            return refAttributeInfo.makerAction.alignsWith([]);
          }

        } else if (ex instanceof ChainExpression) {
          var refExpression = ex.expression;
          if (refExpression instanceof RefExpression) {
            var ref = refExpression.name;
            var refAttributeInfo = this.getAttributesInfo(ref);
            if (refAttributeInfo && refAttributeInfo.makerAction) {
              return refAttributeInfo.makerAction.alignsWith(ex.actions);
            }
          }

        }
        return null;
      });
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

    public addDelegate(delegate: External): External {
      var value = this.valueOf();
      if (!value.delegates) value.delegates = [];
      value.delegates = value.delegates.concat(delegate);
      return External.fromValue(value);
    }

    public getBase(): External {
      var value = this.valueOf();
      value.suppress = true;
      value.mode = 'raw';
      value.dataName = null;
      if (this.mode !== 'raw') value.attributes = value.rawAttributes;
      value.rawAttributes = null;
      value.filter = null;
      value.applies = [];
      value.split = null;
      value.sort = null;
      value.limit = null;

      value.delegates = nullMap(value.delegates, (e) => e.getBase());
      return External.fromValue(value);
    }

    public getRaw(): External {
      if (this.mode === 'raw') return this;

      var value = this.valueOf();
      value.suppress = true;
      value.mode = 'raw';
      value.dataName = null;
      if (this.mode !== 'raw') value.attributes = value.rawAttributes;
      value.rawAttributes = null;
      value.applies = [];
      value.split = null;
      value.sort = null;
      value.limit = null;

      value.delegates = nullMap(value.delegates, (e) => e.getRaw());
      return External.fromValue(value);
    }

    public makeTotal(applies: ApplyAction[]): External {
      if (this.mode !== 'raw') return null;
      if (!this.canHandleTotal()) return null;

      if (!applies.length) throw new Error('must have applies');

      var externals: External[] = [];
      for (let apply of applies) {
        let applyExpression = apply.expression;
        if (applyExpression instanceof ExternalExpression) {
          externals.push(applyExpression.external);
        }
      }

      var commonFilter = External.getCommonFilterFromExternals(externals);

      var value = this.valueOf();
      value.mode = 'total';
      value.suppress = false;
      value.rawAttributes = value.attributes;
      value.derivedAttributes = External.getMergedDerivedAttributesFromExternals(externals);
      value.filter = commonFilter;
      value.attributes = [];
      value.applies = [];
      value.delegates = nullMap(value.delegates, (e) => e.makeTotal(applies));
      var totalExternal = External.fromValue(value);

      for (let apply of applies) {
        totalExternal = totalExternal._addApplyAction(apply);
        if (!totalExternal) return null;
      }

      return totalExternal;
    }

    public addAction(action: Action): External {
      if (action instanceof FilterAction) {
        return this._addFilterAction(action);
      }
      if (action instanceof SelectAction) {
        return this._addSelectAction(action);
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
      if (action.isAggregate()) {
        return this._addAggregateAction(action);
      }
      return this._addPostAggregateAction(action);
    }

    private _addFilterAction(action: FilterAction): External {
      return this.addFilter(action.expression);
    }

    public addFilter(expression: Expression): External {
      if (!expression.resolved()) return null;
      if (!this.expressionDefined(expression)) return null;

      var value = this.valueOf();
      switch (this.mode) {
        case 'raw':
          if (this.concealBuckets && !this.bucketsConcealed(expression)) return null;
          if (!this.canHandleFilter(expression)) return null;
          if (value.filter.equals(Expression.TRUE)) {
            value.filter = expression;
          } else {
            value.filter = value.filter.and(expression);
          }
          break;

        case 'split':
          if (!this.canHandleHavingFilter(expression)) return null;
          value.havingFilter = value.havingFilter.and(expression).simplify();
          break;

        default:
          return null; // can not add filter in total mode
      }

      value.delegates = nullMap(value.delegates, (e) => e.addFilter(expression));
      return External.fromValue(value);
    }

    private _addSelectAction(selectAction: SelectAction): External {
      if (this.mode !== 'raw') return null; // Can only select on 'raw' datasets

      const { datasetType } = this.getFullType();
      const { attributes } = selectAction;
      for (var attribute of attributes) {
        if (!datasetType[attribute]) return null;
      }

      var value = this.valueOf();
      value.suppress = false;
      value.select = selectAction;
      value.delegates = nullMap(value.delegates, (e) => e._addSelectAction(selectAction));
      return External.fromValue(value);
    }

    private _addSplitAction(splitAction: SplitAction): External {
      if (this.mode !== 'raw') return null; // Can only split on 'raw' datasets
      var splitKeys = splitAction.keys;
      for (var splitKey of splitKeys) {
        var splitExpression = splitAction.splits[splitKey];
        if (!this.expressionDefined(splitExpression)) return null;
        if (this.concealBuckets && !this.bucketsConcealed(splitExpression)) return null;
        if (!this.canHandleSplit(splitExpression)) return null;
      }

      var value = this.valueOf();
      value.suppress = false;
      value.mode = 'split';
      value.dataName = splitAction.dataName;
      value.split = splitAction;
      value.rawAttributes = value.attributes;
      value.attributes = splitAction.mapSplits((name, expression) => new AttributeInfo({ name, type: expression.type }));
      value.delegates = nullMap(value.delegates, (e) => e._addSplitAction(splitAction));
      return External.fromValue(value);
    }

    private _addApplyAction(action: ApplyAction): External {
      var expression = action.expression;
      if (expression.type === 'DATASET') return null;
      if (!expression.contained()) return null;
      if (!this.expressionDefined(expression)) return null;
      if (!this.canHandleApply(action.expression)) return null;

      if (this.mode === 'raw') {
        var value = this.valueOf();
        value.derivedAttributes = immutableAdd(
          value.derivedAttributes, action.name, action.expression
        );
      } else {
        // Can not redefine index for now.
        if (this.split && this.split.hasKey(action.name)) return null;

        var actionExpression = action.expression;
        if (actionExpression instanceof ExternalExpression) {
          action = <ApplyAction>action.changeExpression(actionExpression.external.valueExpressionWithinFilter(this.filter));
        }

        var value = this.valueOf();
        var added = External.normalizeAndAddApply(value, action);
        value.applies = added.applies;
        value.attributes = added.attributes;
      }
      value.delegates = nullMap(value.delegates, (e) => e._addApplyAction(action));
      return External.fromValue(value);
    }

    private _addSortAction(action: SortAction): External {
      if (this.limit) return null; // Can not sort after limit
      if (!this.canHandleSort(action)) return null;

      var value = this.valueOf();
      value.sort = action;
      value.delegates = nullMap(value.delegates, (e) => e._addSortAction(action));
      return External.fromValue(value);
    }

    private _addLimitAction(action: LimitAction): External {
      if (!this.canHandleLimit(action)) return null;

      var value = this.valueOf();
      value.suppress = false;
      if (!value.limit || action.limit < value.limit.limit) {
        value.limit = action;
      }
      value.delegates = nullMap(value.delegates, (e) => e._addLimitAction(action));
      return External.fromValue(value);
    }

    private _addAggregateAction(action: Action): External {
      if (this.mode !== 'raw' || this.limit) return null; // Can not value aggregate something with a limit
      var actionExpression = action.expression;
      if (actionExpression && !this.expressionDefined(actionExpression)) return null;

      var value = this.valueOf();
      value.mode = 'value';
      value.suppress = false;
      value.valueExpression = $(External.SEGMENT_NAME, 'DATASET').performAction(action);
      value.rawAttributes = value.attributes;
      value.attributes = null;
      value.delegates = nullMap(value.delegates, (e) => e._addAggregateAction(action));
      return External.fromValue(value);
    }

    private _addPostAggregateAction(action: Action): External {
      if (this.mode !== 'value') throw new Error('must be in value mode to call addPostAggregateAction');
      var actionExpression = action.expression;
      // ToDo: do I need to run  expressionDefined here?

      var commonFilter = this.filter;
      var newValueExpression: ChainExpression;
      if (actionExpression instanceof ExternalExpression) {
        var otherExternal = actionExpression.external;
        if (!this.getBase().equals(otherExternal.getBase())) return null;

        var commonFilter = getCommonFilter(commonFilter, otherExternal.filter);
        var newAction = action.changeExpression(otherExternal.valueExpressionWithinFilter(commonFilter));
        newValueExpression = this.valueExpressionWithinFilter(commonFilter).performAction(newAction);

      } else if (!actionExpression || !actionExpression.hasExternal()) {
        newValueExpression = this.valueExpression.performAction(action);

      } else {
        return null;
      }

      var value = this.valueOf();
      value.valueExpression = newValueExpression;
      value.filter = commonFilter;
      value.delegates = nullMap(value.delegates, (e) => e._addPostAggregateAction(action));
      return External.fromValue(value);
    }

    public prePack(prefix: Expression, myAction: Action): External {
      if (this.mode !== 'value') throw new Error('must be in value mode to call prePack');

      var value = this.valueOf();
      value.valueExpression = prefix.performAction(myAction.changeExpression(value.valueExpression));
      value.delegates = nullMap(value.delegates, (e) => e.prePack(prefix, myAction));
      return External.fromValue(value);
    }

    // ----------------------

    public valueExpressionWithinFilter(withinFilter: Expression): ChainExpression {
      if (this.mode !== 'value') return null;
      var extraFilter = filterDiff(this.filter, withinFilter);
      if (!extraFilter) throw new Error('not within the segment');

      var ex = this.valueExpression;
      if (!extraFilter.equals(Expression.TRUE)) {
        ex = <ChainExpression>ex.substitute(ex => {
          if (ex instanceof RefExpression && ex.type === 'DATASET' && ex.name === External.SEGMENT_NAME) {
            return ex.filter(extraFilter);
          }
          return null;
        })
      }

      return ex;
    }

    public toValueApply(): ApplyAction {
      if (this.mode !== 'value') return null;
      return new ApplyAction({
        name: External.VALUE_NAME,
        expression: this.valueExpression
      });
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

    public inlineDerivedAttributes(expression: Expression): Expression {
      const { derivedAttributes } = this;
      return expression.substitute(refEx => {
        if (refEx instanceof RefExpression) {
          var refName = refEx.name;
          return hasOwnProperty(derivedAttributes, refName) ? derivedAttributes[refName] : null;
        } else {
          return null;
        }
      })
    }

    public inlineDerivedAttributesInAggregate(expression: Expression): Expression {
      const { derivedAttributes } = this;
      return expression.substituteAction(
        (action) => {
          if (!action.isAggregate()) return false;
          return action.getFreeReferences().some(ref => hasOwnProperty(derivedAttributes, ref));
        },
        (preEx, action) => {
          return preEx.performAction(action.changeExpression(this.inlineDerivedAttributes(action.expression)));
        }
      );
    }

    public switchToRollupCount(expression: Expression): Expression {
      if (!this.rollup) return expression;

      var countRef: RefExpression = null;
      return expression.substituteAction(
        (action) => {
          return action.action === 'count';
        },
        (preEx) => {
          if (!countRef) countRef = $(this.getRollupCountName(), 'NUMBER');
          return preEx.sum(countRef);
        }
      );
    }

    public getRollupCountName(): string {
      const { rawAttributes } = this;
      for (var attribute of rawAttributes) {
        var makerAction = attribute.makerAction;
        if (makerAction && makerAction.action === 'count') return attribute.name;
      }
      throw new Error(`could not find rollup count`);
    }

    public getQuerySplit(): SplitAction {
      return this.split.transformExpressions((ex) => {
        return this.inlineDerivedAttributes(ex);
      })
    }

    public getQueryFilter(): Expression {
      return this.inlineDerivedAttributes(this.filter).simplify();
    }

    public getSelectedAttributes(): Attributes {
      var { select, attributes, derivedAttributes } = this;
      attributes = attributes.slice();
      for (var k in derivedAttributes) {
        attributes.push(new AttributeInfo({ name: k, type: derivedAttributes[k].type }));
      }
      if (!select) return attributes;
      const selectAttributes = select.attributes;
      return attributes.filter(a => selectAttributes.indexOf(a.name) !== -1);
    }

    // -----------------

    public addNextExternal(dataset: Dataset): Dataset {
      const { mode, dataName, split } = this;
      if (mode !== 'split') throw new Error('must be in split mode to addNextExternal');
      return dataset.apply(dataName, (d: Datum) => {
        return this.getRaw().addFilter(split.filterFromDatum(d));
      }, 'DATASET', null);
    }

    public getDelegate(): External {
      const { mode, delegates } = this;
      if (!delegates || !delegates.length || mode === 'raw') return null;
      return delegates[0];
    }

    public simulateValue(lastNode: boolean, simulatedQueries: any[], externalForNext: External = null): PlywoodValue {
      const { mode } = this;

      if (!externalForNext) externalForNext = this;

      var delegate = this.getDelegate();
      if (delegate) {
        return delegate.simulateValue(lastNode, simulatedQueries, externalForNext);
      }

      simulatedQueries.push(this.getQueryAndPostProcess().query);

      if (mode === 'value') {
        var valueExpression = this.valueExpression;
        return getSampleValue(valueExpression.type, valueExpression);
      }

      var datum: Datum = {};

      if (mode === 'raw') {
        var attributes = this.attributes;
        for (let attribute of attributes) {
          datum[attribute.name] = getSampleValue(attribute.type, null);
        }
      } else {
        if (mode === 'split') {
          this.split.mapSplits((name, expression) => {
            datum[name] = getSampleValue(unwrapSetType(expression.type), expression);
          });
        }

        var applies = this.applies;
        for (let apply of applies) {
          datum[apply.name] = getSampleValue(apply.expression.type, apply.expression);
        }
      }

      var dataset = new Dataset({ data: [datum] });
      if (!lastNode && mode === 'split') dataset = externalForNext.addNextExternal(dataset);
      return dataset;
    }

    public getQueryAndPostProcess(): QueryAndPostProcess<any> {
      throw new Error("can not call getQueryAndPostProcess directly");
    }

    public queryValue(lastNode: boolean, externalForNext: External = null): Q.Promise<PlywoodValue> {
      const { mode, requester } = this;

      if (!externalForNext) externalForNext = this;

      var delegate = this.getDelegate();
      if (delegate) {
        return delegate.queryValue(lastNode, externalForNext);
      }

      if (!requester) {
        return <Q.Promise<PlywoodValue>>Q.reject(new Error('must have a requester to make queries'));
      }
      try {
        var queryAndPostProcess = this.getQueryAndPostProcess();
      } catch (e) {
        return <Q.Promise<PlywoodValue>>Q.reject(e);
      }

      var { query, postProcess, next } = queryAndPostProcess;
      if (!query || typeof postProcess !== 'function') {
        return <Q.Promise<PlywoodValue>>Q.reject(new Error('no query or postProcess'));
      }

      var finalResult: Q.Promise<PlywoodValue>;
      if (next) {
        var results: any[] = [];
        finalResult = helper.promiseWhile(
          () => query,
          () => {
            return requester({ query })
              .then((result) => {
                results.push(result);
                query = next(query, result);
              })
          }
        )
          .then(() => {
            return queryAndPostProcess.postProcess(results);
          })
      } else {
        finalResult = requester({ query })
          .then(queryAndPostProcess.postProcess);
      }

      if (!lastNode && mode === 'split') {
        finalResult = <Q.Promise<PlywoodValue>>finalResult.then(externalForNext.addNextExternal.bind(externalForNext));
      }

      return finalResult;
    }

    // -------------------------

    public needsIntrospect(): boolean {
      return !this.attributes;
    }

    public getIntrospectAttributes(): Q.Promise<IntrospectResult> {
      throw new Error("can not call getIntrospectAttributes directly");
    }

    public introspect(): Q.Promise<External> {
      if (!this.requester) {
        return <Q.Promise<External>>Q.reject(new Error('must have a requester to introspect'));
      }

      return this.getIntrospectAttributes()
        .then(({version, attributes}) => {
          var value = this.valueOf();

          // Apply user provided (if any) overrides to the received attributes
          if (value.attributeOverrides) {
            attributes = AttributeInfo.override(attributes, value.attributeOverrides);
          }

          // Override any existing attributes (we do not just replace them)
          if (value.attributes) {
            attributes = AttributeInfo.override(value.attributes, attributes);
          }

          if (version) value.version = version;
          value.attributes = attributes;
          // Once attributes are set attributeOverrides will be ignored
          return External.fromValue(value);
        });
    }

    public getRawDatasetType(): Lookup<FullType> {
      var { attributes, rawAttributes, derivedAttributes } = this;
      if (!attributes) throw new Error("dataset has not been introspected");

      if (!rawAttributes) rawAttributes = attributes

      var myDatasetType: Lookup<FullType> = {};
      for (var rawAttribute of rawAttributes) {
        var attrName = rawAttribute.name;
        myDatasetType[attrName] = {
          type: <PlyTypeSimple>rawAttribute.type
        };
      }

      for (var name in derivedAttributes) {
        myDatasetType[name] = {
          type: <PlyTypeSimple>derivedAttributes[name].type
        };
      }

      return myDatasetType;
    }

    public getFullType(): DatasetFullType {
      const { mode, attributes } = this;

      if (mode === 'value') throw new Error('not supported for value mode yet');
      var myDatasetType = this.getRawDatasetType();

      if (mode !== 'raw') {
        var splitDatasetType: Lookup<FullType> = {};
        splitDatasetType[this.dataName || External.SEGMENT_NAME] = {
          type: 'DATASET',
          datasetType: myDatasetType,
          remote: true
        };

        for (var attribute of attributes) {
          var attrName = attribute.name;
          splitDatasetType[attrName] = {
            type: <PlyTypeSimple>attribute.type
          };
        }

        myDatasetType = splitDatasetType;
      }

      return {
        type: 'DATASET',
        datasetType: myDatasetType,
        remote: true
      };
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

  }
}
