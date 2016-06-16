module Plywood {
  export function foldContext(d: Datum, c: Datum): Datum {
    var newContext = Object.create(c);
    for (var k in d) {
      newContext[k] = d[k];
    }
    return newContext;
  }

  export interface ComputeFn {
    (d: Datum, c: Datum, index?: number): any;
  }

  export interface SplitFns {
    [name: string]: ComputeFn;
  }

  export interface ComputePromiseFn {
    (d: Datum, c: Datum): Q.Promise<any>;
  }

  export interface DirectionFn {
    (a: any, b: any): number;
  }

  var directionFns: Lookup<DirectionFn> = {
    ascending: (a: any, b: any): number => {
      if (a == null) {
        return b == null ? 0 : -1;
      } else {
        if (a.compare) return a.compare(b);
        if (b == null) return 1;
      }
      return a < b ? -1 : a > b ? 1 : a >= b ? 0 : NaN;
    },
    descending: (a: any, b: any): number => {
      if (b == null) {
        return a == null ? 0 : -1;
      } else {
        if (b.compare) return b.compare(a);
        if (a == null) return 1;
      }
      return b < a ? -1 : b > a ? 1 : b >= a ? 0 : NaN;
    }
  };

  export interface Column {
    name: string;
    type: string;
    columns?: Column[];
  }

  function typePreference(type: string): number {
    switch (type) {
      case 'TIME': return 0;
      case 'STRING': return 1;
      case 'DATASET': return 5;
      default: return 2;
    }
  }

  function uniqueColumns(columns: Column[]): Column[] {
    var seen: Lookup<boolean> = {};
    var uniqueColumns: Column[] = [];
    for (var column of columns) {
      if (!seen[column.name]) {
        uniqueColumns.push(column);
        seen[column.name] = true;
      }
    }
    return uniqueColumns;
  }

  function flattenColumns(nestedColumns: Column[], prefixColumns: boolean): Column[] {
    var flatColumns: Column[] = [];
    var i = 0;
    var prefixString = '';
    while (i < nestedColumns.length) {
      var nestedColumn = nestedColumns[i];
      if (nestedColumn.type === 'DATASET') {
        nestedColumns = nestedColumn.columns;
        if (prefixColumns) prefixString += nestedColumn.name + '.';
        i = 0;
      } else {
        flatColumns.push({
          name: prefixString + nestedColumn.name,
          type: nestedColumn.type
        });
        i++;
      }
    }
    return uniqueColumns(flatColumns);
  }

  function removeLineBreaks(v: string): string {
    return v.replace(/(?:\r\n|\r|\n)/g, ' ');
  }

  var escapeFnCSV = (v: string) => {
    v = removeLineBreaks(v);
    if (v.indexOf('"') === -1 &&  v.indexOf(",") === -1) return v;
    return `"${v.replace(/"/g, '""')}"`;
  };

  var escapeFnTSV = (v: string) => {
    return removeLineBreaks(v).replace(/\t/g, "").replace(/"/g, '""');
  };

  var typeOrder: Lookup<number> = {
    'NULL': 0,
    'TIME': 1,
    'TIME_RANGE': 2,
    'SET/TIME': 3,
    'SET/TIME_RANGE': 4,
    'STRING': 5,
    'SET/STRING': 6,
    'BOOLEAN': 7,
    'NUMBER': 8,
    'NUMBER_RANGE': 9,
    'SET/NUMBER': 10,
    'SET/NUMBER_RANGE': 11,
    'DATASET': 12
  };

  export interface Formatter extends Lookup<Function> {
    'NULL'?: (v: any) => string;
    'TIME'?: (v: Date) => string;
    'TIME_RANGE'?: (v: TimeRange) => string;
    'SET/TIME'?: (v: Set) => string;
    'SET/TIME_RANGE'?: (v: Set) => string;
    'STRING'?: (v: string) => string;
    'SET/STRING'?: (v: Set) => string;
    'BOOLEAN'?: (v: boolean) => string;
    'NUMBER'?: (v: number) => string;
    'NUMBER_RANGE'?: (v: NumberRange) => string;
    'SET/NUMBER'?: (v: Set) => string;
    'SET/NUMBER_RANGE'?: (v: Set) => string;
    'DATASET'?: (v: Dataset) => string;
  }

  var defaultFormatter: Formatter = {
    'NULL': (v: any) => { return 'NULL'; },
    'TIME': (v: Date) => { return v.toISOString(); },
    'TIME_RANGE': (v: TimeRange) => { return '' + v; },
    'SET/TIME': (v: Set) => { return '' + v; },
    'SET/TIME_RANGE': (v: Set) => { return '' + v; },
    'STRING': (v: string) => { return '' + v; },
    'SET/STRING': (v: Set) => { return '' + v; },
    'BOOLEAN': (v: boolean) => { return '' + v; },
    'NUMBER': (v: number) => { return '' + v; },
    'NUMBER_RANGE': (v: NumberRange) => { return '' + v; },
    'SET/NUMBER': (v: Set) => { return '' + v; },
    'SET/NUMBER_RANGE': (v: Set) => { return '' + v; },
    'DATASET': (v: Dataset) => { return 'DATASET'; }
  };

  export interface FlattenOptions {
    prefixColumns?: boolean;
    order?: string; // preorder, inline [default], postorder
    nestingName?: string;
    parentName?: string;
  }

  export type FinalLineBreak = 'include' | 'suppress';

  export interface TabulatorOptions extends FlattenOptions {
    separator?: string;
    lineBreak?: string;
    finalLineBreak?: FinalLineBreak;
    formatter?: Formatter;
    finalizer?: (v: string) => string;
  }

  function isBoolean(b: any) {
    return b === true || b === false;
  }

  function isNumber(n: any) {
    return n !== null && !isNaN(Number(n));
  }

  function isString(str: string) {
    return typeof str === "string";
  }

  function getAttributeInfo(name: string, attributeValue: any): AttributeInfo {
    if (attributeValue == null) return null;
    if (isDate(attributeValue)) {
      return new AttributeInfo({ name, type: 'TIME' });
    } else if (isBoolean(attributeValue)) {
      return new AttributeInfo({ name, type: 'BOOLEAN' });
    } else if (isNumber(attributeValue)) {
      return new AttributeInfo({ name, type: 'NUMBER' });
    } else if (isString(attributeValue)) {
      return new AttributeInfo({ name, type: 'STRING' });
    } else if (NumberRange.isNumberRange(attributeValue)) {
      return new AttributeInfo({ name, type: 'NUMBER_RANGE' });
    } else if (TimeRange.isTimeRange(attributeValue)) {
      return new AttributeInfo({ name, type: 'TIME_RANGE' });
    } else if (Set.isSet(attributeValue)) {
      return new AttributeInfo({ name, type: attributeValue.getType() });
    } else if (Dataset.isDataset(attributeValue)) {
      return new AttributeInfo({ name, type: 'DATASET', datasetType: attributeValue.getFullType().datasetType });
    } else {
      throw new Error("Could not introspect");
    }
  }

  function datumFromJS(js: Datum): Datum {
    if (typeof js !== 'object') throw new TypeError("datum must be an object");

    var datum: Datum = Object.create(null);
    for (var k in js) {
      if (!hasOwnProperty(js, k)) continue;
      datum[k] = valueFromJS(js[k]);
    }

    return datum;
  }

  function datumToJS(datum: Datum): Datum {
    var js: Datum = {};
    for (var k in datum) {
      var v = datum[k];
      if (v && (v as any).suppress) continue;
      js[k] = valueToJSInlineType(v);
    }
    return js;
  }

  function joinDatums(datumA: Datum, datumB: Datum): Datum {
    var newDatum: Datum = Object.create(null);
    for (var k in datumA) {
      newDatum[k] = datumA[k];
    }
    for (var k in datumB) {
      newDatum[k] = datumB[k];
    }
    return newDatum;
  }

  function copy(obj: Lookup<any>): Lookup<any> {
    var newObj: Lookup<any> = {};
    var k: string;
    for (k in obj) {
      if (hasOwnProperty(obj, k)) newObj[k] = obj[k];
    }
    return newObj;
  }

  export interface DatasetValue {
    attributeOverrides?: Attributes;

    attributes?: Attributes;
    keys?: string[];
    data?: Datum[];
    suppress?: boolean;
  }

  export interface DatasetJS {
    attributes?: AttributeJSs;
    keys?: string[];
    data?: Datum[];
  }

  var check: Class<DatasetValue, any>;
  export class Dataset implements Instance<DatasetValue, any> {
    static type = 'DATASET';

    static isDataset(candidate: any): candidate is Dataset {
      return isInstanceOf(candidate, Dataset);
    }

    static getAttributesFromData(data: Datum[]): Attributes {
      if (!data.length) return [];

      var attributeNamesToIntrospect = Object.keys(data[0]);
      var attributes: Attributes = [];

      for (var datum of data) {
        var attributeNamesStillToIntrospect: string[] = [];
        for (var attributeNameToIntrospect of attributeNamesToIntrospect) {
          var attributeInfo = getAttributeInfo(attributeNameToIntrospect, datum[attributeNameToIntrospect]);
          if (attributeInfo) {
            attributes.push(attributeInfo);
          } else {
            attributeNamesStillToIntrospect.push(attributeNameToIntrospect);
          }
        }

        attributeNamesToIntrospect = attributeNamesStillToIntrospect;
        if (!attributeNamesToIntrospect.length) break;
      }

      // Assume all the remaining nulls are strings
      for (var attributeName of attributeNamesToIntrospect) {
        attributes.push(new AttributeInfo({ name: attributeName, type: 'STRING' }));
      }

      attributes.sort((a, b) => {
        var typeDiff = typeOrder[a.type] - typeOrder[b.type];
        if (typeDiff) return typeDiff;
        return a.name.localeCompare(b.name)
      });

      return attributes;
    }

    static fromJS(parameters: any): Dataset {
      if (Array.isArray(parameters)) {
        parameters = { data: parameters }
      }

      if (!Array.isArray(parameters.data)) {
        throw new Error('must have data')
      }

      var value: DatasetValue = {};

      if (hasOwnProperty(parameters, 'attributes')) {
        value.attributes = AttributeInfo.fromJSs(parameters.attributes);
      } else if (hasOwnProperty(parameters, 'attributeOverrides')) {
        value.attributeOverrides = AttributeInfo.fromJSs(parameters.attributeOverrides);
      }

      value.keys = parameters.keys;
      value.data = parameters.data.map(datumFromJS);
      return new Dataset(value)
    }

    public suppress: boolean;
    public attributes: Attributes = null;
    public keys: string[] = null;
    public data: Datum[];

    constructor(parameters: DatasetValue) {
      if (parameters.suppress === true) this.suppress = true;

      if (parameters.keys) {
        this.keys = parameters.keys;
      }
      var data = parameters.data;
      if (!Array.isArray(data)) {
        throw new TypeError("must have a `data` array");
      }
      this.data = data;

      var attributes = parameters.attributes;
      if (!attributes) attributes = Dataset.getAttributesFromData(data);

      var attributeOverrides = parameters.attributeOverrides;
      if (attributeOverrides) {
        attributes = AttributeInfo.override(attributes, attributeOverrides);
      }

      this.attributes = attributes;
    }

    public valueOf(): DatasetValue {
      var value: DatasetValue = {};
      if (this.suppress) value.suppress = true;
      if (this.attributes) value.attributes = this.attributes;
      if (this.keys) value.keys = this.keys;
      value.data = this.data;
      return value;
    }

    public toJS(): any {
      return this.data.map(datumToJS);
    }

    public toString(): string {
      return "Dataset(" + this.data.length + ")";
    }

    public toJSON(): any {
      return this.toJS();
    }

    public equals(other: Dataset): boolean {
      return Dataset.isDataset(other) &&
        this.data.length === other.data.length;
        // ToDo: probably add something else here?
    }

    public hide(): Dataset {
      var value = this.valueOf();
      value.suppress = true;
      return new Dataset(value);
    }

    public basis(): boolean {
      var data = this.data;
      return data.length === 1 && Object.keys(data[0]).length === 0;
    }

    public hasExternal(): boolean {
      if (!this.data.length) return false;
      return datumHasExternal(this.data[0]);
    }

    public getFullType(): DatasetFullType {
      var { attributes } = this;
      if (!attributes) throw new Error("dataset has not been introspected");

      var myDatasetType: Lookup<FullType> = {};
      for (var attribute of attributes) {
        var attrName = attribute.name;
        if (attribute.type === 'DATASET') {
          myDatasetType[attrName] = {
            type: 'DATASET',
            datasetType: attribute.datasetType
          };
        } else {
          myDatasetType[attrName] = {
            type: <PlyTypeSimple>attribute.type
          };
        }
      }

      return {
        type: 'DATASET',
        datasetType: myDatasetType
      };
    }

    // Actions
    public select(attrs: string[]): Dataset {
      var attributes = this.attributes;
      var newAttributes: Attributes = [];
      var attrLookup: Lookup<boolean> = Object.create(null);
      for (var attr of attrs) {
        attrLookup[attr] = true;
        var existingAttribute = helper.findByName(attributes, attr);
        if (existingAttribute) newAttributes.push(existingAttribute)
      }

      var data = this.data;
      var n = data.length;
      var newData = new Array(n);
      for (var i = 0; i < n; i++) {
        var datum = data[i];
        var newDatum = Object.create(null);
        for (let key in datum) {
          if (attrLookup[key]) {
            newDatum[key] = datum[key];
          }
        }
        newData[i] = newDatum;
      }

      var value = this.valueOf();
      value.attributes = newAttributes;
      value.data = newData;
      return new Dataset(value);
    }

    public apply(name: string, exFn: ComputeFn, type: PlyType, context: Datum): Dataset {
      var data = this.data;
      var n = data.length;
      var newData = new Array(n);
      for (var i = 0; i < n; i++) {
        var datum = data[i];
        var newDatum = Object.create(null);
        for (let key in datum) newDatum[key] = datum[key];
        newDatum[name] = exFn(datum, context, i);
        newData[i] = newDatum;
      }

      // Hack
      var datasetType: Lookup<FullType> = null;
      if (type === 'DATASET' && newData[0] && newData[0][name]) {
        datasetType = (newData[0][name] as any).getFullType().datasetType;
      }
      // End Hack

      var value = this.valueOf();
      value.attributes = helper.overrideByName(value.attributes, new AttributeInfo({ name, type, datasetType }));
      value.data = newData;
      return new Dataset(value);
    }

    public applyPromise(name: string, exFn: ComputePromiseFn, type: PlyType, context: Datum): Q.Promise<Dataset> {
      var value = this.valueOf();
      var promises = value.data.map(datum => exFn(datum, context));
      return Q.all(promises).then(values => {
        return this.apply(name, ((d, c, i) => values[i]), type, context);
      });
    }

    public filter(exFn: ComputeFn, context: Datum): Dataset {
      var value = this.valueOf();
      value.data = value.data.filter(datum => exFn(datum, context));
      return new Dataset(value);
    }

    public sort(exFn: ComputeFn, direction: string, context: Datum): Dataset {
      var value = this.valueOf();
      var directionFn = directionFns[direction];
      value.data = this.data.sort((a, b) => { // Note: this modifies the original, fix if needed
        return directionFn(exFn(a, context), exFn(b, context))
      });
      return new Dataset(value);
    }

    public limit(limit: number): Dataset {
      var data = this.data;
      if (data.length <= limit) return this;
      var value = this.valueOf();
      value.data = data.slice(0, limit);
      return new Dataset(value);
    }

    // Aggregators
    public count(): int {
      return this.data.length;
    }

    public sum(exFn: ComputeFn, context: Datum): number {
      var data = this.data;
      var sum = 0;
      for (let datum of data) {
        sum += exFn(datum, context);
      }
      return sum;
    }

    public average(exFn: ComputeFn, context: Datum): number {
      var count = this.count();
      return count ? (this.sum(exFn, context) / count) : null;
    }

    public min(exFn: ComputeFn, context: Datum): number {
      var data = this.data;
      var min = Infinity;
      for (let datum of data) {
        var v = exFn(datum, context);
        if (v < min) min = v;
      }
      return min;
    }

    public max(exFn: ComputeFn, context: Datum): number {
      var data = this.data;
      var max = -Infinity;
      for (let datum of data) {
        var v = exFn(datum, context);
        if (max < v) max = v;
      }
      return max;
    }

    public countDistinct(exFn: ComputeFn, context: Datum): number {
      var data = this.data;
      var seen: Lookup<number> = Object.create(null);
      var count = 0;
      for (let datum of data) {
        var v = exFn(datum, context);
        if (!seen[v]) {
          seen[v] = 1;
          ++count;
        }
      }
      return count;
    }

    public quantile(exFn: ComputeFn, quantile: number, context: Datum): number {
      var data = this.data;
      var vs: number[] = [];
      for (let datum of data) {
        var v = exFn(datum, context);
        if (v != null) vs.push(v);
      }

      vs.sort((a: number, b: number) => a - b);

      var n = vs.length;
      if (quantile === 0) return vs[0];
      if (quantile === 1) return vs[n - 1];
      var rank = n * quantile - 1;

      // Is the rank an integer?
      if (rank === Math.floor(rank)) {
        return (vs[rank] + vs[rank + 1]) / 2;
      } else {
        return vs[Math.ceil(rank)];
      }
    }

    public split(splitFns: SplitFns, datasetName: string, context: Datum): Dataset {
      var { data, attributes } = this;

      var keys = Object.keys(splitFns);
      var numberOfKeys = keys.length;
      var splitFnList = keys.map(k => splitFns[k]);

      var splits: Lookup<Datum> = {};
      var datumGroups: Lookup<Datum[]> = {};
      var finalData: Datum[] = [];
      var finalDataset: Datum[][] = [];

      function addDatum(datum: Datum, valueList: any): void {
        var key = valueList.join(';_PLYw00d_;');
        if (hasOwnProperty(datumGroups, key)) {
          datumGroups[key].push(datum);
        } else {
          var newDatum: Datum = Object.create(null);
          for (var i = 0; i < numberOfKeys; i++) {
            newDatum[keys[i]] = valueList[i];
          }
          finalDataset.push(datumGroups[key] = [datum]);
          splits[key] = newDatum;
          finalData.push(newDatum)
        }
      }

      for (var datum of data) {
        var valueList = splitFnList.map(splitFn => splitFn(datum, context));
        if (Set.isSet(valueList[0])) {
          if (valueList.length > 1) throw new Error('multi-dimensional set split is not implemented');
          var elements = valueList[0].elements;
          for (var element of elements) {
            addDatum(datum, [element]);
          }
        } else {
          addDatum(datum, valueList);
        }
      }

      for (var i = 0; i < finalData.length; i++) {
        finalData[i][datasetName] = new Dataset({
          suppress: true,
          attributes,
          data: finalDataset[i]
        });
      }

      return new Dataset({
        keys,
        data: finalData
      });
    }

    public introspect(): void {
      console.error('introspection is always done, `.introspect()` method never needs to be called');
    }

    public getExternals(): External[] {
      if (this.data.length === 0) return [];
      var datum = this.data[0];
      var externals: External[] = [];
      Object.keys(datum).forEach(applyName => {
        var applyValue = datum[applyName];
        if (applyValue instanceof Dataset) {
          externals.push(...applyValue.getExternals());
        }
      });
      return External.deduplicateExternals(externals);
    }

    public join(other: Dataset): Dataset {
      if (!other) return this;

      var thisKey = this.keys[0]; // ToDo: temp fix
      if (!thisKey) throw new Error('join lhs must have a key (be a product of a split)');
      var otherKey = other.keys[0]; // ToDo: temp fix
      if (!otherKey) throw new Error('join rhs must have a key (be a product of a split)');

      var thisData = this.data;
      var otherData = other.data;
      var k: string;

      var mapping: Lookup<Datum[]> = Object.create(null);
      for (var i = 0; i < thisData.length; i++) {
        let datum = thisData[i];
        k = String(thisKey ? datum[thisKey] : i);
        mapping[k] = [datum];
      }
      for (var i = 0; i < otherData.length; i++) {
        let datum = otherData[i];
        k = String(otherKey ? datum[otherKey] : i);
        if (!mapping[k]) mapping[k] = [];
        mapping[k].push(datum);
      }

      var newData: Datum[] = [];
      for (var j in mapping) {
        var datums = mapping[j];
        if (datums.length === 1) {
          newData.push(datums[0]);
        } else {
          newData.push(joinDatums(datums[0], datums[1]));
        }
      }
      return new Dataset({ data: newData });
    }

    public findDatumByAttribute(attribute: string, value: any): Datum {
      return helper.find(this.data, (d) => generalEqual(d[attribute], value));
    }

    public getNestedColumns(): Column[] {
      var nestedColumns: Column[] = [];
      var attributes = this.attributes;

      var subDatasetAdded: boolean = false;
      for (var attribute of attributes) {
        var column: Column = {
          name: attribute.name,
          type: attribute.type
        };
        if (attribute.type === 'DATASET') {
          var subDataset = this.data[0][attribute.name]; // ToDo: fix this!
          if (!subDatasetAdded && Dataset.isDataset(subDataset)) {
            subDatasetAdded = true;
            column.columns = subDataset.getNestedColumns();
            nestedColumns.push(column);
          }
        } else {
          nestedColumns.push(column);
        }
      }

      return nestedColumns;
    }

    public getColumns(options: FlattenOptions = {}): Column[] {
      var prefixColumns = options.prefixColumns;
      return flattenColumns(this.getNestedColumns(), prefixColumns);
    }

    private _flattenHelper(nestedColumns: Column[], prefix: string, order: string, nestingName: string, parentName: string, nesting: number, context: Datum, flat: PseudoDatum[]): void {
      var nestedColumnsLength = nestedColumns.length;
      if (!nestedColumnsLength) return;

      var data = this.data;
      var datasetColumn = nestedColumns.filter((nestedColumn) => nestedColumn.type === 'DATASET')[0];
      for (let datum of data) {
        var flatDatum: PseudoDatum = context ? copy(context) : {};
        if (nestingName) flatDatum[nestingName] = nesting;
        if (parentName) flatDatum[parentName] = context;

        for (let flattenedColumn of nestedColumns) {
          if (flattenedColumn.type === 'DATASET') continue;
          var flatName = (prefix !== null ? prefix : '') + flattenedColumn.name;
          flatDatum[flatName] = datum[flattenedColumn.name];
        }

        if (datasetColumn) {
          var nextPrefix: string = null;
          if (prefix !== null) nextPrefix = prefix + datasetColumn.name + '.';

          if (order === 'preorder') flat.push(flatDatum);
          (datum[datasetColumn.name] as Dataset)._flattenHelper(datasetColumn.columns, nextPrefix, order, nestingName, parentName, nesting + 1, flatDatum, flat);
          if (order === 'postorder') flat.push(flatDatum);
        }

        if (!datasetColumn) flat.push(flatDatum);
      }
    }

    public flatten(options: FlattenOptions = {}): PseudoDatum[] {
      var prefixColumns = options.prefixColumns;
      var order = options.order; // preorder, inline [default], postorder
      var nestingName = options.nestingName;
      var parentName = options.parentName;
      var nestedColumns = this.getNestedColumns();
      var flatData: PseudoDatum[] = [];
      if (nestedColumns.length) {
        this._flattenHelper(nestedColumns, (prefixColumns ? '' : null), order, nestingName, parentName, 0, null, flatData);
      }
      return flatData;
    }

    public toTabular(tabulatorOptions: TabulatorOptions): string {
      var formatter: Formatter = tabulatorOptions.formatter || {};
      var finalizer: (v: string) => string = tabulatorOptions.finalizer;
      var data = this.flatten(tabulatorOptions);
      var columns = this.getColumns(tabulatorOptions);

      var lines: string[] = [];
      lines.push(columns.map(c => c.name).join(tabulatorOptions.separator || ','));

      for (var i = 0; i < data.length; i++) {
        var datum = data[i];
        lines.push(columns.map(c => {
          var value = datum[c.name];
          var formatted = String((formatter[c.type] || defaultFormatter[c.type])(value));
          var finalized = formatted && finalizer ? finalizer(formatted) : formatted;
          return finalized;
        }).join(tabulatorOptions.separator || ','));
      }

      var lineBreak = tabulatorOptions.lineBreak || '\n';
      return lines.join(lineBreak) + (tabulatorOptions.finalLineBreak === 'include' && lines.length > 0 ? lineBreak : '');
    }

    public toCSV(tabulatorOptions: TabulatorOptions = {}): string {
      tabulatorOptions.finalizer = escapeFnCSV;
      tabulatorOptions.separator = tabulatorOptions.separator || ',';
      tabulatorOptions.lineBreak = tabulatorOptions.lineBreak || '\r\n';
      tabulatorOptions.finalLineBreak = tabulatorOptions.finalLineBreak || 'suppress';
      return this.toTabular(tabulatorOptions);
    }

    public toTSV(tabulatorOptions: TabulatorOptions = {}): string {
      tabulatorOptions.finalizer = escapeFnTSV;
      tabulatorOptions.separator = tabulatorOptions.separator || '\t';
      tabulatorOptions.lineBreak = tabulatorOptions.lineBreak || '\r\n';
      tabulatorOptions.finalLineBreak = tabulatorOptions.finalLineBreak || 'suppress';
      return this.toTabular(tabulatorOptions);
    }

  }
  check = Dataset;
}
