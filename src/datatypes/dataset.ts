module Plywood {
  export function foldContext(d: Datum, c: Datum): Datum {
    var newContext = Object.create(c);
    for (var k in d) {
      newContext[k] = d[k];
    }
    return newContext;
  }

  export interface ComputeFn {
    (d: Datum, c: Datum): any;
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

  function sortColumns(columns: Column[]): Column[] {
    return columns.sort((c1, c2) => {
      var typeDiff = typePreference(c1.type) - typePreference(c2.type);
      if (typeDiff) return typeDiff;
      return c1.name.localeCompare(c2.name);
    });
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
    return sortColumns(uniqueColumns(flatColumns));
  }

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
    'TIME_RANGE': (v: TimeRange) => { return String(v) },
    'SET/TIME': (v: Set) => { return String(v); },
    'SET/TIME_RANGE': (v: Set) => { return String(v); },
    'STRING': (v: string) => {
      v = '' + v;
      if (v.indexOf('"') === -1) return v;
      return '"' + v.replace(/"/g, '""') + '"';
    },
    'SET/STRING': (v: Set) => { return String(v); },
    'BOOLEAN': (v: boolean) => { return String(v); },
    'NUMBER': (v: number) => { return String(v); },
    'NUMBER_RANGE': (v: NumberRange) => { return String(v); },
    'SET/NUMBER': (v: Set) => { return String(v); },
    'SET/NUMBER_RANGE': (v: Set) => { return String(v); },
    'DATASET': (v: Dataset) => { return 'DATASET'; }
  };

  export interface FlattenOptions {
    prefixColumns?: boolean;
    order?: string; // preorder, inline [default], postorder
    nestingName?: string;
    parentName?: string;
  }

  export interface TabulatorOptions extends FlattenOptions {
    separator?: string;
    lineBreak?: string;
    formatter?: Formatter;
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
      if (v && v.suppress === true) continue;
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
    attributes?: Attributes;
    attributeOverrides?: Attributes;
    keys?: string[];
    data?: Datum[];
    suppress?: boolean;
  }

  export interface DatasetJS {
    attributes?: AttributeJSs;
    attributeOverrides?: AttributeJSs;
    keys?: string[];
    data?: Datum[];
  }

  var check: Class<DatasetValue, any>;
  export class Dataset implements Instance<DatasetValue, any> {
    static type = 'DATASET';

    static isDataset(candidate: any): boolean {
      return isInstanceOf(candidate, Dataset);
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
    public attributeOverrides: Attributes = null;
    public keys: string[] = null;
    public data: Datum[];

    constructor(parameters: DatasetValue) {
      if (parameters.suppress === true) this.suppress = true;
      if (parameters.attributes) {
        this.attributes = parameters.attributes;
      }
      if (parameters.attributeOverrides) {
        this.attributeOverrides = parameters.attributeOverrides;
      }
      if (parameters.keys) {
        this.keys = parameters.keys;
      }
      var data = parameters.data;
      if (Array.isArray(data)) {
        this.data = data;
      } else {
        throw new TypeError("must have a `data` array");
      }
    }

    public valueOf(): DatasetValue {
      var value: DatasetValue = {};
      if (this.suppress) value.suppress = true;
      if (this.attributes) value.attributes = this.attributes;
      if (this.attributeOverrides) value.attributeOverrides = this.attributeOverrides;
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

    public getFullType(): FullType {
      this.introspect();
      var attributes = this.attributes;
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
            type: attribute.type
          };
        }
      }
      var myFullType: FullType = {
        type: 'DATASET',
        datasetType: myDatasetType
      };
      return myFullType;
    }

    // Actions
    public apply(name: string, exFn: ComputeFn, context: Datum): Dataset {
      // Note this works in place, fix that later if needed.
      var data = this.data;
      for (let datum of data) {
        datum[name] = exFn(datum, context);
      }
      this.attributes = null; // Since we did the change in place, blow out the attributes
      return this;
    }

    public applyPromise(name: string, exFn: ComputePromiseFn, context: Datum): Q.Promise<Dataset> {
      // Note this works in place, fix that later if needed.
      var ds = this;
      var promises = this.data.map(datum => exFn(datum, context));
      return Q.all(promises).then(values => {
        var data = ds.data;
        var n = data.length;
        for (var i = 0; i < n; i++) data[i][name] = values[i];
        this.attributes = null; // Since we did the change in place, blow out the attributes
        return ds; // ToDo: make a new Dataset here
      });
    }

    public filter(exFn: ComputeFn, context: Datum): Dataset {
      var value = this.valueOf();
      value.data = this.data.filter(datum => exFn(datum, context));
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
      for (let datum of data) {
        seen[exFn(datum, context)] = 1;
      }
      return Object.keys(seen).length;
    }

    public quantile(exFn: ComputeFn, quantile: number, context: Datum): number {
      return quantile; // ToDo fill this in
    }

    public split(splitFns: SplitFns, datasetName: string, context: Datum): Dataset {
      var data = this.data;

      var keys = Object.keys(splitFns);
      var numberOfKeys = keys.length;
      var splitFnList = keys.map(k => splitFns[k]);

      var splits: Lookup<Datum> = {};
      var datas: Lookup<Datum[]> = {};
      var finalData: Datum[] = [];

      function addDatum(datum: Datum, valueList: any): void {
        var key = valueList.join(';_PLYw00d_;');
        if (hasOwnProperty(datas, key)) {
          datas[key].push(datum);
        } else {
          var newDatum = Object.create(null);
          for (var i = 0; i < numberOfKeys; i++) {
            newDatum[keys[i]] = valueList[i];
          }
          newDatum[datasetName] = (datas[key] = [datum]);
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

      for (var finalDatum of finalData) {
        finalDatum[datasetName] = new Dataset({ suppress: true, data: finalDatum[datasetName] });
      }

      return new Dataset({
        keys,
        data: finalData
      });
    }

    // Introspection
    public introspect(): void {
      if (this.attributes) return;

      var data = this.data;
      if (!data.length) {
        this.attributes = [];
        return;
      }

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

      var attributeOverrides = this.attributeOverrides;
      if (attributeOverrides) {
        attributes = AttributeInfo.applyOverrides(attributes, attributeOverrides);
        this.attributeOverrides = null;
      }

      // ToDo: make this immutable so it matches the rest of the code   [+1]
      this.attributes = attributes;
    }

    public getExternals(): External[] {
      if (this.data.length === 0) return [];
      var datum = this.data[0];
      var externals: External[][] = [];
      Object.keys(datum).forEach(applyName => {
        var applyValue = datum[applyName];
        if (applyValue instanceof Dataset) {
          externals.push(applyValue.getExternals());
        }
      });
      return mergeExternals(externals);
    }

    public getExternalIds(): string[] {
      if (this.data.length === 0) return [];
      var datum = this.data[0];
      var push = Array.prototype.push;
      var externalIds: string[] = [];
      Object.keys(datum).forEach(applyName => {
        var applyValue = datum[applyName];
        if (applyValue instanceof Dataset) {
          push.apply(externalIds, applyValue.getExternals());
        }
      });
      return deduplicateSort(externalIds);
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

    public getNestedColumns(): Column[] {
      this.introspect();
      var nestedColumns: Column[] = [];
      var attributes = this.attributes;

      var subDatasetAdded: boolean = false;
      for (var attribute of attributes) {
        var column: Column = {
          name: attribute.name,
          type: attribute.type
        };
        if (attribute.type === 'DATASET') {
          if (!subDatasetAdded) {
            subDatasetAdded = true;
            column.columns = this.data[0][attribute.name].getNestedColumns();
            nestedColumns.push(column);
          }
        } else {
          nestedColumns.push(column);
        }
      }

      return nestedColumns.sort((a, b) => {
        var typeDiff = typeOrder[a.type] - typeOrder[b.type];
        if (typeDiff) return typeDiff;
        return a.name.localeCompare(b.name);
      });
    }

    public getColumns(options: FlattenOptions = {}): Column[] {
      var prefixColumns = options.prefixColumns;
      return flattenColumns(this.getNestedColumns(), prefixColumns);
    }

    private _flattenHelper(nestedColumns: Column[], prefix: string, order: string, nestingName: string, parentName: string, nesting: number, context: Datum, flat: Datum[]): void {
      var data = this.data;
      var leaf = nestedColumns[nestedColumns.length - 1].type !== 'DATASET';
      for (let datum of data) {
        var flatDatum: Datum = context ? copy(context) : {};
        if (nestingName) flatDatum[nestingName] = nesting;
        if (parentName) flatDatum[parentName] = context;

        for (let flattenedColumn of nestedColumns) {
          if (flattenedColumn.type === 'DATASET') {
            var nextPrefix: string = null;
            if (prefix !== null) nextPrefix = prefix + flattenedColumn.name + '.';

            if (order === 'preorder') flat.push(flatDatum);
            datum[flattenedColumn.name]._flattenHelper(flattenedColumn.columns, nextPrefix, order, nestingName, parentName, nesting + 1, flatDatum, flat);
            if (order === 'postorder') flat.push(flatDatum);
          } else {
            var flatName = (prefix !== null ? prefix : '') + flattenedColumn.name;
            flatDatum[flatName] = datum[flattenedColumn.name];
          }
        }

        if (leaf) flat.push(flatDatum);
      }
    }

    public flatten(options: FlattenOptions = {}): Datum[] {
      var prefixColumns = options.prefixColumns;
      var order = options.order; // preorder, inline [default], postorder
      var nestingName = options.nestingName;
      var parentName = options.parentName;
      var nestedColumns = this.getNestedColumns();
      var flatData: Datum[] = [];
      if (nestedColumns.length) {
        this._flattenHelper(nestedColumns, (prefixColumns ? '' : null), order, nestingName, parentName, 0, null, flatData);
      }
      return flatData;
    }

    public toTabular(tabulatorOptions: TabulatorOptions): string {
      var formatter: Formatter = tabulatorOptions.formatter || {};
      var data = this.flatten(tabulatorOptions);
      var columns = this.getColumns(tabulatorOptions);

      var lines: string[] = [];
      lines.push(columns.map(c => c.name).join(tabulatorOptions.separator || ','));

      for (var i = 0; i < data.length; i++) {
        var datum = data[i];
        lines.push(columns.map(c => {
          return String((formatter[c.type] || defaultFormatter[c.type])(datum[c.name]));
        }).join(tabulatorOptions.separator || ','));
      }

      var lineBreak = tabulatorOptions.lineBreak || '\n';
      return lines.join(lineBreak) + (lines.length > 0 ? lineBreak : '');
    }

    public toCSV(tabulatorOptions: TabulatorOptions = {}): string {
      tabulatorOptions.separator = tabulatorOptions.separator || ',';
      tabulatorOptions.lineBreak = tabulatorOptions.lineBreak || '\r\n';
      return this.toTabular(tabulatorOptions);
    }

    public toTSV(tabulatorOptions: TabulatorOptions = {}): string {
      tabulatorOptions.separator = tabulatorOptions.separator || '\t';
      tabulatorOptions.lineBreak = tabulatorOptions.lineBreak || '\r\n';
      return this.toTabular(tabulatorOptions);
    }

  }
  check = Dataset;
}
