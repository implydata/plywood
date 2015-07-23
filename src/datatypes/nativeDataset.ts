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

  export interface ComputePromiseFn {
    (d: Datum, c: Datum): Q.Promise<any>;
  }

  export interface DirectionFn {
    (a: any, b: any): number;
  }

  var directionFns: Lookup<DirectionFn> = {
    ascending: (a: any, b: any): number => {
      if (a.compare) return a.comapre(b);
      return a < b ? -1 : a > b ? 1 : a >= b ? 0 : NaN;
    },
    descending: (a: any, b: any): number => {
      if (b.compare) return b.comapre(a);
      return b < a ? -1 : b > a ? 1 : b >= a ? 0 : NaN;
    }
  };

  export interface Column {
    name: string;
    type: string;
    columns?: OrderedColumns;
  }

  export type OrderedColumns = Column[];
  
  export interface FlatData {
    columns: OrderedColumns;
    data: Datum[];
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

  export interface TabulatorOptions {
    separator?: string;
    lineBreak?: string;
    formatter?: Formatter;
  }

  function isDate(dt: any) {
    return Boolean(dt.toISOString)
  }

  function isNumber(n: any) {
    return !isNaN(Number(n));
  }

  function isString(str: string) {
    return typeof str === "string";
  }

  function getAttributeInfo(attributeValue: any): AttributeInfo {
    if (isDate(attributeValue)) {
      return new AttributeInfo({ type: 'TIME' });
    } else if (isNumber(attributeValue)) {
      return new AttributeInfo({ type: 'NUMBER' });
    } else if (isString(attributeValue)) {
      return new AttributeInfo({ type: 'STRING' });
    } else if (attributeValue instanceof Dataset) {
      return new AttributeInfo(attributeValue.getFullType())
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
      if (v.suppress === true) continue;
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

  export class NativeDataset extends Dataset {
    static type = 'DATASET';

    static fromJS(datasetJS: any): NativeDataset {
      var value = Dataset.jsToValue(datasetJS);
      value.data = datasetJS.data.map(datumFromJS);
      return new NativeDataset(value)
    }

    public data: Datum[];

    constructor(parameters: DatasetValue) {
      super(parameters, dummyObject);
      this.data = parameters.data;
      this._ensureSource("native");
      if (!Array.isArray(this.data)) {
        throw new TypeError("must have a `data` array")
      }
    }

    public valueOf(): DatasetValue {
      var value = super.valueOf();
      value.data = this.data;
      return value;
    }

    public toJS(): any {
      return this.data.map(datumToJS);
    }

    public toString(): string {
      return "NativeDataset(" + this.data.length + ")";
    }

    public equals(other: NativeDataset): boolean {
      return super.equals(other) &&
        this.data.length === other.data.length;
        // ToDo: probably add something else here?
    }

    public basis(): boolean {
      var data = this.data;
      return data.length === 1 && Object.keys(data[0]).length === 0;
    }

    public hasRemote(): boolean {
      if (!this.data.length) return false;
      return datumHasRemote(this.data[0]);
    }

    // Actions
    public apply(name: string, exFn: ComputeFn, context: Datum): NativeDataset {
      // Note this works in place, fix that later if needed.
      var data = this.data;
      for (let datum of data) {
        datum[name] = exFn(datum, context);
      }
      this.attributes = null; // Since we did the change in place, blow out the attributes
      return this;
    }

    public applyPromise(name: string, exFn: ComputePromiseFn, context: Datum): Q.Promise<NativeDataset> {
      // Note this works in place, fix that later if needed.
      var ds = this;
      var promises = this.data.map(datum => exFn(datum, context));
      return Q.all(promises).then(values => {
        var data = ds.data;
        var n = data.length;
        for (var i = 0; i < n; i++) data[i][name] = values[i];
        this.attributes = null; // Since we did the change in place, blow out the attributes
        return ds; // ToDo: make a new NativeDataset here
      });
    }

    public filter(exFn: ComputeFn, context: Datum): NativeDataset {
      var value = this.valueOf();
      value.data = this.data.filter(datum => exFn(datum, context));
      return new NativeDataset(value);
    }

    public sort(exFn: ComputeFn, direction: string, context: Datum): NativeDataset {
      var value = this.valueOf();
      var directionFn = directionFns[direction];
      value.data = this.data.sort((a, b) => { // Note: this modifies the original, fix if needed
        return directionFn(exFn(a, context), exFn(b, context))
      });
      return new NativeDataset(value);
    }

    public limit(limit: number): NativeDataset {
      var data = this.data;
      if (data.length <= limit) return this;
      var value = this.valueOf();
      value.data = data.slice(0, limit);
      return new NativeDataset(value);
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
      var max = Infinity;
      for (let datum of data) {
        var v = exFn(datum, context);
        if (max < v) max = v;
      }
      return max;
    }

    public quantile(exFn: ComputeFn, quantile: number, context: Datum): number {
      return quantile; // ToDo fill this in
    }

    public group(exFn: ComputeFn, context: Datum): Set {
      var data = this.data;
      var splits: Lookup<any> = {};
      for (let datum of data) {
        var v: any = exFn(datum, context);
        splits[v] = v;
      }
      return Set.fromJS(Object.keys(splits).map(k => splits[k]));
    }

    public split(exFn: ComputeFn, splitAs: string, datasetName: string, context: Datum): NativeDataset {
      var data = this.data;
      var splits: Lookup<any> = {};
      var datas: Lookup<Datum[]> = {};
      for (let datum of data) {
        var v: any = exFn(datum, context);
        var key = String(v);
        if (hasOwnProperty(splits, key)) {
          datas[key].push(datum);
        } else {
          splits[key] = v;
          datas[key] = [datum];
        }
      }
      return new NativeDataset({
        key: splitAs,
        data: Object.keys(splits).map(k => {
          var datum: Datum = {};
          datum[splitAs] = splits[k];
          datum[datasetName] = new NativeDataset({ suppress: true, data: datas[k] });
          return datum;
        })
      })
    }

    // Introspection
    public introspect(): void {
      if (this.attributes) return;

      var data = this.data;
      if (!data.length) {
        this.attributes = {};
        return;
      }
      var datum = data[0];

      var attributes: Attributes = {};
      Object.keys(datum).forEach(applyName => {
        var applyValue = datum[applyName];
        attributes[applyName] = getAttributeInfo(applyValue);
      });

      var attributeOverrides = this.attributeOverrides;
      if (attributeOverrides) {
        for (var k in attributeOverrides) {
          attributes[k] = attributeOverrides[k];
        }
        this.attributeOverrides = null;
      }

      // ToDo: make this immutable so it matches the rest of the code
      this.attributes = attributes;
    }

    public getFullType(): FullType {
      this.introspect();
      return super.getFullType();
    }

    public getRemoteDatasets(): RemoteDataset[] {
      if (this.data.length === 0) return [];
      var datum = this.data[0];
      var remoteDatasets: RemoteDataset[][] = [];
      Object.keys(datum).forEach(applyName => {
        var applyValue = datum[applyName];
        if (applyValue instanceof Dataset) {
          remoteDatasets.push(applyValue.getRemoteDatasets());
        }
      });
      return mergeRemoteDatasets(remoteDatasets);
    }

    public getRemoteDatasetIds(): string[] {
      if (this.data.length === 0) return [];
      var datum = this.data[0];
      var push = Array.prototype.push;
      var remoteDatasetIds: string[] = [];
      Object.keys(datum).forEach(applyName => {
        var applyValue = datum[applyName];
        if (applyValue instanceof Dataset) {
          push.apply(remoteDatasetIds, applyValue.getRemoteDatasets());
        }
      });
      return deduplicateSort(remoteDatasetIds);
    }

    public join(other: NativeDataset): NativeDataset {
      if (!other) return this;

      var thisKey = this.key;
      if (!thisKey) throw new Error('join lhs must have a key (be a product of a split)');
      var otherKey = other.key;
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
      return new NativeDataset({ data: newData });
    }

    public getOrderedColumns(): OrderedColumns {
      this.introspect();
      var orderedColumns: OrderedColumns = [];
      var attributes = this.attributes;

      var subDatasetAdded: boolean = false;
      for (var attributeName in attributes) {
        if (!hasOwnProperty(attributes, attributeName)) continue;
        var attributeInfo = attributes[attributeName];
        var column: Column = {
          name: attributeName,
          type: attributeInfo.type
        };
        if (attributeInfo.type === 'DATASET') {
          if (!subDatasetAdded) {
            subDatasetAdded = true;
            column.columns = this.data[0][attributeName].getOrderedColumns();
            orderedColumns.push(column);
          }
        } else {
          orderedColumns.push(column);
        }
      }

      return orderedColumns.sort((a, b) => {
        var typeDiff = typeOrder[a.type] - typeOrder[b.type];
        if (typeDiff) return typeDiff;
        return a.name.localeCompare(b.name);
      });
    }

    private _flattenHelper(flattenedColumns: OrderedColumns, prefix: string, context: Datum, flat: Datum[]): void {
      var data = this.data;
      for (let datum of data) {
        var flatDatum = copy(context);
        for (let flattenedColumn of flattenedColumns) {
          if (flattenedColumn.type === 'DATASET') {
            datum[flattenedColumn.name]._flattenHelper(
              flattenedColumn.columns,
              prefix + flattenedColumn.name + '.',
              flatDatum,
              flat
            );
          } else {
            flatDatum[prefix + flattenedColumn.name] = datum[flattenedColumn.name];
          }
        }
        if (flattenedColumns[flattenedColumns.length - 1].type !== 'DATASET') {
          // There is no subset to delegate to
          flat.push(flatDatum);
        }
      }
    }

    public flatten(): FlatData {
      var flattenedColumns = this.getOrderedColumns();
      var flatData: Datum[] = [];
      this._flattenHelper(flattenedColumns, '', {}, flatData);

      var flatColumns: OrderedColumns = [];
      var workingColumns = flattenedColumns;
      var i = 0;
      var prefix = '';
      while (i < workingColumns.length) {
        var workingColumn = workingColumns[i];
        if (workingColumn.type === 'DATASET') {
          workingColumns = workingColumn.columns;
          prefix += workingColumn.name + '.';
          i = 0;
        } else {
          flatColumns.push({
            name: prefix + workingColumn.name,
            type: workingColumn.type
          });
          i++;
        }
      }
      
      return {
        columns: flatColumns,
        data: flatData
      };
    }

    public toTabular(tabulatorOptions: TabulatorOptions): string {
      var formatter: Formatter = tabulatorOptions.formatter || {};
      var flatData = this.flatten();
      var columns = flatData.columns;
      var data = flatData.data;

      var lines: string[] = [];
      lines.push(columns.map(c => c.name).join(tabulatorOptions.separator || ','));

      for (var i = 0; i < data.length; i++) {
        var datum = data[i];
        lines.push(columns.map(c => {
          return String((formatter[c.type] || defaultFormatter[c.type])(datum[c.name]));
        }).join(tabulatorOptions.separator || ','));
      }

      return lines.join(tabulatorOptions.lineBreak || '\n');
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

  Dataset.register(NativeDataset);
}
