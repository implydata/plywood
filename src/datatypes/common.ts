module Plywood {
  export function isDate(dt: any): dt is Date {
    return !!(dt && dt.toISOString);
  }

  export function getValueType(value: any): PlyType {
    var typeofValue = typeof value;
    if (typeofValue === 'object') {
      if (value === null) {
        return 'NULL';
      } else if (isDate(value)) {
        return 'TIME';
      } else if (hasOwnProperty(value, 'start') && hasOwnProperty(value, 'end')) {
        if (isDate(value.start) || isDate(value.end)) return 'TIME_RANGE';
        if (typeof value.start === 'number' || typeof value.end === 'number') return 'NUMBER_RANGE';
        throw new Error("unrecognizable range");
      } else {
        var ctrType = value.constructor.type;
        if (!ctrType) {
          if (Expression.isExpression(value)) {
            throw new Error(`expression used as datum value ${value}`);
          } else {
            throw new Error(`can not have an object without a type: ${JSON.stringify(value)}`);
          }
        }
        if (ctrType === 'SET') ctrType += '/' + value.setType;
        return <PlyType>ctrType;
      }
    } else {
      if (typeofValue !== 'boolean' && typeofValue !== 'number' && typeofValue !== 'string') {
        throw new TypeError('unsupported JS type ' + typeofValue);
      }
      return <PlyType>typeofValue.toUpperCase();
    }
  }

  export function getFullType(value: any): FullType {
    var myType = getValueType(value);
    return myType === 'DATASET' ? (<Dataset>value).getFullType() : { type: <PlyTypeSimple>myType };
  }

  export function getFullTypeFromDatum(datum: Datum): DatasetFullType {
    var datasetType: Lookup<FullType> = {};
    for (var k in datum) {
      if (!hasOwnProperty(datum, k)) continue;
      datasetType[k] = getFullType(datum[k]);
    }

    return {
      type: 'DATASET',
      datasetType: datasetType
    };
  }

  export function valueFromJS(v: any, typeOverride: string = null): any {
    if (v == null) {
      return null;
    } else if (Array.isArray(v)) {
      if (v.length && typeof v[0] !== 'object') {
        return Set.fromJS(v);
      } else {
        return Dataset.fromJS(v);
      }
    } else if (typeof v === 'object') {
      switch (typeOverride || v.type) {
        case 'NUMBER':
          var n = Number(v.value);
          if (isNaN(n)) throw new Error(`bad number value '${v.value}'`);
          return n;

        case 'NUMBER_RANGE':
          return NumberRange.fromJS(v);

        case 'TIME':
          return typeOverride ? v : new Date(v.value);

        case 'TIME_RANGE':
          return TimeRange.fromJS(v);

        case 'SET':
          return Set.fromJS(v);

        default:
          if (v.toISOString) {
            return v; // Allow native date
          } else {
            throw new Error('can not have an object without a `type` as a datum value');
          }
      }
    } else if (typeof v === 'string' && typeOverride === 'TIME') {
      return new Date(v);
    }
    return v;
  }

  export function valueToJS(v: any): any {
    if (v == null) {
      return null;
    } else {
      var typeofV = typeof v;
      if (typeofV === 'object') {
        if (v.toISOString) {
          return v;
        } else {
          return v.toJS();
        }
      } else if (typeofV === 'number' && !isFinite(v)) {
        return String(v)
      }
    }
    return v;
  }

  export function valueToJSInlineType(v: any): any {
    if (v == null) {
      return null;
    } else {
      var typeofV = typeof v;
      if (typeofV === 'object') {
        if (v.toISOString) {
          return { type: 'TIME', value: v };
        } else {
          var js = v.toJS();
          if (!Array.isArray(js)) {
            js.type = v.constructor.type;
          }
          return js;
        }
      } else if (typeofV === 'number' && !isFinite(v)) {
        return { type: 'NUMBER', value: String(v) };
      }
    }
    return v;
  }

  // Remote functionality

  export function datumHasExternal(datum: Datum): boolean {
    for (var name in datum) {
      var value = datum[name];
      if (value instanceof External) return true;
      if (value instanceof Dataset && value.hasExternal()) return true;
    }
    return false;
  }

  export function introspectDatum(datum: Datum): Q.Promise<Datum> {
    var promises: Q.Promise<void>[] = [];
    var newDatum: Datum = Object.create(null);
    Object.keys(datum)
      .forEach(name => {
        var v = datum[name];
        if (v instanceof External && v.needsIntrospect()) {
          promises.push(
            v.introspect().then((introspectedExternal: External) => {
              newDatum[name] = introspectedExternal;
            })
          );
        } else {
          newDatum[name] = v;
        }
      });

    return Q.all(promises).then(() => newDatum);
  }

  export type PlyTypeSimple = 'NULL' | 'BOOLEAN' | 'NUMBER' | 'TIME' | 'STRING' | 'NUMBER_RANGE' | 'TIME_RANGE' | 'SET' | 'SET/NULL' | 'SET/BOOLEAN' | 'SET/NUMBER' | 'SET/TIME' | 'SET/STRING' | 'SET/NUMBER_RANGE' | 'SET/TIME_RANGE';

  export type PlyType = PlyTypeSimple | 'DATASET';

  export function isSetType(type: PlyType): boolean {
    return type && type.indexOf('SET/') === 0;
  }

  export function wrapSetType(type: PlyType): PlyType {
    return isSetType(type) ? type : <PlyType>('SET/' + type);
  }

  export function unwrapSetType(type: PlyType): PlyType {
    if (!type) return null;
    return isSetType(type) ? <PlyType>type.substr(4) : type;
  }

  export interface SimpleFullType {
    type: PlyTypeSimple;
  }

  export interface DatasetFullType {
    type: 'DATASET';
    datasetType: Lookup<FullType>;
    parent?: DatasetFullType;
    remote?: boolean;
  }

  export type FullType = SimpleFullType | DatasetFullType;
}
