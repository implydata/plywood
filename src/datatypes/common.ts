module Plywood {
  export function getValueType(value: any): string {
    var typeofValue = typeof value;
    if (typeofValue === 'object') {
      if (value === null) {
        return 'NULL';
      } else if (value.toISOString) {
        return 'TIME';
      } else {
        var ctrType = value.constructor.type;
        if (!ctrType) {
          if (Expression.isExpression(value)) {
            throw new Error("expression used as datum value " + value.toString());
          } else {
            throw new Error("can not have an object without a type: " + JSON.stringify(value));
          }
        }
        if (ctrType === 'SET') ctrType += '/' + value.setType;
        return ctrType;
      }
    } else {
      if (typeofValue !== 'boolean' && typeofValue !== 'number' && typeofValue !== 'string') {
        throw new TypeError('unsupported JS type ' + typeofValue);
      }
      return typeofValue.toUpperCase();
    }
  }

  export function getFullType(value: any): FullType {
    var myType = getValueType(value);
    return myType === 'DATASET' ? (<Dataset>value).getFullType() : { type: myType };
  }

  export function valueFromJS(v: any, typeOverride: string = null): any {
    if (v == null) {
      return null;
    } else if (Array.isArray(v)) {
      return Dataset.fromJS(v);
    } else if (typeof v === 'object') {
      switch (typeOverride || v.type) {
        case 'NUMBER':
          var n = Number(v.value);
          if (isNaN(n)) throw new Error("bad number value '" + String(v.value) + "'");
          return n;

        case 'NUMBER_RANGE':
          return NumberRange.fromJS(v);

        case 'TIME':
          return typeOverride ? v : new Date(v.value);

        case 'TIME_RANGE':
          return TimeRange.fromJS(v);

        case 'SHAPE':
          return Shape.fromJS(v);

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

  export function numberToSQL(num: number): string {
    if (num === null) return null;
    return String(num);
  }

  export function timeToSQL(date: Date): string {
    if (!date) return null;
    var str = date.toISOString()
      .replace("T", " ")
      .replace(/\.\d\d\dZ$/, "")
      .replace(" 00:00:00", "");
    return "'" + str + "'";
  }

  // Remote functionality

  export function datumHasExternal(datum: Datum): boolean {
    for (var applyName in datum) {
      var applyValue = datum[applyName];
      if (applyValue instanceof Dataset && applyValue.hasRemote()) {
        return true;
      }
    }
    return false;
  }

  export function introspectDatum(datum: Datum): Q.Promise<Datum> {
    return Q.all(
      Object.keys(datum).map(applyName => {
        /*
        var applyValue = datum[applyName];
        if (applyValue instanceof External && applyValue.needsIntrospect()) {
          return applyValue.introspect().then((newExternal: External) => {
            datum[applyName] = newExternal;
          })
        }
        */
        return null;
      }).filter(Boolean)
    ).then(() => datum);
  }

  export interface FullType {
    type: string;
    datasetType?: Lookup<FullType>;
    parent?: FullType;
    remote?: string[];
  }
}
