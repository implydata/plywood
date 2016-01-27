module Plywood {
  function isInteger(n: any): boolean {
    return !isNaN(n) && n % 1 === 0;
  }

  function isPositiveInteger(n: any): boolean {
    return isInteger(n) && 0 < n;
  }

  export type Attributes = AttributeInfo[];
  export type AttributeJSs = AttributeInfoJS[];

  export interface AttributeInfoValue {
    special?: string;
    name: string;
    type?: string;
    datasetType?: Lookup<FullType>;
    unsplitable?: boolean;
    makerAction?: Action;

    // range
    separator?: string;
    rangeSize?: number;
    digitsBeforeDecimal?: int;
    digitsAfterDecimal?: int;
  }

  export interface AttributeInfoJS {
    special?: string;
    name: string;
    type?: string;
    datasetType?: Lookup<FullType>;
    unsplitable?: boolean;
    makerAction?: ActionJS;

    // range
    separator?: string;
    rangeSize?: number;
    digitsBeforeDecimal?: int;
    digitsAfterDecimal?: int;
  }

  var check: Class<AttributeInfoValue, AttributeInfoJS>;
  export class AttributeInfo implements Instance<AttributeInfoValue, AttributeInfoJS> {
    static isAttributeInfo(candidate: any): boolean {
      return isInstanceOf(candidate, AttributeInfo);
    }

    static jsToValue(parameters: AttributeInfoJS): AttributeInfoValue {
      var value: AttributeInfoValue = {
        special: parameters.special,
        name: parameters.name
      };
      if (parameters.type) value.type = parameters.type;
      if (parameters.datasetType) value.datasetType = parameters.datasetType;
      if (parameters.unsplitable) value.unsplitable = true;
      if (parameters.makerAction) value.makerAction = Action.fromJS(parameters.makerAction);
      return value;
    }

    static classMap: Lookup<typeof AttributeInfo> = {};
    static register(ex: typeof AttributeInfo): void {
      var op = (<any>ex).name.replace('AttributeInfo', '').replace(/^\w/, (s: string) => s.toLowerCase());
      AttributeInfo.classMap[op] = ex;
    }

    static fromJS(parameters: AttributeInfoJS): AttributeInfo {
      if (typeof parameters !== "object") {
        throw new Error("unrecognizable attributeMeta");
      }
      if (!hasOwnProperty(parameters, 'special')) {
        return new AttributeInfo(AttributeInfo.jsToValue(parameters));
      }
      var Class = AttributeInfo.classMap[parameters.special];
      if (!Class) {
        throw new Error(`unsupported special attributeInfo '${parameters.special}'`);
      }
      return Class.fromJS(parameters);
    }

    static fromJSs(attributeJSs: AttributeJSs): Attributes {
      if (!Array.isArray(attributeJSs)) {
        if (attributeJSs && typeof attributeJSs === 'object') {
          var newAttributeJSs: any[] = [];
          for (var attributeName in attributeJSs) {
            if (!hasOwnProperty(attributeJSs, attributeName)) continue;
            var attributeJS = attributeJSs[attributeName];
            attributeJS['name'] = attributeName;
            newAttributeJSs.push(attributeJS);
          }
          console.warn('attributes now needs to be passed as an array like so: ' + JSON.stringify(newAttributeJSs, null, 2));
          attributeJSs = newAttributeJSs;
        } else {
          throw new TypeError("invalid attributeJSs");
        }
      }
      return attributeJSs.map(attributeJS => AttributeInfo.fromJS(attributeJS));
    }

    static toJSs(attributes: Attributes): AttributeJSs {
      return attributes.map(attribute => attribute.toJS());
    }

    static applyOverrides(attributes: Attributes, attributeOverrides: Attributes): Attributes {
      attributeOverrides.forEach(attributeOverride => {
        var attributeOverrideName = attributeOverride.name;
        var added = false;
        attributes = attributes.map(a => {
          if (a.name === attributeOverrideName) {
            added = true;
            return attributeOverride;
          } else {
            return a;
          }
        });
        if (!added) {
          attributes = attributes.concat(attributeOverride);
        }
      });
      return attributes;
    }


    public special: string;
    public name: string;
    public type: string;
    public datasetType: Lookup<FullType>;
    public unsplitable: boolean;
    public makerAction: Action;

    constructor(parameters: AttributeInfoValue) {
      if (parameters.special) this.special = parameters.special;

      if (typeof parameters.name !== "string") {
        throw new Error("name must be a string");
      }
      this.name = parameters.name;

      if (hasOwnProperty(parameters, 'type') && typeof parameters.type !== "string") {
        throw new Error("type must be a string");
      }
      this.type = parameters.type;

      this.datasetType = parameters.datasetType;
      this.unsplitable = Boolean(parameters.unsplitable);
      this.makerAction = parameters.makerAction;
    }

    public _ensureSpecial(special: string) {
      if (!this.special) {
        this.special = special;
        return;
      }
      if (this.special !== special) {
        throw new TypeError("incorrect attributeInfo `special` '" + this.special + "' (needs to be: '" + special + "')");
      }
    }

    public _ensureType(myType: string) {
      if (!this.type) {
        this.type = myType;
        return;
      }
      if (this.type !== myType) {
        throw new TypeError("incorrect attributeInfo `type` '" + this.type + "' (needs to be: '" + myType + "')");
      }
    }

    public toString(): string {
      var special = this.special || 'basic';
      return `${special}(${this.type})`;
    }

    public valueOf(): AttributeInfoValue {
      var value: AttributeInfoValue = {
        name: this.name,
        type: this.type,
        unsplitable: this.unsplitable
      };
      if (this.special) value.special = this.special;
      if (this.datasetType) value.datasetType = this.datasetType;
      if (this.makerAction) value.makerAction = this.makerAction;
      return value;
    }

    public toJS(): AttributeInfoJS {
      var js: AttributeInfoJS = {
        name: this.name,
        type: this.type
      };
      if (this.unsplitable) js.unsplitable = true;
      if (this.special) js.special = this.special;
      if (this.datasetType) js.datasetType = this.datasetType;
      if (this.makerAction) js.makerAction = this.makerAction.toJS();
      return js;
    }

    public toJSON(): AttributeInfoJS {
      return this.toJS();
    }

    public equals(other: AttributeInfo): boolean {
      return AttributeInfo.isAttributeInfo(other) &&
        this.special === other.special &&
        this.name === other.name &&
        this.type === other.type &&
        Boolean(this.makerAction) === Boolean(other.makerAction) &&
        (!this.makerAction || this.makerAction.equals(other.makerAction));
    }

    public serialize(value: any): any {
      return value;
    }
  }
  check = AttributeInfo;


  export class RangeAttributeInfo extends AttributeInfo {
    static fromJS(parameters: AttributeInfoJS): RangeAttributeInfo {
      var value = AttributeInfo.jsToValue(parameters);
      value.separator = parameters.separator;
      value.rangeSize = parameters.rangeSize;
      value.digitsBeforeDecimal = parameters.digitsBeforeDecimal;
      value.digitsAfterDecimal = parameters.digitsAfterDecimal;
      return new RangeAttributeInfo(value);
    }

    public separator: string;
    public rangeSize: number;
    public digitsBeforeDecimal: int;
    public digitsAfterDecimal: int;

    constructor(parameters: AttributeInfoValue) {
      super(parameters);
      this.separator = parameters.separator || ';';
      this.rangeSize = parameters.rangeSize;
      this.digitsBeforeDecimal = parameters.digitsBeforeDecimal;
      this.digitsAfterDecimal = parameters.digitsAfterDecimal;
      this._ensureSpecial("range");
      this._ensureType('NUMBER_RANGE');
      if (!(typeof this.separator === "string" && this.separator.length)) {
        throw new TypeError("`separator` must be a non-empty string");
      }
      if (typeof this.rangeSize !== "number") {
        throw new TypeError("`rangeSize` must be a number");
      }
      if (this.rangeSize > 1) {
        if (!isInteger(this.rangeSize)) {
          throw new Error("`rangeSize` greater than 1 must be an integer");
        }
      } else {
        if (!isInteger(1 / this.rangeSize)) {
          throw new Error("`rangeSize` less than 1 must divide 1");
        }
      }

      if (this.digitsBeforeDecimal != null) {
        if (!isPositiveInteger(this.digitsBeforeDecimal)) {
          throw new Error("`digitsBeforeDecimal` must be a positive integer");
        }
      } else {
        this.digitsBeforeDecimal = null;
      }

      if (this.digitsAfterDecimal != null) {
        if (!isPositiveInteger(this.digitsAfterDecimal)) {
          throw new Error("`digitsAfterDecimal` must be a positive integer");
        }
        var digitsInSize = (String(this.rangeSize).split(".")[1] || "").length;
        if (this.digitsAfterDecimal < digitsInSize) {
          throw new Error("`digitsAfterDecimal` must be at least " + digitsInSize + " to accommodate for a `rangeSize` of " + this.rangeSize);
        }
      } else {
        this.digitsAfterDecimal = null;
      }
    }

    public valueOf() {
      var value = super.valueOf();
      value.separator = this.separator;
      value.rangeSize = this.rangeSize;
      if (this.digitsBeforeDecimal !== null) value.digitsBeforeDecimal = this.digitsBeforeDecimal;
      if (this.digitsAfterDecimal !== null) value.digitsAfterDecimal = this.digitsAfterDecimal;
      return value;
    }

    public toJS(): AttributeInfoJS {
      var js = super.toJS();
      js.separator = this.separator;
      js.rangeSize = this.rangeSize;
      if (this.digitsBeforeDecimal !== null) js.digitsBeforeDecimal = this.digitsBeforeDecimal;
      if (this.digitsAfterDecimal !== null) js.digitsAfterDecimal = this.digitsAfterDecimal;
      return js;
    }

    public equals(other: RangeAttributeInfo): boolean {
      return super.equals(other) &&
        this.separator === other.separator &&
        this.rangeSize === other.rangeSize &&
        this.digitsBeforeDecimal === other.digitsBeforeDecimal &&
        this.digitsAfterDecimal === other.digitsAfterDecimal;
    }

    public _serializeNumber(value: number): string {
      if (value === null) return "";
      var valueStr = String(value);
      if (this.digitsBeforeDecimal === null && this.digitsAfterDecimal === null) {
        return valueStr;
      }
      var valueStrSplit = valueStr.split(".");
      var before = valueStrSplit[0];
      var after = valueStrSplit[1];
      if (this.digitsBeforeDecimal) {
        before = repeat("0", this.digitsBeforeDecimal - before.length) + before;
      }

      if (this.digitsAfterDecimal) {
        after || (after = "");
        after += repeat("0", this.digitsAfterDecimal - after.length);
      }

      valueStr = before;
      if (after) valueStr += "." + after;
      return valueStr;
    }

    public serialize(range: any): string {
      if (!(Array.isArray(range) && range.length === 2)) return null;
      return this._serializeNumber(range[0]) + this.separator + this._serializeNumber(range[1]);
    }

    public getMatchingRegExpString() {
      var separatorRegExp = this.separator.replace(/[.$^{[(|)*+?\\]/g, (c) => "\\" + c);
      var beforeRegExp = this.digitsBeforeDecimal ? "-?\\d{" + this.digitsBeforeDecimal + "}" : "(?:-?[1-9]\\d*|0)";
      var afterRegExp = this.digitsAfterDecimal ? "\\.\\d{" + this.digitsAfterDecimal + "}" : "(?:\\.\\d*[1-9])?";
      var numberRegExp = beforeRegExp + afterRegExp;
      return "/^(" + numberRegExp + ")" + separatorRegExp + "(" + numberRegExp + ")$/";
    }
  }
  AttributeInfo.register(RangeAttributeInfo);


  export class UniqueAttributeInfo extends AttributeInfo {
    static fromJS(parameters: AttributeInfoJS): UniqueAttributeInfo {
      return new UniqueAttributeInfo(AttributeInfo.jsToValue(parameters));
    }

    constructor(parameters: AttributeInfoValue) {
      super(parameters);
      this._ensureSpecial("unique");
      this._ensureType('STRING');
    }

    public serialize(value: any): string {
      throw new Error("can not serialize an approximate unique value");
    }
  }
  AttributeInfo.register(UniqueAttributeInfo);

  export class HistogramAttributeInfo extends AttributeInfo {
    static fromJS(parameters: AttributeInfoJS): HistogramAttributeInfo {
      return new HistogramAttributeInfo(AttributeInfo.jsToValue(parameters));
    }

    constructor(parameters: AttributeInfoValue) {
      super(parameters);
      this._ensureSpecial("histogram");
      this._ensureType('NUMBER');
    }

    public serialize(value: any): string {
      throw new Error("can not serialize a histogram value");
    }
  }
  AttributeInfo.register(HistogramAttributeInfo);
}
