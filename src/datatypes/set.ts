module Plywood {
  export interface SetValue {
    setType: string;
    elements: Array<any>; // These are value any
  }

  export interface SetJS {
    setType: string;
    elements: Array<any>; // These are JS any
  }

  function dateString(date: Date): string {
    return date.toISOString();
  }

  function arrayFromJS(xs: Array<any>, setType: string): Array<any> {
    return xs.map(x => valueFromJS(x, setType));
  }

  function unifyElements(elements: Array<PlywoodRange>): Array<PlywoodRange> {
    var newElements: Lookup<PlywoodRange> = Object.create(null);
    for (var accumulator of elements) {
      var newElementsKeys = Object.keys(newElements);
      for (let newElementsKey of newElementsKeys) {
        var newElement = newElements[newElementsKey];
        var unionElement = accumulator.union(newElement);
        if (unionElement) {
          accumulator = unionElement;
          delete newElements[newElementsKey];
        }
      }
      newElements[accumulator.toString()] = accumulator;
    }
    return Object.keys(newElements).map(k => newElements[k]);
  }

  function intersectElements(elements1: Array<PlywoodRange>, elements2: Array<PlywoodRange>): Array<PlywoodRange> {
    var newElements: Array<PlywoodRange> = [];
    for (var element1 of elements1) {
      for (var element2 of elements2) {
        var intersect = element1.intersect(element2);
        if (intersect) newElements.push(intersect);
      }
    }
    return newElements;
  }

  var typeUpgrades: Lookup<string> = {
    'NUMBER': 'NUMBER_RANGE',
    'TIME': 'TIME_RANGE'
  };

  var check: Class<SetValue, SetJS>;
  export class Set implements Instance<SetValue, SetJS> {
    static type = 'SET';
    static EMPTY: Set;

    static isSet(candidate: any): candidate is Set {
      return isInstanceOf(candidate, Set);
    }

    static convertToSet(thing: any): Set {
      var thingType = getValueType(thing);
      if (isSetType(thingType)) return thing;
      return Set.fromJS({ setType: thingType, elements: [thing] });
    }

    static generalUnion(a: any, b: any): any {
      var aSet = Set.convertToSet(a);
      var bSet = Set.convertToSet(b);
      var aSetType = aSet.setType;
      var bSetType = bSet.setType;

      if (typeUpgrades[aSetType] === bSetType) {
        aSet = aSet.upgradeType();
      } else if (typeUpgrades[bSetType] === aSetType) {
        bSet = bSet.upgradeType();
      } else if (aSetType !== bSetType) {
        return null;
      }

      return aSet.union(bSet).simplify();
    }

    static generalIntersect(a: any, b: any): any {
      var aSet = Set.convertToSet(a);
      var bSet = Set.convertToSet(b);
      var aSetType = aSet.setType;
      var bSetType = bSet.setType;

      if (typeUpgrades[aSetType] === bSetType) {
        aSet = aSet.upgradeType();
      } else if (typeUpgrades[bSetType] === aSetType) {
        bSet = bSet.upgradeType();
      } else if (aSetType !== bSetType) {
        return null;
      }

      return aSet.intersect(bSet).simplify();
    }

    static fromJS(parameters: Array<any>): Set;
    static fromJS(parameters: SetJS): Set;
    static fromJS(parameters: any): Set {
      if (Array.isArray(parameters)) {
        parameters = { elements: parameters };
      }
      if (typeof parameters !== "object") {
        throw new Error("unrecognizable set");
      }
      var setType = parameters.setType;
      var elements = parameters.elements;
      if (!setType) {
        setType = getValueType(elements.length ? elements[0] : null);
      }
      return new Set({
        setType: setType,
        elements: arrayFromJS(elements, setType)
      });
    }

    public setType: string;
    public elements: Array<any>;

    private keyFn: (v: any) => string;
    private hash: Lookup<any>;

    constructor(parameters: SetValue) {
      var setType = parameters.setType;
      this.setType = setType;
      var keyFn = setType === 'TIME' ? dateString : String;
      this.keyFn = keyFn;

      var elements = parameters.elements;
      var newElements: any[] = null;
      var hash: Lookup<any> = Object.create(null);
      for (var i = 0; i < elements.length; i++) {
        var element = elements[i];
        var key = keyFn(element);
        if (hash[key]) {
          if (!newElements) newElements = elements.slice(0, i);
        } else {
          hash[key] = element;
          if (newElements) newElements.push(element);
        }
      }

      if (newElements) {
        elements = newElements
      }

      if (setType === 'NUMBER_RANGE' || setType === 'TIME_RANGE') {
        elements = unifyElements(elements);
      }
      this.elements = elements;
      this.hash = hash;
    }

    public valueOf(): SetValue {
      return {
        setType: this.setType,
        elements: this.elements
      };
    }

    public toJS(): SetJS {
      return {
        setType: this.setType,
        elements: this.elements.map(valueToJS)
      };
    }

    public toJSON(): SetJS {
      return this.toJS();
    }

    public toString(): string {
      if (this.setType === "NULL") return "null";
      return `${this.elements.map(String).join(", ")}`;
    }

    public equals(other: Set): boolean {
      return Set.isSet(other) &&
        this.setType === other.setType &&
        this.elements.length === other.elements.length &&
        this.elements.slice().sort().join('') === other.elements.slice().sort().join('');
    }

    public size(): int {
      return this.elements.length;
    }

    public empty(): boolean {
      return this.elements.length === 0;
    }

    public simplify(): any {
      var simpleSet = this.downgradeType();
      var simpleSetElements = simpleSet.elements;
      return simpleSetElements.length === 1 ? simpleSetElements[0] : simpleSet;
    }

    public getType(): string {
      return 'SET/' + this.setType;
    }

    public upgradeType(): Set {
      if (this.setType === 'NUMBER') {
        return Set.fromJS({
          setType: 'NUMBER_RANGE',
          elements: this.elements.map(NumberRange.fromNumber)
        })
      } else if (this.setType === 'TIME') {
        return Set.fromJS({
          setType: 'TIME_RANGE',
          elements: this.elements.map(TimeRange.fromTime)
        })
      } else {
        return this;
      }
    }

    public downgradeType(): Set {
      if (this.setType === 'NUMBER_RANGE' || this.setType === 'TIME_RANGE') {
        var elements = this.elements;
        var simpleElements: any[] = [];
        for (let element of elements) {
          if (element.degenerate()) {
            simpleElements.push(element.start);
          } else {
            return this;
          }
        }
        return Set.fromJS(simpleElements)
      } else {
        return this;
      }
    }

    public extent(): PlywoodRange {
      var setType = this.setType;
      if (hasOwnProperty(typeUpgrades, setType)) {
        return this.upgradeType().extent();
      }
      if (setType !== 'NUMBER_RANGE' && setType !== 'TIME_RANGE') return null;
      var elements = this.elements;
      var extent: PlywoodRange = elements[0] || null;
      for (var i = 1; i < elements.length; i++) {
        extent = extent.extend(elements[i]);
      }
      return extent;
    }

    public union(other: Set): Set {
      if (this.empty()) return other;
      if (other.empty()) return this;

      if (this.setType !== other.setType) {
        throw new TypeError("can not union sets of different types");
      }

      var newElements: Array<any> = this.elements.slice();

      var otherElements = other.elements;
      for (var el of otherElements) {
        if (this.contains(el)) continue;
        newElements.push(el);
      }

      return new Set({
        setType: this.setType,
        elements: newElements
      });
    }

    public intersect(other: Set): Set {
      if (this.empty() || other.empty()) return Set.EMPTY;

      var setType = this.setType;
      if (this.setType !== other.setType) {
        throw new TypeError("can not intersect sets of different types");
      }

      var thisElements = this.elements;
      var newElements: Array<any>;
      if (setType === 'NUMBER_RANGE' || setType === 'TIME_RANGE') {
        var otherElements = other.elements;
        newElements = intersectElements(thisElements, otherElements);
      } else {
        newElements = [];
        for (var el of thisElements) {
          if (!other.contains(el)) continue;
          newElements.push(el);
        }
      }

      return new Set({
        setType: this.setType,
        elements: newElements
      });
    }

    public overlap(other: Set): boolean {
      if (this.empty() || other.empty()) return false;

      if (this.setType !== other.setType) {
        throw new TypeError("can determine overlap sets of different types");
      }

      var thisElements = this.elements;
      for (var el of thisElements) {
        if (!other.contains(el)) continue;
        return true;
      }

      return false;
    }

    public contains(value: any): boolean {
      const { setType } = this;
      if ((setType === 'NUMBER_RANGE' && typeof value === 'number')
        || (setType === 'TIME_RANGE' && isDate(value))) {
        return this.containsWithin(value);
      }
      return hasOwnProperty(this.hash, this.keyFn(value));

    }

    public containsWithin(value: any): boolean {
      var elements = this.elements;
      for (var k in elements) {
        if (!hasOwnProperty(elements, k)) continue;
        if ((<NumberRange>elements[k]).contains(value)) return true;
      }
      return false;
    }

    public add(value: any): Set {
      var setType = this.setType;
      var valueType = getValueType(value);
      if (setType === 'NULL') setType = valueType;
      if (valueType !== 'NULL' && setType !== valueType) throw new Error('value type must match');

      if (this.contains(value)) return this;
      return new Set({
        setType: setType,
        elements: this.elements.concat([value])
      });
    }

    public remove(value: any): Set {
      if (!this.contains(value)) return this;
      var keyFn = this.keyFn;
      var key = keyFn(value);
      return new Set({
        setType: this.setType, // There must be a set type since at least the value is there
        elements: this.elements.filter(element => keyFn(element) !== key)
      });
    }

    public toggle(value: any): Set {
      return this.contains(value) ? this.remove(value) : this.add(value);
    }
  }
  check = Set;

  Set.EMPTY = Set.fromJS([]);
}
