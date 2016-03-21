module Plywood {
  export class NumberBucketAction extends Action {
    static fromJS(parameters: ActionJS): NumberBucketAction {
      var value = Action.jsToValue(parameters);
      value.size = parameters.size;
      value.offset = hasOwnProperty(parameters, 'offset') ? parameters.offset : 0;
      value.lowerLimit = hasOwnProperty(parameters, 'lowerLimit') ? parameters.lowerLimit : null;
      value.upperLimit = hasOwnProperty(parameters, 'upperLimit') ? parameters.upperLimit : null;
      return new NumberBucketAction(value);
    }

    public size: number;
    public offset: number;
    public lowerLimit: number;
    public upperLimit: number;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      var size = parameters.size;
      this.size = size;

      var offset = parameters.offset;
      this.offset = offset;

      var lowerLimit = parameters.lowerLimit;
      this.lowerLimit = lowerLimit;

      var upperLimit = parameters.upperLimit;
      this.upperLimit = upperLimit;

      this._ensureAction("numberBucket");
      if (lowerLimit !== null && upperLimit !== null && upperLimit - lowerLimit < size) {
        throw new Error('lowerLimit and upperLimit must be at least size apart');
      }
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.size = this.size;
      value.offset = this.offset;
      value.lowerLimit = this.lowerLimit;
      value.upperLimit = this.upperLimit;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.size = this.size;
      if (this.offset) js.offset = this.offset;
      if (this.lowerLimit !== null) js.lowerLimit = this.lowerLimit;
      if (this.upperLimit !== null) js.upperLimit = this.upperLimit;
      return js;
    }

    public equals(other: NumberBucketAction): boolean {
      return super.equals(other) &&
        this.size === other.size &&
        this.offset === other.offset &&
        this.lowerLimit === other.lowerLimit &&
        this.upperLimit === other.upperLimit;
    }

    protected _toStringParameters(expressionString: string): string[] {
      var params: string[] = [String(this.size)];
      if (this.offset) params.push(String(this.offset));
      return params;
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'NUMBER', 'NUMBER_RANGE');
      return 'NUMBER_RANGE';
    }

    public _fillRefSubstitutions(): FullType {
      return {
        type: 'NUMBER_RANGE',
      };
    }

    protected _getFnHelper(inputFn: ComputeFn): ComputeFn {
      var size = this.size;
      var offset = this.offset;
      var lowerLimit = this.lowerLimit;
      var upperLimit = this.upperLimit;
      return (d: Datum, c: Datum) => {
        var num = inputFn(d, c);
        if (num === null) return null;
        return NumberRange.numberBucket(num, size, offset); // ToDo: lowerLimit, upperLimit
      }
    }

    protected _getJSHelper(inputJS: string): string {
      return continuousFloorExpression(inputJS, "Math.floor", this.size, this.offset); // ToDo: lowerLimit, upperLimit
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return continuousFloorExpression(inputSQL, "FLOOR", this.size, this.offset); // ToDo: lowerLimit, upperLimit
    }
  }

  Action.register(NumberBucketAction);
}
