module Plywood {
  export class NumberBucketAction extends Action {
    static fromJS(parameters: ActionJS): NumberBucketAction {
      var value = Action.jsToValue(parameters);
      value.size = parameters.size;
      value.offset = hasOwnProperty(parameters, 'offset') ? parameters.offset : 0;
      return new NumberBucketAction(value);
    }

    public size: number;
    public offset: number;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.size = parameters.size;
      this.offset = parameters.offset;
      this._ensureAction("numberBucket");
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.size = this.size;
      value.offset = this.offset;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.size = this.size;
      if (this.offset) js.offset = this.offset;
      return js;
    }

    public equals(other: NumberBucketAction): boolean {
      return super.equals(other) &&
        this.size === other.size &&
        this.offset === other.offset;
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
      return (d: Datum, c: Datum) => {
        var num = inputFn(d, c);
        if (num === null) return null;
        return NumberRange.numberBucket(num, size, offset);
      }
    }

    protected _getJSHelper(inputJS: string): string {
      return continuousFloorExpression(inputJS, "Math.floor", this.size, this.offset);
    }

    protected _getSQLHelper(dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      return continuousFloorExpression(inputSQL, "FLOOR", this.size, this.offset);
    }
  }

  Action.register(NumberBucketAction);
}
