/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

module Plywood {
  export class CustomTransformAction extends Action {
    static fromJS(parameters: ActionJS): CustomTransformAction {
      var value = Action.jsToValue(parameters);
      value.jsStatement = parameters.jsStatement;
      return new CustomTransformAction(value);
    }

    static argumentName = "_";
    static allowedFnReturnTypes = ['STRING', 'NUMBER', 'BOOLEAN', 'NULL'];

    public jsStatement: string;

    constructor(parameters: ActionValue) {
      super(parameters, dummyObject);
      this.jsStatement = parameters.jsStatement;
      this._ensureAction("customTransform");
    }

    public valueOf(): ActionValue {
      var value = super.valueOf();
      value.jsStatement = this.jsStatement;
      return value;
    }

    public toJS(): ActionJS {
      var js = super.toJS();
      js.jsStatement = this.jsStatement;
      return js;
    }

    public equals(other: CustomTransformAction): boolean {
      return super.equals(other) &&
        this.jsStatement === other.jsStatement;
    }

    protected _toStringParameters(expressionString: string): string[] {
      return [`function(_) { return ${this.jsStatement} }`];
    }

    public getOutputType(inputType: PlyType): PlyType {
      this._checkInputTypes(inputType, 'STRING');

      try {
        var testReturnVal = this.getTransformFunction()('str');
      } catch(e) {
        throw new Error(`Couldn't evaluate return value of '${this.jsStatement}': ${e.message}`);
      }

      var returnType = (typeof testReturnVal).toUpperCase();
      if (Array.isArray(testReturnVal)) returnType = "array"; // no PlyType to support str.split()
      if (testReturnVal === null) returnType = "NULL";
      if (CustomTransformAction.allowedFnReturnTypes.indexOf(returnType) === -1) {
        throw new Error(`Unsupported return type: ${returnType} from '${this.jsStatement}'`);
      }

      return returnType as PlyType;
    }

    public _fillRefSubstitutions(typeContext: DatasetFullType, inputType: FullType): FullType {
      return inputType;
    }

    protected _getFnHelper(inputType: PlyType, inputFn: ComputeFn): ComputeFn {
      return (d: Datum, c: Datum) => {
        var inV = inputFn(d, c);
        if (inV === null) return null;
        return this.getTransformFunction()(inV);
      }
    }

    protected _getJSHelper(inputType: PlyType, inputJS: string): string {
      var { jsStatement } = this;
      var regex = new RegExp(CustomTransformAction.argumentName, "g");
      jsStatement = `(${CustomTransformAction.argumentName}==null?null:${jsStatement})`;
      return jsStatement.replace(regex, inputJS);
    }

    protected _getSQLHelper(inputType: PlyType, dialect: SQLDialect, inputSQL: string, expressionSQL: string): string {
      throw new Error("Custom transform not supported in SQL");
    }

    private getTransformFunction(): Function {
      return new Function(CustomTransformAction.argumentName, `return ${this.jsStatement}`);
    }
  }

  Action.register(CustomTransformAction);
}
