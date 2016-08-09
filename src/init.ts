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

// --------------------------------------------------------

interface PEGParserOptions {
  cache?: boolean;
  allowedStartRules?: string;
  output?: string;
  optimize?: string;
  plugins?: any;
  [key: string]: any;
}

interface PEGParser {
  parse: (str: string, options?: PEGParserOptions) => any;
}

interface PEGParserFactory {
  (plywood: any, chronoshift: any): PEGParser;
}

// --------------------------------------------------------




var expressionParser: PEGParser;
var plyqlParser: PEGParser;


export const version = '###_VERSION_###';

// The default timezone within which dates in expressions are parsed
export var defaultParserTimezone: Timezone = Timezone.UTC;



export function safeAdd(num: number, delta: number): number {
  var stringDelta = String(delta);
  var dotIndex = stringDelta.indexOf(".");
  if (dotIndex === -1 || stringDelta.length === 18) {
    return num + delta;
  } else {
    var scale = Math.pow(10, stringDelta.length - dotIndex - 1);
    return (num * scale + delta * scale) / scale;
  }
}

export function continuousFloorExpression(variable: string, floorFn: string, size: number, offset: number): string {
  var expr = variable;
  if (offset !== 0) {
    expr = expr + " - " + offset;
  }
  if (offset !== 0 && size !== 1) {
    expr = "(" + expr + ")";
  }
  if (size !== 1) {
    expr = expr + " / " + size;
  }
  expr = floorFn + "(" + expr + ")";
  if (size !== 1) {
    expr = expr + " * " + size;
  }
  if (offset !== 0) {
    expr = expr + " + " + offset;
  }
  return expr;
}

// Taken from: https://stackoverflow.com/questions/31089801/extending-error-in-javascript-with-es6-syntax
export class ExtendableError extends Error {
  public stack: string;

  constructor(message: string) {
    super(message);
    this.name = (this.constructor as any).name;
    this.message = message;
    if (typeof (Error as any).captureStackTrace === 'function') {
      (Error as any).captureStackTrace(this, this.constructor);
    } else {
      this.stack = (new Error(message) as any).stack;
    }
  }
}

