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

import * as Q from 'q';
import { Expression } from "../expressions/baseExpression";
import { PlywoodValue, Datum } from "../datatypes/index";
import { Environment } from "../actions/baseAction";

export interface Executor {
  (ex: Expression, env?: Environment): Q.Promise<PlywoodValue>;
}

export interface BasicExecutorParameters {
  datasets: Datum;
}

export function basicExecutorFactory(parameters: BasicExecutorParameters): Executor {
  var datasets = parameters.datasets;
  return (ex: Expression, env: Environment = {}) => {
    return ex.compute(datasets, env);
  }
}
