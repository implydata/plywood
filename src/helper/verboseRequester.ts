/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2020 Imply Data, Inc.
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

import { DatabaseRequest, PlywoodRequester } from 'plywood-base-api';

export interface CallbackParameters {
  name?: string;
  queryNumber?: number;
  query?: any;
  context?: any;
  time?: number;
  data?: any[];
  error?: Error;
}

export interface VerboseRequesterParameters<T> {
  requester: PlywoodRequester<T>;
  name?: string;
  printLine?: (line: string) => void;
  onQuery?: (param: CallbackParameters) => void;
  onSuccess?: (param: CallbackParameters) => void;
  onError?: (param: CallbackParameters) => void;
}

export function verboseRequesterFactory<T>(
  parameters: VerboseRequesterParameters<T>,
): PlywoodRequester<any> {
  const requester = parameters.requester;
  const myName = parameters.name || 'rq' + String(Math.random()).substr(2, 5);

  // Back compat.
  if ((parameters as any).preQuery) {
    console.warn('verboseRequesterFactory option preQuery has been renamed to onQuery');
    parameters.onQuery = (parameters as any).preQuery;
  }

  const printLine =
    parameters.printLine ||
    ((line: string): void => {
      console['log'](line);
    });

  const onQuery =
    parameters.onQuery ||
    ((param: CallbackParameters): void => {
      printLine('vvvvvvvvvvvvvvvvvvvvvvvvvv');
      const ctx = param.context ? ` [context: ${JSON.stringify(param.context)}]` : '';
      printLine(`Requester ${param.name} sending query ${param.queryNumber}:${ctx}`);
      printLine(JSON.stringify(param.query, null, 2));
      printLine('^^^^^^^^^^^^^^^^^^^^^^^^^^');
    });

  const onSuccess =
    parameters.onSuccess ||
    ((param: CallbackParameters): void => {
      printLine('vvvvvvvvvvvvvvvvvvvvvvvvvv');
      printLine(
        `Requester ${param.name} got result from query ${param.queryNumber}: (in ${param.time}ms)`,
      );
      printLine(JSON.stringify(param.data, null, 2));
      printLine('^^^^^^^^^^^^^^^^^^^^^^^^^^');
    });

  const onError =
    parameters.onError ||
    ((param: CallbackParameters): void => {
      printLine('vvvvvvvvvvvvvvvvvvvvvvvvvv');
      printLine(
        `Requester ${param.name} got error in query ${param.queryNumber}: ${param.error.message} (in ${param.time}ms)`,
      );
      printLine('^^^^^^^^^^^^^^^^^^^^^^^^^^');
    });

  let curQueryNumber: int = 0;
  return (request: DatabaseRequest<any>) => {
    curQueryNumber++;
    const myQueryNumber = curQueryNumber;
    onQuery({
      name: myName,
      queryNumber: myQueryNumber,
      query: request.query,
      context: request.context,
    });

    const startTime = Date.now();
    const stream = requester(request);

    let errorSeen = false;
    stream.on('error', (error: Error) => {
      errorSeen = true;
      onError({
        name: myName,
        queryNumber: myQueryNumber,
        query: request.query,
        context: request.context,
        time: Date.now() - startTime,
        error,
      });
    });

    const data: any[] = [];
    stream.on('data', (datum: any) => {
      data.push(JSON.parse(JSON.stringify(datum)));
    });
    stream.on('end', () => {
      if (errorSeen) return;
      onSuccess({
        name: myName,
        queryNumber: myQueryNumber,
        query: request.query,
        context: request.context,
        time: Date.now() - startTime,
        data,
      });
    });

    return stream;
  };
}
