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

import { PlywoodRequester, DatabaseRequest } from 'plywood-base-api';
import { PassThrough } from 'readable-stream';

export interface RetryRequesterParameters<T> {
  requester: PlywoodRequester<T>;
  delay?: number;
  retry?: int;
  retryOnTimeout?: boolean;
}

export function retryRequesterFactory<T>(parameters: RetryRequesterParameters<T>): PlywoodRequester<T> {
  let requester = parameters.requester;
  let delay = parameters.delay || 500;
  let retry = parameters.retry || 3;
  let retryOnTimeout = Boolean(parameters.retryOnTimeout);

  if (typeof delay !== "number") throw new TypeError("delay should be a number");
  if (typeof retry !== "number") throw new TypeError("retry should be a number");

  return (request: DatabaseRequest<T>) => {
    const output = new PassThrough({ objectMode: true });

    let tries = 0;

    function tryRequest() {
      tries++;
      let seenData = false;
      let rs = requester(request);
      rs.on('error', (e: Error) => {
        if (seenData || tries > retry || (e.message === "timeout" && !retryOnTimeout)) {
          rs.unpipe(output);
          output.emit('error', e);
        } else {
          setTimeout(tryRequest, delay);
        }
      });
      rs.on('meta', (m: any) => { output.emit('meta', m); });
      rs.on('data', (d: any) => { seenData = true; });
      rs.pipe(output);
    }

    tryRequest();
    return output;
  };
}
