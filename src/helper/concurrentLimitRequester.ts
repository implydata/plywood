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

import { PassThrough } from 'readable-stream';
import { PlywoodRequester, DatabaseRequest } from 'plywood-base-api';

export interface ConcurrentLimitRequesterParameters<T> {
  requester: PlywoodRequester<T>;
  concurrentLimit: int;
}

interface QueueItem<T> {
  request: DatabaseRequest<T>;
  stream: PassThrough;
}

export function concurrentLimitRequesterFactory<T>(parameters: ConcurrentLimitRequesterParameters<T>): PlywoodRequester<T> {
  let requester = parameters.requester;
  let concurrentLimit = parameters.concurrentLimit || 5;

  if (typeof concurrentLimit !== "number") throw new TypeError("concurrentLimit should be a number");

  let requestQueue: QueueItem<T>[] = [];
  let outstandingRequests: int = 0;

  function requestFinished(): void {
    outstandingRequests--;
    if (!(requestQueue.length && outstandingRequests < concurrentLimit)) return;
    let queueItem = requestQueue.shift();
    outstandingRequests++;

    const stream = requester(queueItem.request);
    stream.on('end', requestFinished);
    stream.pipe(queueItem.stream);
  }

  return (request: DatabaseRequest<T>) => {
    if (outstandingRequests < concurrentLimit) {
      outstandingRequests++;
      const stream = requester(request);
      stream.on('end', requestFinished);
      return stream;
    } else {
      const stream = new PassThrough({});
      requestQueue.push({
        request,
        stream
      });
      return stream;
    }
  };
}
