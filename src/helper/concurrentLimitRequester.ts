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

import * as Promise from 'any-promise';
import { PlywoodRequester, DatabaseRequest } from 'plywood-base-api';

export interface ConcurrentLimitRequesterParameters<T> {
  requester: PlywoodRequester<T>;
  concurrentLimit: int;
}

interface QueueItem<T> {
  request: DatabaseRequest<T>;
  resolve: (value?: any) => void;
  reject: (error?: any) => void;
}

export function concurrentLimitRequesterFactory<T>(parameters: ConcurrentLimitRequesterParameters<T>): PlywoodRequester<T> {
  let requester = parameters.requester;
  let concurrentLimit = parameters.concurrentLimit || 5;

  if (typeof concurrentLimit !== "number") throw new TypeError("concurrentLimit should be a number");

  let requestQueue: QueueItem<T>[] = [];
  let outstandingRequests: int = 0;

  function requestFinishedOk(v: any): any {
    requestFinished();
    return v;
  }

  function requestFinishedError(e: Error): void {
    requestFinished();
    throw e;
  }

  function requestFinished(): void {
    outstandingRequests--;
    if (!(requestQueue.length && outstandingRequests < concurrentLimit)) return;
    let queueItem = requestQueue.shift();
    outstandingRequests++;
    requester(queueItem.request)
      .then(queueItem.resolve, queueItem.reject)
      .then(requestFinishedOk, requestFinishedError);
  }

  return (request: DatabaseRequest<T>): Promise<any> => {
    if (outstandingRequests < concurrentLimit) {
      outstandingRequests++;
      return requester(request).then(requestFinishedOk, requestFinishedError);
    } else {
      return new Promise((resolve, reject) => {
        requestQueue.push({
          request: request,
          resolve,
          reject
        });
      });
    }
  };
}
