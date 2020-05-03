/*
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



export function promiseWhile(condition: () => boolean, action: () => Promise<any>): Promise<any> {
  let loop = (): Promise<any> => {
    if (!condition()) return Promise.resolve(null);
    return Promise.resolve(action()).then(loop);
  };

  return Promise.resolve(null).then(loop);
}
