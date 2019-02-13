/*
 * Copyright 2015-2019 Imply Data, Inc.
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

import { PassThrough, Stream } from 'readable-stream';

export class StreamConcat extends PassThrough {
  public next: () => Stream;
  public currentStream: Stream;
  public streamIndex: number;

  constructor(options: any) {
    super(options);

    this.next = options.next;
    this.currentStream = null;
    this.streamIndex = 0;

    this._nextStream();
  };

  private _nextStream() {
    this.currentStream = null;
    this.currentStream = this.next();

    if (this.currentStream == null) {
      this.push(null);
    } else {
      this.currentStream.pipe(this, {end: false});
      this.currentStream.on('error', (e: any) => this.emit('error', e));
      this.currentStream.on('end', this._nextStream.bind(this));
    }
  }
}
