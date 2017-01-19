/*
 * Copyright 2015-2017 Imply Data, Inc.
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
  public streams: Stream[];
  public canAddStream: boolean;
  public currentStream: Stream;
  public streamIndex: number;

  constructor(options: any) {
    super(options);

    this.streams = options.streams;
    this.canAddStream = Array.isArray(this.streams);
    this.currentStream = null;
    this.streamIndex = 0;

    this._nextStream();
  };

  private _nextStream() {
    this.currentStream = null;
    if (Array.isArray(this.streams) && this.streamIndex < this.streams.length) {
      this.currentStream = this.streams[this.streamIndex++];
    } else if (typeof this.streams === 'function') {
      this.currentStream = this.streams();
    }

    if (this.currentStream == null) {
      this.canAddStream = false;
      this.push(null);
    } else {
      this.currentStream.pipe(this, {end: false});
      this.currentStream.on('error', (e: any) => this.emit('error', e));
      this.currentStream.on('end', this._nextStream.bind(this));
    }
  }

  public addStream(newStream: Stream) {
    if (this.canAddStream) {
      this.streams.push(newStream);
    } else {
      this.emit('error', new Error('Can\'t add stream.'));
    }
  }
}
