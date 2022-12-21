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

import { Class, Instance } from 'immutable-class';
import { isIP } from 'node:net';
// TODO: Goes in plywood.js after compile. have to figure out why this disappears after compile
// const { isIP } = require('net');

export interface IpValue {
  ip: string;
}

export interface IpJS {
  ip: string;
}

export class Ip implements Instance<IpValue, IpJS> {
  static type = 'IP';
  private readonly ip: string;

  static isIp(candidate: any): candidate is Ip {
    return isIP(candidate) !== 0;
  }

  static fromString(ipString: string): Ip {
    return new Ip({
      ip: ipString,
    });
  }

  static fromJS(parameters: IpJS): Ip {
    if (typeof parameters !== 'object') {
      throw new Error('unrecognizable Ip');
    }
    return new Ip({
      ip: parameters.ip,
    });
  }

  constructor(parameters: IpValue) {
    this.ip = parameters.ip;
  }

  public toJSON(): IpJS {
    return this.toJS();
  }

  public equals(other: Instance<IpValue, IpJS>): boolean {
    throw new Error('Method not implemented.');
  }

  public valueOf(): IpJS {
    return {
      ip: this.ip,
    };
  }

  public toJS(): IpJS {
    const js: IpJS = {
      ip: this.ip,
    };

    return js;
  }

  public toString(): string {
    return this.ip;
  }
}

// eslint-disable-next-line unused-imports/no-unused-vars
const check: Class<IpValue, IpJS> = Ip;
