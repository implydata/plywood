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

import { PlyType } from '../types';

export interface IpValue {
  ip: string;
}

export interface IpJS {
  type: PlyType;
  ip: string;
}

export class Ip implements Instance<IpValue, IpJS> {
  static type = 'IP';
  readonly ip: string;

  static isIp(candidate: any): candidate is Ip {
    if (String(candidate).includes('/')) {
      candidate = candidate.split('/')[0];
    }

    return (
      // IPv4
      /^(\d+)\.(\d+?)\.(\d+?)\.(\d+?)$/.test(candidate) ||
      // IPv6
      /^([\da-zA-Z]+):([\da-zA-Z]+):([\da-zA-Z]+):([\da-zA-Z]+):*$/.test(candidate)
    );
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

  public equals(other: Ip | undefined): boolean {
    return other instanceof Ip && other.ip === this.ip;
  }

  public valueOf(): IpJS {
    return {
      type: Ip.type as PlyType,
      ip: this.ip,
    };
  }

  public toJS(): IpJS {
    const js: IpJS = {
      type: Ip.type as PlyType,
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
