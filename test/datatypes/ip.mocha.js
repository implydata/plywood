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

const { expect } = require('chai');

const { testImmutableClass } = require('immutable-class-tester');

const plywood = require('../plywood');

const { $, ply, r, Ip } = plywood;

describe('Ip', () => {
  it('is immutable class', () => {
    testImmutableClass(Ip, [
      {
        ip: '192.0.1.2/32',
        type: 'IP',
      },
      {
        ip: '192.0.1.2',
        type: 'IP',
      },
    ]);
  });

  it('should convert from string', () => {
    const ip = Ip.fromString('192.0.1.2/32');
    expect(ip instanceof Ip).equals(true);
    expect(ip.toJS()).to.deep.equals({
      ip: '192.0.1.2/32',
      type: 'IP',
    });
  });

  it('should convert to string', () => {
    const ip = Ip.fromJS({
      ip: '192.0.1.2/32',
      type: 'IP',
    });

    expect(ip.toString()).equal('192.0.1.2/32');
  });

  describe('isIp', () => {
    it('should work for ipv4', function () {
      expect(Ip.isIp('192.0.1.2')).equals(true);
    });

    it('should work for ipv6', function () {
      expect(Ip.isIp('2001:4d98:bffb:ff01::')).equals(true);
    });

    it('should work for ipv6 range', function () {
      expect(Ip.isIp('2001:4d98:bffb:ff01::/64')).equals(true);
    });

    it('should work for ipv4 range', function () {
      expect(Ip.isIp('192.0.1.2/32')).equals(true);
    });
  });

  describe('equals', () => {
    it('should work for ipv4', function () {
      expect(Ip.fromString('192.0.1.2').equals(Ip.fromString('192.0.1.2'))).equals(true);
      expect(Ip.fromString('192.0.1.2').equals(Ip.fromString('192.0.1.3'))).equals(false);
    });

    it('should work for ipv6', function () {
      expect(
        Ip.fromString('2001:4d98:bffb:ff01::').equals(Ip.fromString('2001:4d98:bffb:ff01::')),
      ).equals(true);
      expect(
        Ip.fromString('2001:4d98:bffb:ff01::').equals(Ip.fromString('2002:4d98:bffb:ff01::')),
      ).equals(false);
    });

    it('should work for ipv6 range', function () {
      expect(
        Ip.fromString('2001:4d98:bffb:ff01::/64').equals(Ip.fromString('2001:4d98:bffb:ff01::/64')),
      ).equals(true);
      expect(
        Ip.fromString('2001:4d98:bffb:ff01::/64').equals(Ip.fromString('2002:4d98:bffb:ff01::/64')),
      ).equals(false);
    });

    it('should work for ipv4 range', function () {
      expect(Ip.fromString('192.0.1.2/32').equals(Ip.fromString('192.0.1.2/32'))).equals(true);
      expect(Ip.fromString('192.0.1.2/32').equals(Ip.fromString('192.0.1.3/32'))).equals(false);
    });
  });
});
