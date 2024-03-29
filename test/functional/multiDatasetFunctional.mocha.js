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

const { expect } = require('chai');

const { druidRequesterFactory } = require('plywood-druid-requester');
const { mySqlRequesterFactory } = require('plywood-mysql-requester');

const plywood = require('../plywood');

const { External, TimeRange, $, ply, basicExecutorFactory, verboseRequesterFactory } = plywood;

const utils = require('../utils');
const info = require('../info');

const druidRequester = druidRequesterFactory({
  host: info.druidHost,
});

const mySqlRequester = mySqlRequesterFactory({
  host: info.mySqlHost,
  database: info.mySqlDatabase,
  user: info.mySqlUser,
  password: info.mySqlPassword,
});

// druidRequester = verboseRequesterFactory({
//  requester: druidRequester
// });
// mySqlRequester = verboseRequesterFactory({
//  requester: mySqlRequester
// });

const attributes = [
  { name: 'time', type: 'TIME' },
  { name: 'sometimeLater', type: 'TIME' },
  { name: 'channel', type: 'STRING' },
  { name: 'cityName', type: 'STRING' },
  { name: 'comment', type: 'STRING' },
  { name: 'commentLength', type: 'NUMBER' },
  { name: 'countryIsoCode', type: 'STRING' },
  { name: 'countryName', type: 'STRING' },
  { name: 'isAnonymous', type: 'BOOLEAN' },
  { name: 'isMinor', type: 'BOOLEAN' },
  { name: 'isNew', type: 'BOOLEAN' },
  { name: 'isRobot', type: 'BOOLEAN' },
  { name: 'isUnpatrolled', type: 'BOOLEAN' },
  { name: 'metroCode', type: 'STRING' },
  { name: 'namespace', type: 'STRING' },
  { name: 'page', type: 'STRING' },
  { name: 'regionIsoCode', type: 'STRING' },
  { name: 'regionName', type: 'STRING' },
  { name: 'user', type: 'STRING' },
  // { name: "userChars", type: 'SET/STRING' },
  { name: 'count', type: 'NUMBER' },
  { name: 'delta', type: 'NUMBER' },
  { name: 'min_delta', type: 'NUMBER' },
  { name: 'max_delta', type: 'NUMBER' },
  { name: 'deltaByTen', type: 'NUMBER' },
  { name: 'added', type: 'NUMBER' },
  { name: 'deleted', type: 'NUMBER' },
];

const mixedExecutor = basicExecutorFactory({
  datasets: {
    wiki_druid: External.fromJS(
      {
        engine: 'druid',
        source: 'wikipedia',
        timeAttribute: 'time',
        context: info.druidContext,
        attributes,
        filter: $('time').overlap(
          TimeRange.fromJS({
            start: new Date('2015-09-12T00:00:00Z'),
            end: new Date('2015-09-13T00:00:00Z'),
          }),
        ),
        version: info.druidVersion,
      },
      druidRequester,
    ),
    wiki_mysql: External.fromJS(
      {
        engine: 'mysql',
        source: 'wikipedia',
        attributes,
      },
      mySqlRequester,
    ),
  },
});

describe('Multi Dataset Functional', function () {
  this.timeout(10000);

  // ToDo: make this work
  it.skip('works in basic case', () => {
    const ex = ply()
      .apply('wiki_druid', $('wiki_druid').filter($('channel').is('en')))
      .apply('TotalAddedDruid', '$wiki_druid.sum($added)')
      .apply('wiki_mysql', $('wiki_mysql').filter($('channel').is('en')))
      .apply('TotalAddedMySQL', '$wiki_mysql.sum($added)');

    return mixedExecutor(ex).then(result => {
      expect(result.toJS().data).to.deep.equal([]);
    });
  });

  it('mixed split case', () => {
    const ex = $('wiki_mysql')
      .split('$channel', 'Channel')
      .apply('TotalAddedMySQL', '$wiki_mysql.sum($added)')
      .sort('$TotalAddedMySQL', 'descending')
      .limit(3)
      .apply('wiki_druid', $('wiki_druid').filter('$channel == $Channel'))
      .apply(
        'Namespaces',
        $('wiki_druid')
          .split('$namespace', 'Namespace')
          .apply('TotalAddedDruid', '$wiki_druid.sum($added)')
          .sort('$TotalAddedDruid', 'descending')
          .limit(3),
      );

    return mixedExecutor(ex).then(result => {
      expect(result.toJS().data).to.deep.equal([
        {
          Channel: 'en',
          Namespaces: {
            attributes: [
              {
                name: 'Namespace',
                type: 'STRING',
              },
              {
                name: 'TotalAddedDruid',
                type: 'NUMBER',
              },
            ],
            data: [
              {
                Namespace: 'Main',
                TotalAddedDruid: 11594002,
              },
              {
                Namespace: 'User talk',
                TotalAddedDruid: 9210976,
              },
              {
                Namespace: 'Wikipedia',
                TotalAddedDruid: 4720291,
              },
            ],
            keys: ['Namespace'],
          },
          TotalAddedMySQL: 32553107,
        },
        {
          Channel: 'it',
          Namespaces: {
            attributes: [
              {
                name: 'Namespace',
                type: 'STRING',
              },
              {
                name: 'TotalAddedDruid',
                type: 'NUMBER',
              },
            ],
            data: [
              {
                Namespace: 'Discussioni utente',
                TotalAddedDruid: 5938398,
              },
              {
                Namespace: 'Main',
                TotalAddedDruid: 1545491,
              },
              {
                Namespace: 'Utente',
                TotalAddedDruid: 97907,
              },
            ],
            keys: ['Namespace'],
          },
          TotalAddedMySQL: 7852924,
        },
        {
          Channel: 'fr',
          Namespaces: {
            attributes: [
              {
                name: 'Namespace',
                type: 'STRING',
              },
              {
                name: 'TotalAddedDruid',
                type: 'NUMBER',
              },
            ],
            data: [
              {
                Namespace: 'Main',
                TotalAddedDruid: 3830175,
              },
              {
                Namespace: 'Discussion utilisateur',
                TotalAddedDruid: 1381838,
              },
              {
                Namespace: 'Projet',
                TotalAddedDruid: 639063,
              },
            ],
            keys: ['Namespace'],
          },
          TotalAddedMySQL: 7050247,
        },
      ]);
    });
  });
});
