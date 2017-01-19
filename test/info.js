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

const DOCKER_MACHINE = '192.168.99.100';

exports.druidVersion = '0.9.2-rc2';
exports.druidHost = `${DOCKER_MACHINE}:8082`;
exports.druidContext = {
  timeout: 10000,
  groupByStrategy: 'v2',
  useCache: false,
  populateCache: false
};

exports.mySqlVersion = '5.7.13';
exports.mySqlHost = `${DOCKER_MACHINE}:3306`;
exports.mySqlDatabase = 'datazoo';
exports.mySqlUser = 'root';
exports.mySqlPassword = '';

exports.postgresVersion = '9.5.2';
exports.postgresHost = `${DOCKER_MACHINE}:5432`;
exports.postgresDatabase = 'datazoo';
exports.postgresUser = 'root';
exports.postgresPassword = 'datazoo';
