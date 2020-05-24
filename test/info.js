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

exports.druidVersion = '0.18.0-iap4';
exports.druidHost = `localhost:8082`;
exports.druidContext = {
  timeout: 10000,
  useCache: false,
  populateCache: false,
};

exports.mySqlVersion = '5.7.29';
exports.mySqlHost = `localhost:3306`;
exports.mySqlDatabase = 'datazoo';
exports.mySqlUser = 'datazoo';
exports.mySqlPassword = 'datazoo';

exports.postgresVersion = '9.5.21';
exports.postgresHost = `localhost:5432`;
exports.postgresDatabase = 'datazoo';
exports.postgresUser = 'root';
exports.postgresPassword = 'datazoo';
