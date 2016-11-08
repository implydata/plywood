/*
 * Copyright 2016-2016 Imply Data, Inc.
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

import { Timezone } from 'chronoshift';

export type PlyTypeSingleValue = 'NULL' | 'BOOLEAN' | 'NUMBER' | 'TIME' | 'STRING';
export type PlyTypeSimple = PlyTypeSingleValue | 'NUMBER_RANGE' | 'TIME_RANGE' | 'STRING_RANGE' |
  'SET' | 'SET/NULL' | 'SET/BOOLEAN' | 'SET/NUMBER' | 'SET/TIME' | 'SET/STRING' | 'SET/NUMBER_RANGE' | 'SET/TIME_RANGE' | 'SET/STRING_RANGE';

export type PlyType = PlyTypeSimple | 'DATASET';

export interface SimpleFullType {
  type: PlyTypeSimple;
}

export interface DatasetFullType {
  type: 'DATASET';
  datasetType: Lookup<FullType>;
  parent?: DatasetFullType;
}

export type FullType = SimpleFullType | DatasetFullType;

export interface Environment {
  timezone?: Timezone;
}
