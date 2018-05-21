/*
 * Copyright 2016-2018 Imply Data, Inc.
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

import { AttributeInfo, NumberRange } from '../../datatypes/index';
import { Expression } from '../../expressions';
import { DruidFilterBuilder } from './druidFilterBuilder';
import { CustomDruidTransforms } from './druidTypes';


export interface DruidHavingFilterBuilderOptions {
  version: string;
  attributes: AttributeInfo[];
  customTransforms: CustomDruidTransforms;
}

export class DruidHavingFilterBuilder {

  public version: string;
  public attributes: AttributeInfo[];
  public customTransforms: CustomDruidTransforms;

  constructor(options: DruidHavingFilterBuilderOptions) {
    this.version = options.version;
    this.attributes = options.attributes;
    this.customTransforms = options.customTransforms;
  }

  public filterToHavingFilter(filter: Expression): Druid.Having {
    return {
      type: 'filter',
      filter: new DruidFilterBuilder({
        version: this.version,
        rawAttributes: this.attributes,
        timeAttribute: 'z',
        allowEternity: true,
        customTransforms: this.customTransforms
      }).timelessFilterToFilter(filter)
    };
  }

}
