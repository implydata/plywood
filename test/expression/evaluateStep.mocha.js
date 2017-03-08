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

const { expect } = require("chai");

let plywood = require('../plywood');
let { Expression, $, r, ply, Set, Dataset, External, ExternalExpression, fillExpressionExternalAlteration } = plywood;

function stringExternals(key, value) {
  if (value && value.engine) {
    return `External`;
  }
  return value;
}

let diamonds = External.fromJS({
  engine: 'druid',
  source: 'diamonds',
  timeAttribute: 'time',
  attributes: [
    { name: 'time', type: 'TIME' },
    { name: 'color', type: 'STRING' },
    { name: 'cut', type: 'STRING' },
    { name: 'isNice', type: 'BOOLEAN' },
    { name: 'tags', type: 'SET/STRING' },
    { name: 'pugs', type: 'SET/STRING' },
    { name: 'carat', type: 'NUMBER', nativeType: 'STRING' },
    { name: 'height_bucket', type: 'NUMBER' },
    { name: 'price', type: 'NUMBER', unsplitable: true },
    { name: 'tax', type: 'NUMBER', unsplitable: true },
    { name: 'vendor_id', type: 'NULL', nativeType: 'hyperUnique', unsplitable: true }
  ],
  allowEternity: true,
  allowSelectQueries: true
});

describe("evaluate step", () => {

  it('works in basic case', () => {
    let diamondEx = new ExternalExpression({ external: diamonds });

    let ex1 = ply()
      .apply('diamonds', diamondEx)
      .apply('Total', '$diamonds.count()')
      .apply('TotalX2', '$Total * 2')
      .apply('SomeSplit', $('diamonds').split('$cut:STRING', 'Cut').limit(10))
      .apply('SomeNestedSplit',
        $('diamonds').split('$color:STRING', 'Color')
          .limit(10)
          .apply('SubSplit', $('diamonds').split('$cut:STRING', 'SubCut').limit(5))
      );

    let ex2 = ex1.simplify();

    let readyExternals = ex2.getReadyExternals();
    expect(JSON.parse(JSON.stringify(readyExternals, stringExternals))).to.deep.equal({
      "0": [
        {
          "external": "External",
          "index": 0,
          "key": ""
        },
        {
          "external": "External",
          "terminal": true,
          "index": 0,
          "key": "SomeSplit"
        },
        {
          "expressionAlterations": {
            "1": {
              "external": "External"
            }
          },
          "index": 0,
          "key": "SomeNestedSplit"
        }
      ]
    });

    fillExpressionExternalAlteration(readyExternals, (external) => `Ex(${external.mode})`);
    expect(JSON.parse(JSON.stringify(readyExternals, stringExternals)), 'E1').to.deep.equal({
      "0": [
        {
          "external": "External",
          "index": 0,
          "key": "",
          "result": "Ex(total)"
        },
        {
          "external": "External",
          "terminal": true,
          "index": 0,
          "key": "SomeSplit",
          "result": "Ex(split)"
        },
        {
          "expressionAlterations": {
            "1": {
              "external": "External",
              "result": "Ex(split)"
            }
          },
          "index": 0,
          "key": "SomeNestedSplit"
        }
      ]
    });

    fillExpressionExternalAlteration(readyExternals, (external) => external.simulateValue(false, []));
    expect(JSON.parse(JSON.stringify(readyExternals, stringExternals)), 'E2').to.deep.equal({
      "0": [
        {
          "external": "External",
          "index": 0,
          "key": "",
          "result": {
            "datum": {
              "Total": 4,
              "TotalX2": 4
            }
          }
        },
        {
          "external": "External",
          "index": 0,
          "key": "SomeSplit",
          "result": {
            "attributes": [
              {
                "name": "Cut",
                "type": "STRING"
              },
              {
                "name": "diamonds",
                "type": "DATASET"
              }
            ],
            "data": [
              {
                "Cut": "some_cut"
              }
            ],
            "keys": [
              "Cut"
            ]
          },
          "terminal": true
        },
        {
          "expressionAlterations": {
            "1": {
              "external": "External",
              "result": {
                "attributes": [
                  {
                    "name": "Color",
                    "type": "STRING"
                  },
                  {
                    "name": "diamonds",
                    "type": "DATASET"
                  }
                ],
                "data": [
                  {
                    "Color": "some_color"
                  }
                ],
                "keys": [
                  "Color"
                ]
              }
            }
          },
          "index": 0,
          "key": "SomeNestedSplit"
        }
      ]
    });

    let ex3 = ex2.applyReadyExternals(readyExternals);
    expect(JSON.parse(JSON.stringify(ex3, stringExternals)), 'E3').to.deep.equal({
      "op": "literal",
      "type": "DATASET",
      "value": {
        "attributes": [
          {
            "name": "diamonds",
            "type": "DATASET"
          },
          {
            "name": "Total",
            "type": "NUMBER"
          },
          {
            "name": "TotalX2",
            "type": "NUMBER"
          },
          {
            "name": "SomeSplit",
            "type": "DATASET"
          },
          {
            "name": "SomeNestedSplit",
            "type": "DATASET"
          }
        ],
        "data": [
          {
            "SomeNestedSplit": {
              "attributes": [
                {
                  "name": "Color",
                  "type": "STRING"
                },
                {
                  "name": "diamonds",
                  "type": "DATASET"
                },
                {
                  "name": "SubSplit",
                  "type": "DATASET"
                }
              ],
              "data": [
                {
                  "Color": "some_color",
                  "SubSplit": "External"
                }
              ],
              "keys": [
                "Color"
              ]
            },
            "SomeSplit": {
              "attributes": [
                {
                  "name": "Cut",
                  "type": "STRING"
                },
                {
                  "name": "diamonds",
                  "type": "DATASET"
                }
              ],
              "data": [
                {
                  "Cut": "some_cut"
                }
              ],
              "keys": [
                "Cut"
              ]
            },
            "Total": 4,
            "TotalX2": 4
          }
        ],
        "keys": []
      }
    });

    // ---------------------

    readyExternals = ex3.getReadyExternals();
    expect(JSON.parse(JSON.stringify(readyExternals, stringExternals)), 'E4').to.deep.equal({
      "0": [
        {
          "datasetAlterations": [
            {
              "external": "External",
              "terminal": true,
              "index": 0,
              "key": "SubSplit"
            }
          ],
          "index": 0,
          "key": "SomeNestedSplit"
        }
      ]
    });

    fillExpressionExternalAlteration(readyExternals, (external) => external.simulateValue(false, []));
    expect(JSON.parse(JSON.stringify(readyExternals, stringExternals)), 'E5').to.deep.equal({
      "0": [
        {
          "datasetAlterations": [
            {
              "external": "External",
              "index": 0,
              "key": "SubSplit",
              "result": {
                "attributes": [
                  {
                    "name": "SubCut",
                    "type": "STRING"
                  },
                  {
                    "name": "diamonds",
                    "type": "DATASET"
                  }
                ],
                "data": [
                  {
                    "SubCut": "some_cut"
                  }
                ],
                "keys": [
                  "SubCut"
                ]
              },
              "terminal": true
            }
          ],
          "index": 0,
          "key": "SomeNestedSplit"
        }
      ]
    });

    let ex4 = ex3.applyReadyExternals(readyExternals);
    expect(JSON.parse(JSON.stringify(ex4, stringExternals)), 'E6').to.deep.equal({
      "op": "literal",
      "type": "DATASET",
      "value": {
        "attributes": [
          {
            "name": "diamonds",
            "type": "DATASET"
          },
          {
            "name": "Total",
            "type": "NUMBER"
          },
          {
            "name": "TotalX2",
            "type": "NUMBER"
          },
          {
            "name": "SomeSplit",
            "type": "DATASET"
          },
          {
            "name": "SomeNestedSplit",
            "type": "DATASET"
          }
        ],
        "data": [
          {
            "SomeNestedSplit": {
              "attributes": [
                {
                  "name": "Color",
                  "type": "STRING"
                },
                {
                  "name": "diamonds",
                  "type": "DATASET"
                },
                {
                  "name": "SubSplit",
                  "type": "DATASET"
                }
              ],
              "data": [
                {
                  "Color": "some_color",
                  "SubSplit": {
                    "attributes": [
                      {
                        "name": "SubCut",
                        "type": "STRING"
                      },
                      {
                        "name": "diamonds",
                        "type": "DATASET"
                      }
                    ],
                    "data": [
                      {
                        "SubCut": "some_cut"
                      }
                    ],
                    "keys": [
                      "SubCut"
                    ]
                  }
                }
              ],
              "keys": [
                "Color"
              ]
            },
            "SomeSplit": {
              "attributes": [
                {
                  "name": "Cut",
                  "type": "STRING"
                },
                {
                  "name": "diamonds",
                  "type": "DATASET"
                }
              ],
              "data": [
                {
                  "Cut": "some_cut"
                }
              ],
              "keys": [
                "Cut"
              ]
            },
            "Total": 4,
            "TotalX2": 4
          }
        ],
        "keys": []
      }
    });

    // ---------------------

    readyExternals = ex4.getReadyExternals();
    expect(JSON.parse(JSON.stringify(readyExternals, stringExternals)), 'E7').to.deep.equal({}); // all done

  });

});
