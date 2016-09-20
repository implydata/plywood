/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
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

var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { Expression, $, r, ply, Set, Dataset, External, ExternalExpression, fillExpressionExternalAlteration } = plywood;

function stringExternals(key, value) {
  if (value && value.engine) {
    return `External`;
  }
  return value;
}

var diamonds = External.fromJS({
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
    { name: 'carat', type: 'NUMBER' },
    { name: 'height_bucket', type: 'NUMBER' },
    { name: 'price', type: 'NUMBER', unsplitable: true },
    { name: 'tax', type: 'NUMBER', unsplitable: true },
    { name: 'vendor_id', special: 'unique', unsplitable: true }
  ],
  allowEternity: true,
  allowSelectQueries: true
});

describe("evaluate step", () => {

  it('works in basic case', () => {
    var diamondEx = new ExternalExpression({ external: diamonds });

    var ex1 = ply()
      .apply('diamonds', diamondEx)
      .apply('Total', '$diamonds.count()')
      .apply('TotalX2', '$Total * 2')
      .apply('SomeSplit', $('diamonds').split('$cut:STRING', 'Cut').limit(10))
      .apply('SomeNestedSplit',
        $('diamonds').split('$color:STRING', 'Color')
          .limit(10)
          .apply('SubSplit', $('diamonds').split('$cut:STRING', 'SubCut').limit(5))
      );

    var ex2 = ex1.simplify();

    var readyExternals = ex2.getReadyExternals();
    expect(JSON.parse(JSON.stringify(readyExternals, stringExternals))).to.deep.equal({
      "0": [
        {
          "external": "External",
          "index": 0,
          "key": ""
        },
        {
          "external": "External",
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
          "result": [
            {
              "Cut": "some_cut"
            }
          ]
        },
        {
          "expressionAlterations": {
            "1": {
              "external": "External",
              "result": [
                {
                  "Color": "some_color"
                }
              ]
            }
          },
          "index": 0,
          "key": "SomeNestedSplit"
        }
      ]
    });

    var ex3 = ex2.applyReadyExternals(readyExternals);
    expect(JSON.parse(JSON.stringify(ex3, stringExternals)), 'E3').to.deep.equal({
      "op": "literal",
      "type": "DATASET",
      "value": [
        {
          "SomeNestedSplit":  [
            {
              "Color": "some_color",
              "SubSplit": "External"
            }
          ],
          "SomeSplit": [
            {
              "Cut": "some_cut"
            }
          ],
          "Total": 4,
          "TotalX2": 4
        }
      ]
    });

    // ---------------------

    var readyExternals = ex3.getReadyExternals();
    expect(JSON.parse(JSON.stringify(readyExternals, stringExternals)), 'E4').to.deep.equal({
      "0": [
        {
          "datasetAlterations": [
            {
              "external": "External",
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
              "result": [
                {
                  "SubCut": "some_cut"
                }
              ]
            }
          ],
          "index": 0,
          "key": "SomeNestedSplit"
        }
      ]
    });

    var ex4 = ex3.applyReadyExternals(readyExternals);
    expect(JSON.parse(JSON.stringify(ex4, stringExternals)), 'E6').to.deep.equal({
      "op": "literal",
      "type": "DATASET",
      "value": [
        {
          "SomeNestedSplit": [
            {
              "Color": "some_color",
              "SubSplit": [
                {
                  "SubCut": "some_cut"
                }
              ]
            }
          ],
          "SomeSplit": [
            {
              "Cut": "some_cut"
            }
          ],
          "Total": 4,
          "TotalX2": 4
        }
      ]
    });

    // ---------------------

    var readyExternals = ex4.getReadyExternals();
    expect(JSON.parse(JSON.stringify(readyExternals, stringExternals)), 'E7').to.deep.equal({}); // all done

  });

  // it('works in filter sub query', () => {
  //   var diamondEx = new ExternalExpression({ external: diamonds });
  //
  //   var ex1 = ply()
  //     .apply('diamonds', diamondEx)
  //     .apply('Total', '$diamonds.count()')
  //     .apply('TotalX2', '$Total * 2')
  //     .apply('SomeSplit', $('diamonds').split('$cut:STRING', 'Cut').limit(10))
  //     .apply('SomeNestedSplit',
  //       $('diamonds').split('$color:STRING', 'Color')
  //         .limit(10)
  //         .apply('SubSplit', $('diamonds').split('$cut:STRING', 'SubCut').limit(5))
  //     );
  //
  //   var ex2 = ex1.simplify();
  //
  //   var readyExternals = ex2.getReadyExternals();
  //   readyExternals = JSON.parse(JSON.stringify(readyExternals, stringExternals));
  //   expect(readyExternals).to.deep.equal({
  //     "0": [
  //       {
  //         "external": "External",
  //         "index": 0,
  //         "key": "Total"
  //       },
  //       {
  //         "external": "External",
  //         "index": 0,
  //         "key": "TotalX2"
  //       },
  //       {
  //         "external": "External",
  //         "index": 0,
  //         "key": "SomeSplit"
  //       },
  //       {
  //         "expressionAlterations": {
  //           "1": "External"
  //         },
  //         "index": 0,
  //         "key": "SomeNestedSplit"
  //       }
  //     ]
  //   })
  // });

});
