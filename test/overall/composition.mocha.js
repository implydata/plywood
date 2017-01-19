/*
 * Copyright 2012-2015 Metamarkets Group Inc.
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
let { $, ply, r, Expression } = plywood;

describe("composition", () => {

  describe("errors", () => {
    it("throws on a nameless apply", () => {
      expect(() => {
        ply().apply('$data.sum($x)');
      }).to.throw('invalid arguments to .apply, did you forget to specify a name?');
    });

    it("throws on an expression in count", () => {
      expect(() => {
        ply().count('$x');
      }).to.throw('.count() should not have arguments, did you want to .filter().count() ?');
    });

  });

  it("works in blank case", () => {
    let ex = ply();
    expect(ex.toJS()).to.deep.equal({
      "op": "literal",
      "type": "DATASET",
      "value": [{}]
    });
  });

  it("works in ref case", () => {
    let ex = $("diamonds");
    expect(ex.toJS()).to.deep.equal({
      "op": "ref",
      "name": "diamonds"
    });
  });

  it("works in timeShift case", () => {
    let ex = Expression._.timeShift('P1D');
    expect(ex.toJS()).to.deep.equal({
      "op": "timeShift",
      "duration": "P1D",
      "step": 1
    });
  });

  it("works in single split case", () => {
    let ex = $('data')
      .split('$page', 'Page', 'd');

    expect(ex.toJS()).to.deep.equal({
      "dataName": "d",
      "expression": {
        "name": "page",
        "op": "ref"
      },
      "name": "Page",
      "op": "split",
      "operand": {
        "name": "data",
        "op": "ref"
      }
    });
  });

  it("works in multi split case", () => {
    let ex = $('data')
      .split({ Page: '$page', User: '$page' }, 'd');

    expect(ex.toJS()).to.deep.equal({
      "dataName": "d",
      "op": "split",
      "operand": {
        "name": "data",
        "op": "ref"
      },
      "splits": {
        "Page": {
          "name": "page",
          "op": "ref"
        },
        "User": {
          "name": "page",
          "op": "ref"
        }
      }
    });
  });

  it("works in semi-realistic case", () => {
    let ex = ply()
      .apply(
        "Diamonds",
        ply()
          .filter($('color').is('D'))
          .apply("priceOver2", $("price").divide(2))
      )
      .apply('Count', $('Diamonds').count())
      .apply('TotalPrice', $('Diamonds').sum('$priceOver2'));

    expect(ex.toJS()).to.deep.equal({
      "expression": {
        "expression": {
          "name": "priceOver2",
          "op": "ref"
        },
        "op": "sum",
        "operand": {
          "name": "Diamonds",
          "op": "ref"
        }
      },
      "name": "TotalPrice",
      "op": "apply",
      "operand": {
        "expression": {
          "op": "count",
          "operand": {
            "name": "Diamonds",
            "op": "ref"
          }
        },
        "name": "Count",
        "op": "apply",
        "operand": {
          "expression": {
            "expression": {
              "expression": {
                "op": "literal",
                "value": 2
              },
              "op": "divide",
              "operand": {
                "name": "price",
                "op": "ref"
              }
            },
            "name": "priceOver2",
            "op": "apply",
            "operand": {
              "expression": {
                "expression": {
                  "op": "literal",
                  "value": "D"
                },
                "op": "is",
                "operand": {
                  "name": "color",
                  "op": "ref"
                }
              },
              "op": "filter",
              "operand": {
                "op": "literal",
                "type": "DATASET",
                "value": [
                  {}
                ]
              }
            }
          },
          "name": "Diamonds",
          "op": "apply",
          "operand": {
            "op": "literal",
            "type": "DATASET",
            "value": [
              {}
            ]
          }
        }
      }
    });
  });

  it("works in semi-realistic case (using parser)", () => {
    let ex = ply()
      .apply("Diamonds", ply().filter("$color == 'D'").apply("priceOver2", "$price/2"))
      .apply('Count', $('Diamonds').count())
      .apply('TotalPrice', $('Diamonds').sum('$priceOver2'));

    expect(ex.toJS()).to.deep.equal({
      "expression": {
        "expression": {
          "name": "priceOver2",
          "op": "ref"
        },
        "op": "sum",
        "operand": {
          "name": "Diamonds",
          "op": "ref"
        }
      },
      "name": "TotalPrice",
      "op": "apply",
      "operand": {
        "expression": {
          "op": "count",
          "operand": {
            "name": "Diamonds",
            "op": "ref"
          }
        },
        "name": "Count",
        "op": "apply",
        "operand": {
          "expression": {
            "expression": {
              "expression": {
                "op": "literal",
                "value": 2
              },
              "op": "divide",
              "operand": {
                "name": "price",
                "op": "ref"
              }
            },
            "name": "priceOver2",
            "op": "apply",
            "operand": {
              "expression": {
                "expression": {
                  "op": "literal",
                  "value": "D"
                },
                "op": "is",
                "operand": {
                  "name": "color",
                  "op": "ref"
                }
              },
              "op": "filter",
              "operand": {
                "op": "literal",
                "type": "DATASET",
                "value": [
                  {}
                ]
              }
            }
          },
          "name": "Diamonds",
          "op": "apply",
          "operand": {
            "op": "literal",
            "type": "DATASET",
            "value": [
              {}
            ]
          }
        }
      }
    });
  });

});
