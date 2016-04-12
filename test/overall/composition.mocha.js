var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { $, ply, r, Expression } = plywood;

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
      }).to.throw('.count() should not have arguments, did you want to .filter().count()?');
    });

  });

  it("works in blank case", () => {
    var ex = ply();
    expect(ex.toJS()).to.deep.equal({
      "op": "literal",
      "type": "DATASET",
      "value": [{}]
    });
  });

  it("works in ref case", () => {
    var ex = $("diamonds");
    expect(ex.toJS()).to.deep.equal({
      "op": "ref",
      "name": "diamonds"
    });
  });

  it("it bumps lessThan to time", () => {
    var ex = r("2016").lessThan('$x');
    expect(ex.toJS()).to.deep.equal({
      "action": {
        "action": "lessThan",
        "expression": {
          "name": "x",
          "op": "ref"
        }
      },
      "expression": {
        "op": "literal",
        "type": "TIME",
        "value": new Date('2016-01-01T00:00:00.000Z')
      },
      "op": "chain"
    });
  });

  it("overlap to SET", () => {
    var ex = r("hello").overlap('$x');
    expect(ex.toJS()).to.deep.equal({
      "action": {
        "action": "overlap",
        "expression": {
          "name": "x",
          "op": "ref"
        }
      },
      "expression": {
        "op": "literal",
        "type": "SET",
        "value": {
          "elements": [
            "hello"
          ],
          "setType": "STRING"
        }
      },
      "op": "chain"
    });
  });

  it("works in uber-basic case", () => {
    var ex = ply()
      .apply('five', 5)
      .apply('nine', 9);

    expect(ex.toJS()).to.deep.equal({
      "op": "chain",
      "expression": {
        "op": "literal",
        "type": "DATASET",
        "value": [{}]
      },
      "actions": [
        {
          "action": "apply",
          "name": "five",
          "expression": { "op": "literal", "value": 5 }
        },
        {
          "action": "apply",
          "name": "nine",
          "expression": { "op": "literal", "value": 9 }
        }
      ]
    });
  });

  it("works IN of a set", () => {
    var ex = $("x").in(['A', 'B', 'C']);
    expect(ex.toJS()).to.deep.equal({
      "action": {
        "action": "in",
        "expression": {
          "op": "literal",
          "type": "SET",
          "value": {
            "elements": [
              "A",
              "B",
              "C"
            ],
            "setType": "STRING"
          }
        }
      },
      "expression": {
        "name": "x",
        "op": "ref"
      },
      "op": "chain"
    });
  });

  it("works IN of NumberRange", () => {
    var ex = $("x").in(3, 10);
    expect(ex.toJS()).to.deep.equal({
      "action": {
        "action": "in",
        "expression": {
          "op": "literal",
          "type": "NUMBER_RANGE",
          "value": {
            "end": 10,
            "start": 3
          }
        }
      },
      "expression": {
        "name": "x",
        "op": "ref"
      },
      "op": "chain"
    });
  });

  it("works IN of TimeRange", () => {
    var ex = $("x").in(new Date('2015-03-03Z'), new Date('2015-10-10Z'));
    expect(ex.toJS()).to.deep.equal({
      "action": {
        "action": "in",
        "expression": {
          "op": "literal",
          "type": "TIME_RANGE",
          "value": {
            "end": new Date('2015-10-10Z'),
            "start": new Date('2015-03-03Z')
          }
        }
      },
      "expression": {
        "name": "x",
        "op": "ref"
      },
      "op": "chain"
    });
  });

  it("works IN of TimeRange (as strings)", () => {
    var ex = $("x").in('2015-03-03Z', '2015-10-10Z');
    expect(ex.toJS()).to.deep.equal({
      "action": {
        "action": "in",
        "expression": {
          "op": "literal",
          "type": "TIME_RANGE",
          "value": {
            "end": new Date('2015-10-10Z'),
            "start": new Date('2015-03-03Z')
          }
        }
      },
      "expression": {
        "name": "x",
        "op": "ref"
      },
      "op": "chain"
    });
  });

  it("works IN of TimeRange SET", () => {
    var ex = $("x").in([
      { type: 'TIME_RANGE', start: new Date('2015-03-03Z'), end: new Date('2015-10-10Z') },
      { type: 'TIME_RANGE', start: new Date('2015-11-20Z'), end: new Date('2015-11-25Z') }
    ]);
    expect(ex.toJS()).to.deep.equal({
      "action": {
        "action": "in",
        "expression": {
          "op": "literal",
          "type": "SET",
          "value": {
            "elements": [
              {
                "end": new Date('2015-10-10T00:00:00.000Z'),
                "start": new Date('2015-03-03T00:00:00.000Z')
              },
              {
                "end": new Date('2015-11-25T00:00:00.000Z'),
                "start": new Date('2015-11-20T00:00:00.000Z')
              }
            ],
            "setType": "TIME_RANGE"
          }
        }
      },
      "expression": {
        "name": "x",
        "op": "ref"
      },
      "op": "chain"
    });
  });

  it("works in single split case", () => {
    var ex = $('data')
      .split('$page', 'Page', 'd');

    expect(ex.toJS()).to.deep.equal({
      "action": {
        "action": "split",
        "dataName": "d",
        "expression": {
          "name": "page",
          "op": "ref"
        },
        "name": "Page"
      },
      "expression": {
        "name": "data",
        "op": "ref"
      },
      "op": "chain"
    });
  });

  it("works in multi split case", () => {
    var ex = $('data')
      .split({ Page: '$page', User: '$page' }, 'd');

    expect(ex.toJS()).to.deep.equal({
      "action": {
        "action": "split",
        "dataName": "d",
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
      },
      "expression": {
        "name": "data",
        "op": "ref"
      },
      "op": "chain"
    });
  });

  it("works in semi-realistic case", () => {
    var ex = ply()
      .apply(
        "Diamonds",
        ply()
          .filter($('color').is('D'))
          .apply("priceOver2", $("price").divide(2))
      )
      .apply('Count', $('Diamonds').count())
      .apply('TotalPrice', $('Diamonds').sum('$priceOver2'));

    expect(ex.toJS()).to.deep.equal({
      "actions": [
        {
          "action": "apply",
          "expression": {
            "actions": [
              {
                "action": "filter",
                "expression": {
                  "action": {
                    "action": "is",
                    "expression": {
                      "op": "literal",
                      "value": "D"
                    }
                  },
                  "expression": {
                    "name": "color",
                    "op": "ref"
                  },
                  "op": "chain"
                }
              },
              {
                "action": "apply",
                "expression": {
                  "action": {
                    "action": "divide",
                    "expression": {
                      "op": "literal",
                      "value": 2
                    }
                  },
                  "expression": {
                    "name": "price",
                    "op": "ref"
                  },
                  "op": "chain"
                },
                "name": "priceOver2"
              }
            ],
            "expression": {
              "op": "literal",
              "type": "DATASET",
              "value": [
                {}
              ]
            },
            "op": "chain"
          },
          "name": "Diamonds"
        },
        {
          "action": "apply",
          "expression": {
            "action": {
              "action": "count"
            },
            "expression": {
              "name": "Diamonds",
              "op": "ref"
            },
            "op": "chain"
          },
          "name": "Count"
        },
        {
          "action": "apply",
          "expression": {
            "action": {
              "action": "sum",
              "expression": {
                "name": "priceOver2",
                "op": "ref"
              }
            },
            "expression": {
              "name": "Diamonds",
              "op": "ref"
            },
            "op": "chain"
          },
          "name": "TotalPrice"
        }
      ],
      "expression": {
        "op": "literal",
        "type": "DATASET",
        "value": [
          {}
        ]
      },
      "op": "chain"
    });
  });

  it("works in semi-realistic case (using parser)", () => {
    var ex = ply()
      .apply("Diamonds", ply().filter("$color == 'D'").apply("priceOver2", "$price/2"))
      .apply('Count', $('Diamonds').count())
      .apply('TotalPrice', $('Diamonds').sum('$priceOver2'));

    expect(ex.toJS()).to.deep.equal({
      "actions": [
        {
          "action": "apply",
          "expression": {
            "actions": [
              {
                "action": "filter",
                "expression": {
                  "action": {
                    "action": "is",
                    "expression": {
                      "op": "literal",
                      "value": "D"
                    }
                  },
                  "expression": {
                    "name": "color",
                    "op": "ref"
                  },
                  "op": "chain"
                }
              },
              {
                "action": "apply",
                "expression": {
                  "action": {
                    "action": "divide",
                    "expression": {
                      "op": "literal",
                      "value": 2
                    }
                  },
                  "expression": {
                    "name": "price",
                    "op": "ref"
                  },
                  "op": "chain"
                },
                "name": "priceOver2"
              }
            ],
            "expression": {
              "op": "literal",
              "type": "DATASET",
              "value": [
                {}
              ]
            },
            "op": "chain"
          },
          "name": "Diamonds"
        },
        {
          "action": "apply",
          "expression": {
            "action": {
              "action": "count"
            },
            "expression": {
              "name": "Diamonds",
              "op": "ref"
            },
            "op": "chain"
          },
          "name": "Count"
        },
        {
          "action": "apply",
          "expression": {
            "action": {
              "action": "sum",
              "expression": {
                "name": "priceOver2",
                "op": "ref"
              }
            },
            "expression": {
              "name": "Diamonds",
              "op": "ref"
            },
            "op": "chain"
          },
          "name": "TotalPrice"
        }
      ],
      "expression": {
        "op": "literal",
        "type": "DATASET",
        "value": [
          {}
        ]
      },
      "op": "chain"
    });
  });

  it("multi-value case", () => {
    var ex = Expression.concat(['$a', '$b', '"]"']);

    expect(ex.toJS()).to.deep.equal({
      "actions": [
        {
          "action": "concat",
          "expression": {
            "name": "b",
            "op": "ref"
          }
        },
        {
          "action": "concat",
          "expression": {
            "op": "literal",
            "value": "]"
          }
        }
      ],
      "expression": {
        "name": "a",
        "op": "ref"
      },
      "op": "chain"
    });

  });

});
