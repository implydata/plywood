var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { Expression, Dataset, $, ply, r } = plywood;

var chronoshift = require("chronoshift");

if (!chronoshift.WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  chronoshift.WallTime.init(tzData.rules, tzData.zones);
}

var wikiDayData = require('../../data/wikipedia');

wikiDayData.forEach(function(d, i) {
  return d['time'] = new Date(d['time']);
}
);

describe.skip("compute native nontrivial data", function() {
  var ds = Dataset.fromJS(wikiDayData);

  it("works in simple agg case", function(testComplete) {
    var ex = ply()
    .apply('Count', '$data.count()')
    .apply('SumAdded', '$data.sum($added)');

    var p = ex.compute({ data: ds });
    return p.then(function(v) {
      expect(v.toJS()).to.deep.equal([
        {
          "Count": 28673,
          "SumAdded": 6686857
        }
      ]);
      return testComplete();
    }
    ).done();
  });

  it("works in simple split case (small dimension)", function(testComplete) {
    var ex = $('data').split('$language', 'Language')
    .apply('Count', '$data.count()')
    .apply('SumAdded', '$data.sum($added)')
    .sort('$SumAdded', 'descending')
    .limit(5);

    var p = ex.compute({ data: ds });
    return p.then(function(v) {
      expect(v.toJS()).to.deep.equal([
        {
          "Count": 15316,
          "Language": "en",
          "SumAdded": 2086462
        },
        {
          "Count": 397,
          "Language": "pl",
          "SumAdded": 784245
        },
        {
          "Count": 1512,
          "Language": "sv",
          "SumAdded": 573478
        },
        {
          "Count": 1201,
          "Language": "fr",
          "SumAdded": 458503
        },
        {
          "Count": 687,
          "Language": "ru",
          "SumAdded": 412581
        }
      ]);
      return testComplete();
    }
    ).done();
  });

  it("works in simple split case (large dimension)", function(testComplete) {
    var ex = $('data').split('$page', 'Page')
    .apply('Count', '$data.count()')
    .apply('SumAdded', '$data.sum($added)')
    .sort('$SumAdded', 'descending')
    .limit(5);

    var p = ex.compute({ data: ds });
    return p.then(function(v) {
      expect(v.toJS()).to.deep.equal([
        {
          "Count": 3,
          "Page": "Wikipedysta:Malarz_pl/szablony/Miasto_zagranica_infobox",
          "SumAdded": 617145
        },
        {
          "Count": 1,
          "Page": "User:Tim.landscheidt/Sandbox/Unusually_long_IP_blocks",
          "SumAdded": 164225
        },
        {
          "Count": 1,
          "Page": "Demographics_of_the_United_States",
          "SumAdded": 119910
        },
        {
          "Count": 1,
          "Page": "Usuario:Adolfobrigido/Pruebas",
          "SumAdded": 77338
        },
        {
          "Count": 1,
          "Page": "Pedro_Álvares_Cabral",
          "SumAdded": 68390
        }
      ]);
      return testComplete();
    }
    ).done();
  });

  it("works in simple timeBucket case", function(testComplete) {
    var ex = $('data').split('$time.timeBucket(PT1H, "Asia/Kathmandu")', "Time")// America/Los_Angeles
    .apply('Count', '$data.count()')
    .sort('$Time', 'ascending')
    .limit(2);

    var p = ex.compute({ data: ds });
    return p.then(function(v) {
      expect(v.toJS()).to.deep.equal([
        {
          "Count": 1350,
          "Time": {
            "end": new Date('2013-02-26T00:15:00.000Z'),
            "start": new Date('2013-02-25T23:15:00.000Z'),
            "type": "TIME_RANGE"
          }
        },
        {
          "Count": 1590,
          "Time": {
            "end": new Date('2013-02-26T01:15:00.000Z'),
            "start": new Date('2013-02-26T00:15:00.000Z'),
            "type": "TIME_RANGE"
          }
        }
      ]);
      return testComplete();
    }
    ).done();
  });

  return it("works in with funny aggregates", function(testComplete) {
    var ex = $('data').split('$language', 'Language')
    .apply('Language', '"[" ++ $Language ++ "]"')
    .apply('Count', '$data.count()')
    .apply('CountLT1000', '$Count < 1000')
    .apply('CountGT1000', '$Count > 1000')
    .apply('CountLTE397', '$Count <= 397')
    .apply('CountGTE397', '$Count >= 397')
    .apply('SumAdded', '$data.sum($added)')
    .apply('NegSumAdded', '-$SumAdded')
    .sort('$SumAdded', 'descending')
    .limit(5);

    var p = ex.compute({ data: ds });
    return p.then(function(v) {
      expect(v.toJS()).to.deep.equal([
        {
          "Count": 15316,
          "CountGT1000": true,
          "CountGTE397": true,
          "CountLT1000": false,
          "CountLTE397": false,
          "Language": "[en]",
          "NegSumAdded": -2086462,
          "SumAdded": 2086462
        },
        {
          "Count": 397,
          "CountGT1000": false,
          "CountGTE397": true,
          "CountLT1000": true,
          "CountLTE397": true,
          "Language": "[pl]",
          "NegSumAdded": -784245,
          "SumAdded": 784245
        },
        {
          "Count": 1512,
          "CountGT1000": true,
          "CountGTE397": true,
          "CountLT1000": false,
          "CountLTE397": false,
          "Language": "[sv]",
          "NegSumAdded": -573478,
          "SumAdded": 573478
        },
        {
          "Count": 1201,
          "CountGT1000": true,
          "CountGTE397": true,
          "CountLT1000": false,
          "CountLTE397": false,
          "Language": "[fr]",
          "NegSumAdded": -458503,
          "SumAdded": 458503
        },
        {
          "Count": 687,
          "CountGT1000": false,
          "CountGTE397": true,
          "CountLT1000": true,
          "CountLTE397": false,
          "Language": "[ru]",
          "NegSumAdded": -412581,
          "SumAdded": 412581
        }
      ]);
      return testComplete();
    }
    ).done();
  });
});
