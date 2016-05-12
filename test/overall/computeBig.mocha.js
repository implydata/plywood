var { expect } = require("chai");
var fs = require('fs');
var path = require('path');

var plywood = require('../../build/plywood');
var { Expression, Dataset, $, ply, r, helper } = plywood;

var chronoshift = require("chronoshift");

if (!chronoshift.WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  chronoshift.WallTime.init(tzData.rules, tzData.zones);
}

var rawData = fs.readFileSync(path.join(__dirname, '../../resources/wikipedia-sampled.json'), 'utf-8');
var wikiDayData = helper.parseJSON(rawData);

wikiDayData.forEach((d, i) => {
  d['time'] = new Date(d['time']);
  d['sometimeLater'] = new Date(d['sometimeLater']);
  delete d['userChars'];
});

describe("compute native nontrivial data", function() {
  this.timeout(20000);

  var ds = Dataset.fromJS(wikiDayData);

  it("works in simple agg case", (testComplete) => {
    var ex = ply()
      .apply('Count', '$data.count()')
      .apply('SumAdded', '$data.sum($added)');

    ex.compute({ data: ds })
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Count": 39244,
            "SumAdded": 9385573
          }
        ]);
        testComplete();
      })
      .done();
  });

  it("works in with a filter == null", (testComplete) => {
    var ex = $('data').filter('$countryName == null').count();

    ex.compute({ data: ds })
      .then((v) => {
        expect(v).to.equal(35445);
        testComplete();
      })
      .done();
  });

  it("works in with a filter overlap null", (testComplete) => {
    var ex = $('data').filter($('countryName').overlap([null])).count();

    ex.compute({ data: ds })
      .then((v) => {
        expect(v).to.equal(35445);
        testComplete();
      })
      .done();
  });

  it("works in simple split case (small dimension)", (testComplete) => {
    var ex = $('data').split('$countryName', 'CountryName')
      .apply('Count', '$data.count()')
      .apply('SumAdded', '$data.sum($added)')
      .sort('$SumAdded', 'descending')
      .limit(5);

    ex.compute({ data: ds })
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Count": 35445,
            "CountryName": null,
            "SumAdded": 8761516
          },
          {
            "Count": 69,
            "CountryName": "Colombia",
            "SumAdded": 60398
          },
          {
            "Count": 194,
            "CountryName": "Russia",
            "SumAdded": 50561
          },
          {
            "Count": 528,
            "CountryName": "United States",
            "SumAdded": 44433
          },
          {
            "Count": 256,
            "CountryName": "Italy",
            "SumAdded": 41073
          }
        ]);
        testComplete();
      })
      .done();
  });

  it("works in simple split case (large dimension)", (testComplete) => {
    var ex = $('data').split('$page', 'Page')
      .apply('Count', '$data.count()')
      .apply('SumAdded', '$data.sum($added)')
      .sort('$SumAdded', 'descending')
      .limit(5);

    ex.compute({ data: ds })
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Count": 1,
            "Page": "User:QuackGuru/Electronic cigarettes 1",
            "SumAdded": 199818
          },
          {
            "Count": 1,
            "Page": "Обсуждение участника:पाणिनि/Архив-5",
            "SumAdded": 102719
          },
          {
            "Count": 1,
            "Page": "Campeche",
            "SumAdded": 94187
          },
          {
            "Count": 1,
            "Page": "Équipe de Pologne de football à la Coupe du monde 1986",
            "SumAdded": 92182
          },
          {
            "Count": 1,
            "Page": "Адвокат",
            "SumAdded": 89385
          }
        ]);
        testComplete();
      })
      .done();
  });

  it("works in simple timeBucket case", (testComplete) => {
    var ex = $('data').split('$time.timeBucket(PT1H, "Asia/Kathmandu")', "Time")// America/Los_Angeles
      .apply('Count', '$data.count()')
      .sort('$Time', 'ascending')
      .limit(2);

    ex.compute({ data: ds })
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Count": 556,
            "Time": {
              "end": new Date('2015-09-12T01:15:00.000Z'),
              "start": new Date('2015-09-12T00:15:00.000Z'),
              "type": "TIME_RANGE"
            }
          },
          {
            "Count": 1129,
            "Time": {
              "end": new Date('2015-09-12T02:15:00.000Z'),
              "start": new Date('2015-09-12T01:15:00.000Z'),
              "type": "TIME_RANGE"
            }
          }
        ]);
        testComplete();
      })
      .done();
  });

  it("works in with funny aggregates", (testComplete) => {
    var ex = $('data').split('$countryName', 'CountryName')
      .apply('LabelCountry', '"[" ++ $CountryName ++ "]"')
      .apply('Count', '$data.count()')
      .apply('CountLT1000', '$Count < 1000')
      .apply('CountGT1000', '$Count > 1000')
      .apply('CountLTE397', '$Count <= 397')
      .apply('CountGTE397', '$Count >= 397')
      .apply('SumAdded', '$data.sum($added)')
      .apply('NegSumAdded', '-$SumAdded')
      .apply('DistinctCity', '$data.countDistinct($cityName)')
      .sort('$SumAdded', 'descending')
      .limit(5);

    ex.compute({ data: ds })
      .then((v) => {
        expect(v.toJS()).to.deep.equal([
          {
            "Count": 35445,
            "CountGT1000": true,
            "CountGTE397": true,
            "CountLT1000": false,
            "CountLTE397": false,
            "CountryName": null,
            "DistinctCity": 1,
            "LabelCountry": null,
            "NegSumAdded": -8761516,
            "SumAdded": 8761516
          },
          {
            "Count": 69,
            "CountGT1000": false,
            "CountGTE397": false,
            "CountLT1000": true,
            "CountLTE397": true,
            "CountryName": "Colombia",
            "DistinctCity": 8,
            "LabelCountry": "[Colombia]",
            "NegSumAdded": -60398,
            "SumAdded": 60398
          },
          {
            "Count": 194,
            "CountGT1000": false,
            "CountGTE397": false,
            "CountLT1000": true,
            "CountLTE397": true,
            "CountryName": "Russia",
            "DistinctCity": 36,
            "LabelCountry": "[Russia]",
            "NegSumAdded": -50561,
            "SumAdded": 50561
          },
          {
            "Count": 528,
            "CountGT1000": false,
            "CountGTE397": true,
            "CountLT1000": true,
            "CountLTE397": false,
            "CountryName": "United States",
            "DistinctCity": 252,
            "LabelCountry": "[United States]",
            "NegSumAdded": -44433,
            "SumAdded": 44433
          },
          {
            "Count": 256,
            "CountGT1000": false,
            "CountGTE397": false,
            "CountLT1000": true,
            "CountLTE397": true,
            "CountryName": "Italy",
            "DistinctCity": 78,
            "LabelCountry": "[Italy]",
            "NegSumAdded": -41073,
            "SumAdded": 41073
          }
        ]);
        testComplete();
      })
      .done();
  });
});
