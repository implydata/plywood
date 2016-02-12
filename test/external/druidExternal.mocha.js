var { expect } = require("chai");
var Q = require('q');

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { Expression, External, TimeRange, $, ply, r } = plywood;

var timeFilter = $('time').in(TimeRange.fromJS({
  start: new Date("2013-02-26T00:00:00Z"),
  end: new Date("2013-02-27T00:00:00Z")
}));

var context = {
  wiki: External.fromJS({
    engine: 'druid',
    dataSource: 'wikipedia',
    timeAttribute: 'time',
    attributes: [
      { name: 'time', type: 'TIME' },
      { name: 'language', type: 'STRING' },
      { name: 'page', type: 'STRING' },
      { name: 'added', type: 'NUMBER' },
      { name: 'deleted', type: 'NUMBER' },
      { name: 'inserted', type: 'NUMBER' }
    ],
    filter: timeFilter,
    customAggregations: {
      crazy: {
        accessType: 'getSomeCrazy',
        aggregation: {
          type: 'crazy',
          the: 'borg will rise again',
          activate: false
        }
      },
      stupid: {
        accessType: 'iAmWithStupid',
        aggregation: {
          type: 'stoopid',
          onePlusOne: 3,
          globalWarming: 'hoax'
        }
      }
    }
  })
};

var contextNoApprox = {
  wiki: External.fromJS({
    engine: 'druid',
    dataSource: 'wikipedia',
    timeAttribute: 'time',
    exactResultsOnly: true,
    attributes: [
      { name: 'time', type: 'TIME' },
      { name: 'language', type: 'STRING' },
      { name: 'page', type: 'STRING' },
      { name: 'added', type: 'NUMBER' },
      { name: 'deleted', type: 'NUMBER' },
      { name: 'inserted', type: 'NUMBER' }
    ],
    filter: timeFilter
  })
};

describe("DruidExternal", function() {
  describe("processApply", function() {
    it("breaks up correctly in simple case", function() {
      var ex = ply()
        .apply('wiki', '$wiki')// for now
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$wiki.max($added) - $wiki.min($deleted)');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;

      expect(druidExternal.applies.join('\n')).to.equal(`apply(Count,$wiki:DATASET.count())
apply(Added,$wiki:DATASET.sum($added:NUMBER))
apply(_sd_0,$wiki:DATASET.max($added:NUMBER))
apply(_sd_1,$wiki:DATASET.min($deleted:NUMBER))
apply(Volatile,$_sd_0:NUMBER.subtract($_sd_1:NUMBER))`);
    });

    it("breaks up correctly in absolute cases", function() {
      var ex = ply()
        .apply('wiki', '$wiki')// for now
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('abs', $('AddedByDeleted').absolute());

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;

      expect(druidExternal.applies.join('\n')).to.equal(`apply(_sd_0,$wiki:DATASET.sum($added:NUMBER))
apply(_sd_1,$wiki:DATASET.sum($deleted:NUMBER))
apply(AddedByDeleted,$_sd_0:NUMBER.divide($_sd_1:NUMBER))
apply(abs,$AddedByDeleted:NUMBER.absolute())`);
    });

    it("breaks up correctly in case of duplicate name", function() {
      var ex = ply()
        .apply('wiki', '$wiki')// for now
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$wiki.sum($added) - $wiki.sum($deleted)');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;

      expect(druidExternal.applies.join('\n')).to.equal(`apply(Count,$wiki:DATASET.count())
apply(Added,$wiki:DATASET.sum($added:NUMBER))
apply(_sd_0,$wiki:DATASET.sum($deleted:NUMBER))
apply(Volatile,$Added:NUMBER.subtract($_sd_0:NUMBER))`);
    });

    it("breaks up correctly in case of variable reference", function() {
      var ex = ply()
        .apply('wiki', '$wiki')// for now
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$Added - $wiki.sum($deleted)');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;

      expect(druidExternal.applies.join('\n')).to.equal(`apply(Count,$wiki:DATASET.count())
apply(Added,$wiki:DATASET.sum($added:NUMBER))
apply(_sd_0,$wiki:DATASET.sum($deleted:NUMBER))
apply(Volatile,$Added:NUMBER.subtract($_sd_0:NUMBER))`);
    });

    it("breaks up correctly in complex case", function() {
      var ex = ply()
        .apply('wiki', '$wiki')// for now
        .apply('AddedByDeleted', '$wiki.sum($added) / $wiki.sum($deleted)')
        .apply('DeletedByInserted', '$wiki.sum($deleted) / $wiki.sum($inserted)')
        .apply('Deleted', '$wiki.sum($deleted)');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;

      expect(druidExternal.applies.join('\n')).to.equal(`apply(Deleted,$wiki:DATASET.sum($deleted:NUMBER))
apply(_sd_0,$wiki:DATASET.sum($added:NUMBER))
apply(AddedByDeleted,$_sd_0:NUMBER.divide($Deleted:NUMBER))
apply(_sd_1,$wiki:DATASET.sum($inserted:NUMBER))
apply(DeletedByInserted,$Deleted:NUMBER.divide($_sd_1:NUMBER))`);
    });

    it.skip("breaks up correctly in case of duplicate apply", function() {
      var ex = ply()
        .apply('wiki', '$wiki')// for now
        .apply('Added', '$wiki.sum($added)')
        .apply('Added2', '$wiki.sum($added)')
        .apply('Volatile', '$Added - $wiki.sum($deleted)');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;

      expect(druidExternal.applies.join('\n')).to.equal(`apply(Added,$wiki:DATASET.sum($added:NUMBER))
apply(Added2,$Added:NUMBER)
apply(_sd_0,$wiki:DATASET.sum($deleted:NUMBER))
apply(Volatile,$Added:NUMBER.subtract($_sd_0:NUMBER))`);
    });

    it.skip("breaks up correctly in case of duplicate apply (same name)", function() {
      var ex = ply()
        .apply('wiki', '$wiki')// for now
        .apply('Added', '$wiki.sum($added)')
        .apply('Added', '$wiki.sum($added)')
        .apply('Volatile', '$Added - $wiki.sum($deleted)');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;

      expect(druidExternal.defs.join('\n')).to.equal(`.apply('_sd_0',$wiki:DATASET.sum($deleted:NUMBER))`);

      expect(druidExternal.applies.join('\n')).to.equal(`.apply(Added,$wiki:DATASET.sum($added:NUMBER))
.apply(Volatile,$Added:NUMBER.add($_sd_0:NUMBER.negate()))`);
    });
  });


  describe("simplifies / digests", function() {
    it("a (timeBoundary) total", function() {
      var ex = ply()
        .apply('maximumTime', '$wiki.max($time)')
        .apply('minimumTime', '$wiki.min($time)');

      ex = ex.referenceCheck(context).resolve(context).simplify();
      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "dataSource": "wikipedia",
        "queryType": "timeBoundary"
      });
    });

    it("should properly process a total", function() {
      var ex = ply()
        .apply("wiki", $('wiki', 1).apply('addedTwice', '$added * 2').filter($("language").is('en')))
        .apply('Count', '$wiki.count()')
        .apply('TotalAdded', '$wiki.sum($added)');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          },
          {
            "fieldName": "added",
            "name": "TotalAdded",
            "type": "doubleSum"
          }
        ],
        "dataSource": "wikipedia",
        "filter": {
          "dimension": "language",
          "type": "selector",
          "value": "en"
        },
        "granularity": "all",
        "intervals": [
          "2013-02-26/2013-02-27"
        ],
        "queryType": "timeseries"
      });
    });

    it("inlines a total with no explicit dataset apply", function() {
      var ex = ply()
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply('TotalAddedX2', '$TotalAdded * 2');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      var queryAndPostProcess = druidExternal.getQueryAndPostProcess();
      expect(queryAndPostProcess.query).to.deep.equal({
        "aggregations": [
          {
            "fieldName": "added",
            "name": "TotalAdded",
            "type": "doubleSum"
          }
        ],
        "dataSource": "wikipedia",
        "granularity": "all",
        "intervals": [
          "2013-02-26/2013-02-27"
        ],
        "postAggregations": [
          {
            "fields": [
              {
                "fieldName": "TotalAdded",
                "type": "fieldAccess"
              },
              {
                "type": "constant",
                "value": 2
              }
            ],
            "fn": "*",
            "name": "TotalAddedX2",
            "type": "arithmetic"
          }
        ],
        "queryType": "timeseries"
      });

      expect(queryAndPostProcess.postProcess([
        {
          result: {
            TotalAdded: 5,
            TotalAddedX2: 10
          }
        }
      ]).toJS()).to.deep.equal([
        {
          TotalAdded: 5,
          TotalAddedX2: 10
        }
      ]);
    });

    it("processes a simple split", function() {
      var ex = $('wiki').split("$page", 'Page')
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5);

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          },
          {
            "fieldName": "added",
            "name": "Added",
            "type": "doubleSum"
          }
        ],
        "dataSource": "wikipedia",
        "dimension": {
          "dimension": "page",
          "outputName": "Page",
          "type": "default"
        },
        "granularity": "all",
        "intervals": [
          "2013-02-26/2013-02-27"
        ],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 5
      });
    });

    it("processes a split (no approximate)", function() {
      var ex = $('wiki').split("$page", 'Page')
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5);

      ex = ex.referenceCheck(contextNoApprox).resolve(contextNoApprox).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          },
          {
            "fieldName": "added",
            "name": "Added",
            "type": "doubleSum"
          }
        ],
        "dataSource": "wikipedia",
        "dimensions": [
          {
            "dimension": "page",
            "outputName": "Page",
            "type": "default"
          }
        ],
        "granularity": "all",
        "intervals": [
          "2013-02-26/2013-02-27"
        ],
        "limitSpec": {
          "columns": [
            {
              "dimension": "Count",
              "direction": "descending"
            }
          ],
          "limit": 5,
          "type": "default"
        },
        "queryType": "groupBy"
      });
    });

    it("processes a split with custom aggregations", function() {
      var ex = $('wiki').split("$page", 'Page')
        .apply('CrazyStupid', '$wiki.custom(crazy) * $wiki.custom(stupid)')
        .sort('$CrazyStupid', 'descending')
        .limit(5);

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "activate": false,
            "name": "_sd_0",
            "the": "borg will rise again",
            "type": "crazy"
          },
          {
            "globalWarming": "hoax",
            "name": "_sd_1",
            "onePlusOne": 3,
            "type": "stoopid"
          }
        ],
        "dataSource": "wikipedia",
        "dimension": {
          "dimension": "page",
          "outputName": "Page",
          "type": "default"
        },
        "granularity": "all",
        "intervals": [
          "2013-02-26/2013-02-27"
        ],
        "metric": "CrazyStupid",
        "postAggregations": [
          {
            "fieldName": "_sd_0",
            "name": "_sd_0_fin",
            "type": "getSomeCrazy"
          },
          {
            "fieldName": "_sd_1",
            "name": "_sd_1_fin",
            "type": "iAmWithStupid"
          },
          {
            "fields": [
              {
                "fieldName": "_sd_0_fin",
                "type": "fieldAccess"
              },
              {
                "fieldName": "_sd_1_fin",
                "type": "fieldAccess"
              }
            ],
            "fn": "*",
            "name": "CrazyStupid",
            "type": "arithmetic"
          }
        ],
        "queryType": "topN",
        "threshold": 5
      });
    });

    it("filters (in)", function() {
      var ex = ply()
        .apply("wiki", $('wiki', 1).filter($("language").in(['en'])))
        .apply('Count', '$wiki.count()');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "wikipedia",
        "filter": {
          "dimension": "language",
          "type": "selector",
          "value": "en"
        },
        "granularity": "all",
        "intervals": [
          "2013-02-26/2013-02-27"
        ],
        "queryType": "timeseries"
      });
    });

    it("filters (in [null])", function() {
      var ex = ply()
        .apply("wiki", $('wiki', 1).filter($("language").in([null])))
        .apply('Count', '$wiki.count()');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "wikipedia",
        "filter": {
          "dimension": "language",
          "type": "selector",
          "value": null
        },
        "granularity": "all",
        "intervals": [
          "2013-02-26/2013-02-27"
        ],
        "queryType": "timeseries"
      });
    });

    it("filters (contains)", function() {
      var ex = ply()
        .apply("wiki", $('wiki', 1).filter($("language").contains('en', 'ignoreCase')))
        .apply('Count', '$wiki.count()');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "wikipedia",
        "filter": {
          "dimension": "language",
          "query": {
            "type": "fragment",
            "values": [
              "en"
            ]
          },
          "type": "search"
        },
        "granularity": "all",
        "intervals": [
          "2013-02-26/2013-02-27"
        ],
        "queryType": "timeseries"
      });
    });

    it("should work with complex aggregate expressions", function() {
      var ex = $('wiki').split("$page", 'Page')
        .apply('SumAbs', '$wiki.sum($added.absolute())')
        .apply('SumComplex', '$wiki.sum($added.power(2) * $deleted / $added.absolute())')
        .sort('$SumAbs', 'descending')
        .limit(5);

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "fieldNames": [
              "added"
            ],
            "fnAggregate": "function(_c,added) { return _c+Math.abs(added); }",
            "fnCombine": "function(a,b) { return a+b; }",
            "fnReset": "function() { return 0; }",
            "name": "SumAbs",
            "type": "javascript"
          },
          {
            "fieldNames": [
              "added",
              "deleted"
            ],
            "fnAggregate": "function(_c,added,deleted) { return _c+((Math.pow(added,2)*deleted)/Math.abs(added)); }",
            "fnCombine": "function(a,b) { return a+b; }",
            "fnReset": "function() { return 0; }",
            "name": "SumComplex",
            "type": "javascript"
          }
        ],
        "dataSource": "wikipedia",
        "dimension": {
          "dimension": "page",
          "outputName": "Page",
          "type": "default"
        },
        "granularity": "all",
        "intervals": [
          "2013-02-26/2013-02-27"
        ],
        "metric": "SumAbs",
        "queryType": "topN",
        "threshold": 5
      });
    });

    it("should work with filtered complex aggregate expressions", function() {
      var ex = $('wiki').split("$page", 'Page')
        .apply('FilteredSumDeleted', '$wiki.filter($page.contains("wikipedia")).sum($deleted)')
        .apply('Filtered2', '$wiki.filter($deleted != 100).sum($deleted)')
        .sort('$FilteredSumDeleted', 'descending')
        .limit(5);

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "fieldNames": [
              "deleted",
              "page"
            ],
            "fnAggregate": "function(_c,deleted,page) { return _c+((''+page).indexOf(\"wikipedia\")>-1 ? deleted : 0); }",
            "fnCombine": "function(a,b) { return a+b; }",
            "fnReset": "function() { return 0; }",
            "name": "FilteredSumDeleted",
            "type": "javascript"
          },
          {
            "fieldNames": [
              "deleted"
            ],
            "fnAggregate": "function(_c,deleted) { return _c+(!((deleted===100)) ? deleted : 0); }",
            "fnCombine": "function(a,b) { return a+b; }",
            "fnReset": "function() { return 0; }",
            "name": "Filtered2",
            "type": "javascript"
          }
        ],
        "dataSource": "wikipedia",
        "dimension": {
          "dimension": "page",
          "outputName": "Page",
          "type": "default"
        },
        "granularity": "all",
        "intervals": [
          "2013-02-26/2013-02-27"
        ],
        "metric": "FilteredSumDeleted",
        "queryType": "topN",
        "threshold": 5
      });
    });

    it("processes an expression function fallback", function() {
      var ex = $('wiki').split($('page').extract('^Cat(.+)$').fallback("noMatch"), 'Page');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "wikipedia",
        "dimensions": [
          {
            "dimension": "page",
            "extractionFn": {
              "function": "function(d){return (_ = ((''+d).match(/^Cat(.+)$/) || [])[1] || null, (_ === null ? \"noMatch\" : _));}",
              "type": "javascript"
            },
            "outputName": "Page",
            "type": "extraction"
          }
        ],
        "granularity": "all",
        "intervals": [
          "2013-02-26/2013-02-27"
        ],
        "limitSpec": {
          "columns": [
            "Page"
          ],
          "limit": 500000,
          "type": "default"
        },
        "queryType": "groupBy"
      });
    });

    it("processes a lookup function fallback", function() {
      var ex = $('wiki').split($('page').lookup('wikipedia-language-lookup').fallback('missing'), 'Language');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "wikipedia",
        "dimensions": [
          {
            "dimension": "page",
            "extractionFn": {
              "injective": false,
              "lookup": {
                "namespace": "wikipedia-language-lookup",
                "type": "namespace"
              },
              "replaceMissingValueWith": "missing",
              "type": "lookup"
            },
            "outputName": "Language",
            "type": "extraction"
          }
        ],
        "granularity": "all",
        "intervals": [
          "2013-02-26/2013-02-27"
        ],
        "limitSpec": {
          "columns": [
            "Language"
          ],
          "limit": 500000,
          "type": "default"
        },
        "queryType": "groupBy"
      });
    });

    it("should work in simple cases with power and absolute", function() {
      var ex = $('wiki').split("$page", 'Page')
        .apply('Count', '$wiki.count()')
        .apply('Abs', '$wiki.sum($added).absolute()')
        .apply('Abs2', '$wiki.sum($added).power(2)')
        .sort('$Abs', 'descending')
        .limit(5);

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          },
          {
            "fieldName": "added",
            "name": "_sd_0",
            "type": "doubleSum"
          }
        ],
        "dataSource": "wikipedia",
        "dimension": {
          "dimension": "page",
          "outputName": "Page",
          "type": "default"
        },
        "granularity": "all",
        "intervals": [
          "2013-02-26/2013-02-27"
        ],
        "metric": "Abs",
        "postAggregations": [
          {
            "fieldNames": [
              "_sd_0"
            ],
            "function": "function(_sd_0) { return Math.abs(_sd_0); }",
            "name": "Abs",
            "type": "javascript"
          },
          {
            "fieldNames": [
              "_sd_0"
            ],
            "function": "function(_sd_0) { return Math.pow(_sd_0,2); }",
            "name": "Abs2",
            "type": "javascript"
          }
        ],
        "queryType": "topN",
        "threshold": 5
      });
    });

    it("should work with complex absolute and power expressions", function() {
      var ex = $('wiki').split("$page", 'Page')
        .apply('Count', '$wiki.count()')
        .apply('Abs', '(($wiki.sum($added)/$wiki.count().absolute().power(0.5) + 100 * $wiki.countDistinct($page)).absolute()).power(2) + $wiki.custom(crazy)')
        .sort('$Count', 'descending')
        .limit(5);

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query).to.deep.equal({
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          },
          {
            "fieldName": "added",
            "name": "_sd_0",
            "type": "doubleSum"
          },
          {
            "byRow": true,
            "fieldNames": [
              "page"
            ],
            "name": "_sd_1",
            "type": "cardinality"
          },
          {
            "activate": false,
            "name": "_sd_2",
            "the": "borg will rise again",
            "type": "crazy"
          }
        ],
        "dataSource": "wikipedia",
        "dimension": {
          "dimension": "page",
          "outputName": "Page",
          "type": "default"
        },
        "granularity": "all",
        "intervals": [
          "2013-02-26/2013-02-27"
        ],
        "metric": "Count",
        "postAggregations": [
          {
            "fieldName": "_sd_1",
            "name": "_sd_1_fin",
            "type": "hyperUniqueCardinality"
          },
          {
            "fieldName": "_sd_2",
            "name": "_sd_2_fin",
            "type": "getSomeCrazy"
          },
          {
            "fields": [
              {
                "fieldNames": [
                  "Count",
                  "_sd_0",
                  "_sd_1_fin"
                ],
                "function": "function(Count,_sd_0,_sd_1_fin) { return Math.pow(Math.abs(((_sd_0/Math.pow(Math.abs(Count),0.5))+(100*_sd_1_fin))),2); }",
                "type": "javascript"
              },
              {
                "fieldName": "_sd_2_fin",
                "type": "fieldAccess"
              }
            ],
            "fn": "+",
            "name": "Abs",
            "type": "arithmetic"
          }
        ],
        "queryType": "topN",
        "threshold": 5
      });
    });
  });


  describe("should work when getting back [] and [{result:[]}]", function() {
    var nullExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      requester(query) {
        return Q([]);
      },
      attributes: [
        { name: 'time', type: 'TIME' },
        { name: 'language', type: 'STRING' },
        { name: 'page', type: 'STRING' },
        { name: 'added', type: 'NUMBER' }
      ],
      filter: timeFilter
    });

    var emptyExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      requester(query) {
        return Q([{ result: [] }]);
      },
      attributes: [
        { name: 'time', type: 'TIME' },
        { name: 'language', type: 'STRING' },
        { name: 'page', type: 'STRING' },
        { name: 'added', type: 'NUMBER' }
      ],
      filter: timeFilter
    });

    describe("should return null correctly on a totals query", function() {
      var ex = ply()
        .apply('wiki', '$wiki')// for now
        .apply('Count', '$wiki.count()');

      it("should work with [] return", function(testComplete) {
        return ex.compute({ wiki: nullExternal })
          .then(function(result) {
            expect(result.toJS()).to.deep.equal([
              { Count: 0 }
            ]);
            testComplete();
          })
          .done();
      });
    });

    describe("should return null correctly on a timeseries query", function() {
      var ex = $('wiki').split("$time.timeBucket(P1D, 'Etc/UTC')", 'Time')
        .apply('Count', '$wiki.count()')
        .sort('$Time', 'ascending');

      it("should work with [] return", function(testComplete) {
        return ex.compute({ wiki: nullExternal })
          .then(function(result) {
            expect(result.toJS()).to.deep.equal([]);
            testComplete();
          })
          .done();
      });
    });

    describe("should return null correctly on a topN query", function() {
      var ex = $('wiki').split("$page", 'Page')
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5);

      it("should work with [] return", function(testComplete) {
        return ex.compute({ wiki: nullExternal })
          .then(function(result) {
            expect(result.toJS()).to.deep.equal([]);
            testComplete();
          })
          .done();
      });

      it("should work with [{result:[]}] return", function(testComplete) {
        return ex.compute({ wiki: emptyExternal })
          .then(function(result) {
            expect(result.toJS()).to.deep.equal([]);
            testComplete();
          })
          .done();
      });
    });
  });


  describe("should work when getting back crap data", function() {
    var crapExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      requester(query) {
        return Q("[Does this look like data to you?");
      },
      attributes: [
        { name: 'time', type: 'TIME' },
        { name: 'language', type: 'STRING' },
        { name: 'page', type: 'STRING' },
        { name: 'added', type: 'NUMBER' }
      ],
      filter: timeFilter
    });

    it("should work with all query", function(testComplete) {
      var ex = ply()
        .apply('wiki', '$wiki')// for now
        .apply('Count', '$wiki.count()');

      return ex.compute({ wiki: crapExternal })
        .then(function() {
          throw new Error('DID_NOT_ERROR');
        })
        .fail(function(err) {
          expect(err.message).to.equal('unexpected result from Druid (all)');
          testComplete();
        })
        .done();
    });

    it("should work with timeseries query", function(testComplete) {
      var ex = $('wiki').split("$time.timeBucket(P1D, 'Etc/UTC')", 'Time')
        .apply('Count', '$wiki.count()')
        .sort('$Time', 'ascending');

      return ex.compute({ wiki: crapExternal })
        .then(function() {
          throw new Error('DID_NOT_ERROR');
        })
        .fail(function(err) {
          expect(err.message).to.equal('unexpected result from Druid (timeseries)');
          testComplete();
        })
        .done();
    });
  });
});
