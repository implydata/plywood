var { expect } = require("chai");
var Q = require('q');

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { Expression, External, TimeRange, $, ply, r } = plywood;

describe("DruidExternal Introspection", () => {
  var requesterFail = ({query}) => {
    return Q.reject(new Error('Bad status code'));
  };


  var requesterDruid_0_9_0 = ({query}) => {
    if (query.queryType === 'status') return Q({ version: '0.9.0' });
    expect(query.dataSource).to.equal('wikipedia');

    if (query.queryType === 'segmentMetadata') {
      expect(query.merge).to.equal(true);
      expect(query.analysisTypes).to.be.an('array');
      expect(query.lenientAggregatorMerge).to.equal(true);

      var merged = {
        "id": "merged",
        "intervals": null,
        "size": 0,
        "numRows": 654321,
        "columns": {
          "__time": {
            "type": "LONG",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": null,
            "errorMessage": null
          },
          "added": {
            "type": "FLOAT",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": null,
            "errorMessage": null
          },
          "anonymous": {
            "type": "STRING",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": 0,
            "errorMessage": null
          },
          "count": { "type": "LONG", "hasMultipleValues": false, "size": 0, "cardinality": null, "errorMessage": null },
          "delta": {
            "type": "FLOAT",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": null,
            "errorMessage": null
          },
          "delta_hist": {
            "type": "approximateHistogram",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": null,
            "errorMessage": null
          },
          "language": {
            "type": "STRING",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": 0,
            "errorMessage": null
          },
          "namespace": {
            "type": "STRING",
            "hasMultipleValues": true,
            "size": 0,
            "cardinality": 0,
            "errorMessage": null
          },
          "newPage": {
            "type": "STRING",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": 0,
            "errorMessage": "lol wtf"
          },
          "newUser": {
            "type": "STRING",
            "hasMultipleValues": false,
            "size": -1,
            "cardinality": 0,
            "errorMessage": null
          },
          "page": { "type": "STRING", "hasMultipleValues": false, "size": 0, "cardinality": 0, "errorMessage": null },
          "time": { "type": "STRING", "hasMultipleValues": false, "size": 0, "cardinality": 0, "errorMessage": null },
          "user_unique": {
            "type": "hyperUnique",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": null,
            "errorMessage": null
          }
        }
      };

      if (query.analysisTypes.indexOf("aggregators") !== -1) {
        merged.aggregators = {
          // Normal aggs
          "added": { "type": "doubleSum", "name": "added", "fieldName": "added" },
          "count": { "type": "longSum", "name": "count", "fieldName": "count" },

          // This can happen if the JS agg was used at ingestion time
          "delta": {
            "type": "javascript",
            "name": "delta",
            "fieldNames": [
              "delta"
            ],
            "fnAggregate": "function(partialA,partialB) {return partialA + partialB; }",
            "fnReset": "function() {return 0; }",
            "fnCombine": "function(partialA,partialB) {return partialA + partialB; }"
          },

          // A histogram
          "delta_hist": {
            "type": "approxHistogramFold",
            "name": "delta_hist",
            "fieldName": "delta_hist",
            "resolution": 50,
            "numBuckets": 7,
            "lowerLimit": "-Infinity",
            "upperLimit": "Infinity"
          },
          "user_unique": { "type": "hyperUnique", "name": "user_unique", "fieldName": "user_unique" }
        };
      }

      return Q([merged]);

    } else if (query.queryType === 'introspect') {
      return Q({
        dimensions: ['anonymous', 'language', 'namespace', 'newPage', 'page', 'time'],
        metrics: ['added', 'count', 'delta_hist', 'user_unique']
      });

    } else {
      throw new Error(`unsupported query ${query.queryType}`);
    }
  };


  var requesterDruid_0_9_0_flake = ({query}) => {
    if (query.queryType === 'status') return Q({ version: '0.9.0' });
    return requesterDruid_0_8_3({ query });
  };


  var requesterDruid_0_8_3 = ({query}) => {
    if (query.queryType === 'status') return Q({ version: '0.8.3' });
    expect(query.dataSource).to.equal('wikipedia');

    if (query.queryType === 'segmentMetadata') {
      expect(query.merge).to.equal(true);
      expect(query.analysisTypes).to.be.an('array');

      var merged = {
        "id": "merged",
        "intervals": null,
        "size": 0,
        "numRows": 654321,
        "columns": {
          "__time": {
            "type": "LONG",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": null,
            "errorMessage": null
          },
          "added": {
            "type": "FLOAT",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": null,
            "errorMessage": null
          },
          "anonymous": {
            "type": "STRING",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": 0,
            "errorMessage": null
          },
          "count": { "type": "LONG", "hasMultipleValues": false, "size": 0, "cardinality": null, "errorMessage": null },
          "delta_hist": {
            "type": "approximateHistogram",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": null,
            "errorMessage": null
          },
          "language": {
            "type": "STRING",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": 0,
            "errorMessage": null
          },
          "namespace": {
            "type": "STRING",
            "hasMultipleValues": true,
            "size": 0,
            "cardinality": 0,
            "errorMessage": null
          },
          "newPage": {
            "type": "STRING",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": 0,
            "errorMessage": "lol wtf"
          },
          "newUser": {
            "type": "STRING",
            "hasMultipleValues": false,
            "size": -1,
            "cardinality": 0,
            "errorMessage": null
          },
          "page": { "type": "STRING", "hasMultipleValues": false, "size": 0, "cardinality": 0, "errorMessage": null },
          "time": { "type": "STRING", "hasMultipleValues": false, "size": 0, "cardinality": 0, "errorMessage": null },
          "user_unique": {
            "type": "hyperUnique",
            "hasMultipleValues": false,
            "size": 0,
            "cardinality": null,
            "errorMessage": null
          }
        }
      };

      if (query.analysisTypes.indexOf("aggregators") !== -1) {
        return Q.reject(new Error('Can not construct instance of io.druid.query.metadata.metadata.SegmentMetadataQuery$AnalysisType'));
      }

      return Q([merged]);

    } else if (query.queryType === 'introspect') {
      return Q({
        dimensions: ['anonymous', 'language', 'namespace', 'newPage', 'page', 'time'],
        metrics: ['added', 'count', 'delta_hist', 'user_unique']
      });

    } else {
      throw new Error(`unsupported query ${query.queryType}`);
    }
  };


  var requesterDruid_0_8_2 = ({query}) => {
    if (query.queryType === 'status') return Q({ version: '0.8.2' });
    expect(query.dataSource).to.equal('wikipedia');

    if (query.queryType === 'segmentMetadata') {
      expect(query.merge).to.equal(true);
      expect(query.analysisTypes).to.be.an('array');

      var merged = {
        "id": "merged",
        "intervals": null,
        "size": 0,
        "numRows": 654321,
        "columns": {
          "__time": { "type": "LONG", "size": 0, "cardinality": null, "errorMessage": null },
          "added": { "type": "FLOAT", "size": 0, "cardinality": null, "errorMessage": null },
          "anonymous": { "type": "STRING", "size": 0, "cardinality": 0, "errorMessage": null },
          "count": { "type": "LONG", "size": 0, "cardinality": null, "errorMessage": null },
          "delta_hist": { "type": "COMPLEX", "size": 0, "cardinality": null, "errorMessage": null },
          "language": { "type": "STRING", "size": 0, "cardinality": 0, "errorMessage": null },
          "namespace": { "type": "STRING", "size": 0, "cardinality": 0, "errorMessage": null },
          "newPage": { "type": "STRING", "size": 0, "cardinality": 0, "errorMessage": "lol wtf" },
          "newUser": { "type": "STRING", "size": -1, "cardinality": 0, "errorMessage": null },
          "page": { "type": "STRING", "size": 0, "cardinality": 0, "errorMessage": null },
          "time": { "type": "STRING", "size": 0, "cardinality": 0, "errorMessage": null },
          "user_unique": { "type": "COMPLEX", "size": 0, "cardinality": null, "errorMessage": null }
        }
      };

      if (query.analysisTypes.indexOf("aggregators") !== -1) {
        return Q.reject(new Error('Can not construct instance of io.druid.query.metadata.metadata.SegmentMetadataQuery$AnalysisType'));
      }

      return Q([merged]);

    } else if (query.queryType === 'introspect') {
      return Q({
        dimensions: ['anonymous', 'language', 'namespace', 'newPage', 'page', 'time'],
        metrics: ['added', 'count', 'delta_hist', 'user_unique']
      });

    } else {
      throw new Error(`unsupported query ${query.queryType}`);
    }
  };


  var requesterDruid_0_8_1 = ({query}) => {
    if (query.queryType === 'status') return Q({ version: '0.8.1' });
    expect(query.dataSource).to.equal('wikipedia');

    if (query.queryType === 'segmentMetadata') {
      expect(query.intervals).to.not.exist;
      return Q.reject(new Error("Instantiation of [simple type, class io.druid.query.metadata.metadata.SegmentMetadataQuery] value failed: querySegmentSpec can't be null"));

    } else if (query.queryType === 'introspect') {
      return Q({
        dimensions: ['anonymous', 'language', 'namespace', 'newPage', 'page', 'time'],
        metrics: ['added', 'count', 'delta_hist', 'user_unique']
      });

    } else {
      throw new Error(`unsupported query ${query.queryType}`);
    }
  };


  it("errors on bad introspectionStrategy", () => {
    expect(() => {
      External.fromJS({
        engine: 'druid',
        dataSource: 'wikipedia',
        introspectionStrategy: 'crowd-source'
      }, requesterFail);
    }).to.throw("invalid introspectionStrategy 'crowd-source'");
  });

  it("does an introspect with general failure", (testComplete) => {
    var wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia'
    }, requesterFail);

    return wikiExternal.introspect()
      .then(() => {
        throw new Error('DID_NOT_ERROR');
      })
      .catch((err) => {
        expect(err.message).to.equal('Bad status code');
        testComplete();
      })
      .done();
  });

  it("does an introspect with segmentMetadata (with aggregators)", (testComplete) => {
    var wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time'
    }, requesterDruid_0_9_0);

    return wikiExternal.introspect()
      .then((introspectedExternal) => {
        expect(introspectedExternal.version).to.equal('0.9.0');
        expect(introspectedExternal.toJS().attributes).to.deep.equal([
          {
            "name": "time",
            "type": "TIME"
          },
          {
            "makerAction": {
              "action": "sum",
              "expression": {
                "name": "added",
                "op": "ref"
              }
            },
            "name": "added",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "anonymous",
            "type": "STRING"
          },
          {
            "makerAction": {
              "action": "count"
            },
            "name": "count",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "makerAction": {
              "action": "sum",
              "expression": {
                "name": "delta",
                "op": "ref"
              }
            },
            "name": "delta",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "delta_hist",
            "special": "histogram",
            "type": "NUMBER"
          },
          {
            "name": "language",
            "type": "STRING"
          },
          {
            "name": "namespace",
            "type": "SET/STRING"
          },
          {
            "name": "page",
            "type": "STRING"
          },
          {
            "name": "user_unique",
            "special": "unique",
            "type": "STRING"
          }
        ]);
        testComplete();
      })
      .done();
  });

  it("does an introspect with segmentMetadata (without aggregators, flaky driver)", (testComplete) => {
    var wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time'
    }, requesterDruid_0_9_0_flake);

    return wikiExternal.introspect()
      .then((introspectedExternal) => {
        expect(introspectedExternal.version).to.equal('0.9.0');
        expect(introspectedExternal.toJS().attributes).to.deep.equal([
          {
            "name": "time",
            "type": "TIME"
          },
          {
            "name": "added",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "anonymous",
            "type": "STRING"
          },
          {
            "name": "count",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "delta_hist",
            "special": "histogram",
            "type": "NUMBER"
          },
          {
            "name": "language",
            "type": "STRING"
          },
          {
            "name": "namespace",
            "type": "SET/STRING"
          },
          {
            "name": "page",
            "type": "STRING"
          },
          {
            "name": "user_unique",
            "special": "unique",
            "type": "STRING"
          }
        ]);
        testComplete();
      })
      .done();
  });

  it("does an introspect with segmentMetadata (without aggregators)", (testComplete) => {
    var wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time'
    }, requesterDruid_0_8_3);

    return wikiExternal.introspect()
      .then((introspectedExternal) => {
        expect(introspectedExternal.version).to.equal('0.8.3');
        expect(introspectedExternal.toJS().attributes).to.deep.equal([
          {
            "name": "time",
            "type": "TIME"
          },
          {
            "name": "added",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "anonymous",
            "type": "STRING"
          },
          {
            "name": "count",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "delta_hist",
            "special": "histogram",
            "type": "NUMBER"
          },
          {
            "name": "language",
            "type": "STRING"
          },
          {
            "name": "namespace",
            "type": "SET/STRING"
          },
          {
            "name": "page",
            "type": "STRING"
          },
          {
            "name": "user_unique",
            "special": "unique",
            "type": "STRING"
          }
        ]);
        testComplete();
      })
      .done();
  });

  it("does an introspect with segmentMetadata (with old style COMPLEX columns)", (testComplete) => {
    var wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time'
    }, requesterDruid_0_8_2);

    return wikiExternal.introspect()
      .then((introspectedExternal) => {
        expect(introspectedExternal.version).to.equal('0.8.2');
        expect(introspectedExternal.toJS().attributes).to.deep.equal([
          {
            "name": "time",
            "type": "TIME"
          },
          {
            "name": "added",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "anonymous",
            "type": "STRING"
          },
          {
            "name": "count",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "language",
            "type": "STRING"
          },
          {
            "name": "namespace",
            "type": "STRING"
          },
          {
            "name": "page",
            "type": "STRING"
          }
        ]);
        testComplete();
      })
      .done();
  });

  it("does a simple introspect with GET", (testComplete) => {
    var wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time'
    }, requesterDruid_0_8_1);

    return wikiExternal.introspect()
      .then((introspectedExternal) => {
        expect(introspectedExternal.version).to.equal('0.8.1');
        expect(introspectedExternal.toJS().attributes).to.deep.equal([
          {
            "name": "time",
            "type": "TIME"
          },
          {
            "name": "anonymous",
            "type": "STRING"
          },
          {
            "name": "language",
            "type": "STRING"
          },
          {
            "name": "namespace",
            "type": "STRING"
          },
          {
            "name": "newPage",
            "type": "STRING"
          },
          {
            "name": "page",
            "type": "STRING"
          },
          {
            "name": "added",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "count",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "delta_hist",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "user_unique",
            "type": "NUMBER",
            "unsplitable": true
          }
        ]);
        testComplete();
      })
      .done();
  });

  it("respects the introspectionStrategy flag", (testComplete) => {
    var wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      introspectionStrategy: 'datasource-get',
      timeAttribute: 'time'
    }, requesterDruid_0_9_0);

    return wikiExternal.introspect()
      .then((introspectedExternal) => {
        expect(introspectedExternal.version).to.equal('0.9.0');
        expect(introspectedExternal.toJS().attributes).to.deep.equal([
          {
            "name": "time",
            "type": "TIME"
          },
          {
            "name": "anonymous",
            "type": "STRING"
          },
          {
            "name": "language",
            "type": "STRING"
          },
          {
            "name": "namespace",
            "type": "STRING"
          },
          {
            "name": "newPage",
            "type": "STRING"
          },
          {
            "name": "page",
            "type": "STRING"
          },
          {
            "name": "added",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "count",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "delta_hist",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "user_unique",
            "type": "NUMBER",
            "unsplitable": true
          }
        ]);
        testComplete();
      })
      .done();
  });

  it("does an introspect with overrides", (testComplete) => {
    var wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      attributeOverrides: [
        { name: "user_unique", special: 'unique' },
        { name: "delta_hist", special: 'histogram' }
      ]
    }, requesterDruid_0_8_1);

    return wikiExternal.introspect()
      .then((introspectedExternal) => {
        expect(introspectedExternal.version).to.equal('0.8.1');
        expect(introspectedExternal.toJS().attributes).to.deep.equal([
          {
            "name": "time",
            "type": "TIME"
          },
          {
            "name": "anonymous",
            "type": "STRING"
          },
          {
            "name": "language",
            "type": "STRING"
          },
          {
            "name": "namespace",
            "type": "STRING"
          },
          {
            "name": "newPage",
            "type": "STRING"
          },
          {
            "name": "page",
            "type": "STRING"
          },
          {
            "name": "added",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "count",
            "type": "NUMBER",
            "unsplitable": true
          },
          {
            "name": "delta_hist",
            "special": "histogram",
            "type": "NUMBER"
          },
          {
            "name": "user_unique",
            "special": "unique",
            "type": "STRING"
          }
        ]);
        testComplete();
      })
      .done();
  });

  it("does a context based query", (testComplete) => {
    var selectRequesterDruid = ({query}) => {
      if (query.queryType !== 'select') return requesterDruid_0_9_0({query});
      expect(query).to.deep.equal({
        "dataSource": "wikipedia",
        "dimensions": [
          "anonymous",
          "delta_hist",
          "language",
          "namespace",
          "page",
          "user_unique"
        ],
        "granularity": "all",
        "intervals": "1000/3000",
        "metrics": [
          "added",
          "count",
          "delta"
        ],
        "pagingSpec": {
          "pagingIdentifiers": {},
          "threshold": 50
        },
        "queryType": "select"
      });
      return Q([]);
    };

    var wikiExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      allowEternity: true,
      allowSelectQueries: true
    }, selectRequesterDruid);

    var context = { wiki: wikiExternal };

    return $('wiki').compute(context)
      .then((results) => {
        expect(results.toJS()).to.deep.equal([]);
        testComplete();
      })
      .done();
  });
});
