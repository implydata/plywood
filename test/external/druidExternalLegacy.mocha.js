var { expect } = require("chai");
var Q = require('q');
var { sane } = require('../utils');

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { External, TimeRange, $, ply, r } = plywood;

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
      { name: 'sometimeLater', type: 'TIME' },
      { name: 'language', type: 'STRING' },
      { name: 'page', type: 'STRING' },
      { name: 'tags', type: 'SET/STRING' },
      { name: 'commentLength', type: 'NUMBER' },
      { name: 'isRobot', type: 'BOOLEAN' },
      { name: 'count', type: 'NUMBER', unsplitable: true },
      { name: 'added', type: 'NUMBER', unsplitable: true },
      { name: 'deleted', type: 'NUMBER', unsplitable: true },
      { name: 'inserted', type: 'NUMBER', unsplitable: true }
    ],
    derivedAttributes: {
      pageInBrackets: "'[' ++ $page ++ ']'"
    },
    filter: timeFilter,
    allowSelectQueries: true,
    version: '0.8.2', // Legacy !!!!!!!!!!!!!!!
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

describe("DruidExternal Legacy", () => {
  
  describe("splits", () => {
    
    it("works with .concat()", () => {
      var ex = $('wiki').split('"[%]" ++ $page ++ "[%]"', 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var query = ex.external.getQueryAndPostProcess().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        "dimension": "page",
        "extractionFn": {
          "function": "function(d){return (_=(_=d,(_==null)?null:(\"[%]\"+_)),(_==null)?null:(_+\"[%]\"));}",
          "injective": true,
          "type": "javascript"
        },
        "outputName": "Split",
        "type": "extraction"
      });
    });

    it("works with SET/STRING.concat()", () => {
      var ex = $('wiki').split('"[%]" ++ $page ++ "[%]"', 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var query = ex.external.getQueryAndPostProcess().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        "dimension": "page",
        "extractionFn": {
          "function": "function(d){return (_=(_=d,(_==null)?null:(\"[%]\"+_)),(_==null)?null:(_+\"[%]\"));}",
          "injective": true,
          "type": "javascript"
        },
        "outputName": "Split",
        "type": "extraction"
      });
    });

    it("works with .substr()", () => {
      var ex = $('wiki').split('$page.substr(3, 5)', 'Split');

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var query = ex.external.getQueryAndPostProcess().query;
      expect(query.queryType).to.equal('groupBy');
      expect(query.dimensions[0]).to.deep.equal({
        "dimension": "page",
        "extractionFn": {
          "function": "function(d){return (_=d,_==null?null:(''+_).substr(3,5));}",
          "type": "javascript"
        },
        "outputName": "Split",
        "type": "extraction"
      });
    });
  
  });


});
