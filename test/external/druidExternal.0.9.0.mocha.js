var { expect } = require("chai");
var Q = require('q');
var { sane } = require('../utils');

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { External, TimeRange, $, ply, r, AttributeInfo } = plywood;

var timeFilter = $('time').in(TimeRange.fromJS({
  start: new Date("2013-02-26T00:00:00Z"),
  end: new Date("2013-02-27T00:00:00Z")
}));

var context = {
  wiki: External.fromJS({
    engine: 'druid',
    source: 'wikipedia',
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
      { name: 'inserted', type: 'NUMBER', unsplitable: true },
      { name: 'delta_hist', special: 'histogram' }
    ],
    filter: timeFilter,
    allowSelectQueries: true,
    version: '0.9.0',
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


describe("DruidExternal 0.9.0", () => {

  describe("simplifies / digests", () => {

    it("works with .lookup().overlap(blah, null) (on SET/STRING)", () => {
      var ex = $('wiki').filter($("tags").lookup('tag_lookup').overlap(['Good', null]));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query.filter).to.deep.equal({
        "fields": [
          {
            "dimension": "tags",
            "extractionFn": {
              "lookup": {
                "namespace": "tag_lookup",
                "type": "namespace"
              },
              "type": "lookup"
            },
            "type": "extraction",
            "value": "Good"
          },
          {
            "dimension": "tags",
            "extractionFn": {
              "lookup": {
                "namespace": "tag_lookup",
                "type": "namespace"
              },
              "type": "lookup"
            },
            "type": "extraction",
            "value": null
          }
        ],
        "type": "or"
      });
    });

    it("works with .lookup().contains()", () => {
      var ex = $('wiki').filter($("language").lookup('language_lookup').contains('eN', 'ignoreCase'));

      ex = ex.referenceCheck(context).resolve(context).simplify();

      expect(ex.op).to.equal('external');
      var druidExternal = ex.external;
      expect(druidExternal.getQueryAndPostProcess().query.filter).to.deep.equal({
        "dimension": "language",
        "extractionFn": {
          "extractionFns": [
            {
              "lookup": {
                "namespace": "language_lookup",
                "type": "namespace"
              },
              "type": "lookup"
            },
            {
              "function": "function(d){var _,_2;return (_=d,(_==null)?null:((''+_).toLowerCase().indexOf((''+\"eN\").toLowerCase())>-1));}",
              "type": "javascript"
            }
          ],
          "type": "cascade"
        },
        "type": "extraction",
        "value": "true"
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
          "function": "function(d){var _,_2;return (_=(_=d,(_==null)?null:(\"[%]\"+_)),(_==null)?null:(_+\"[%]\"));}",
          "injective": true,
          "type": "javascript"
        },
        "outputName": "Split",
        "type": "extraction"
      });
    });

  });

});
