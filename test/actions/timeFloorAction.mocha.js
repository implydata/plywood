var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { $, ply, r, Action } = plywood;

describe("TimeFloorAction", () => {

  describe("errors", () => {
    it('errors on non-floorable duration', () => {
      expect(() => {
        Action.fromJS({
          action: 'timeFloor',
          duration: 'PT5H'
        });
      }).to.throw("duration 'PT5H' is not floorable");
    });

  });

  describe("#alignsWith", () => {
    var hourFloorUTC = Action.fromJS({
      action: 'timeFloor',
      duration: 'PT1H',
      timezone: 'Etc/UTC'
    });

    it("works with higher floor (PT2H)", () => {
      var action = Action.fromJS({
        action: 'timeFloor',
        duration: 'PT2H',
        timezone: 'Etc/UTC'
      });

      expect(hourFloorUTC.alignsWith([action])).to.equal(true);
    });

    it("works with higher floor (P1D)", () => {
      var action = Action.fromJS({
        action: 'timeFloor',
        duration: 'P1D',
        timezone: 'Etc/UTC'
      });

      expect(hourFloorUTC.alignsWith([action])).to.equal(true);
    });

    it("works fails on different timezone", () => {
      var action = Action.fromJS({
        action: 'timeFloor',
        duration: 'PT2H',
        timezone: 'America/Los_Angeles'
      });

      expect(hourFloorUTC.alignsWith([action])).to.equal(false);
    });

    it("works fails on lower duration", () => {
      var action = Action.fromJS({
        action: 'timeFloor',
        duration: 'PT30M',
        timezone: 'Etc/UTC'
      });

      expect(hourFloorUTC.alignsWith([action])).to.equal(false);
    });

    it("works with IN range", () => {
      var action = Action.fromJS({
        action: 'in',
        expression: {
          op: 'literal',
          type: 'TIME_RANGE',
          value: {
            start: '2016-09-01T01:00:00Z',
            end: '2016-09-01T02:00:00Z'
          }
        }
      });

      expect(hourFloorUTC.alignsWith([action])).to.equal(true);
    });

    it("works fails IN range (bad)", () => {
      var action = Action.fromJS({
        action: 'in',
        expression: {
          op: 'literal',
          type: 'TIME_RANGE',
          value: {
            start: '2016-09-01T01:00:00Z',
            end: '2016-09-01T02:00:01Z'
          }
        }
      });

      expect(hourFloorUTC.alignsWith([action])).to.equal(false);
    });

    it("works with IN set", () => {
      var action = Action.fromJS({
        action: 'in',
        expression: {
          op: 'literal',
          type: 'SET',
          value: {
            setType: 'TIME_RANGE',
            elements: [
              { start: '2016-09-01T01:00:00Z', end: '2016-09-01T02:00:00Z' },
              { start: '2016-09-01T05:00:00Z', end: '2016-09-01T07:00:00Z' }
            ]
          }
        }
      });

      expect(hourFloorUTC.alignsWith([action])).to.equal(true);
    });

    it("works fails IN set (bad)", () => {
      var action = Action.fromJS({
        action: 'in',
        expression: {
          op: 'literal',
          type: 'SET',
          value: {
            setType: 'TIME_RANGE',
            elements: [
              { start: '2016-09-01T01:00:00Z', end: '2016-09-01T02:00:00Z' },
              { start: '2016-09-01T05:00:00Z', end: '2016-09-01T07:00:01Z' }
            ]
          }
        }
      });

      expect(hourFloorUTC.alignsWith([action])).to.equal(false);
    });

  });

});
