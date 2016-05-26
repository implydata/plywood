var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { External, Dataset, $, ply, r } = plywood;

describe("defineEnvironment", () => {
  it("adds Etc/UTC", () => {
    var ex1 = ply()
      .apply("diamonds", $("diamonds").filter($('color').is('D')))
      .apply('timeBucket', $("diamonds").split("$time.timeBucket(P1D)", 'Split'))
      .apply('timeFloor', $("diamonds").split("$time.timeFloor(P1D)", 'Split'))
      .apply('timeShift', $("diamonds").split("$time.timeShift(P1D, 1)", 'Split'))
      .apply('timeRange', $("diamonds").split("$time.timeRange(P1D, 1)", 'Split'))
      .apply('timePart', $("diamonds").split("$time.timePart(P1D)", 'Split'))
      .apply('multiSplit', $("diamonds").split({ A: "$time.timePart(P1D)", B: "$time.timePart(P1D)" }));

    var environment = { timezone: 'Etc/UTC' };

    var ex2 = ply()
      .apply("diamonds", $("diamonds").filter($('color').is('D')))
      .apply('timeBucket', $("diamonds").split("$time.timeBucket(P1D, 'Etc/UTC')", 'Split'))
      .apply('timeFloor', $("diamonds").split("$time.timeFloor(P1D, 'Etc/UTC')", 'Split'))
      .apply('timeShift', $("diamonds").split("$time.timeShift(P1D, 1, 'Etc/UTC')", 'Split'))
      .apply('timeRange', $("diamonds").split("$time.timeRange(P1D, 1, 'Etc/UTC')", 'Split'))
      .apply('timePart', $("diamonds").split("$time.timePart(P1D, 'Etc/UTC')", 'Split'))
      .apply('multiSplit', $("diamonds").split({ A: "$time.timePart(P1D, 'Etc/UTC')", B: "$time.timePart(P1D, 'Etc/UTC')" }));

    expect(ex1.defineEnvironment(environment).toJS()).to.deep.equal(ex2.toJS());
  });

  it("adds America/Los_Angeles", () => {
    var ex1 = ply()
      .apply("diamonds", $("diamonds").filter($('color').is('D')))
      .apply('timeBucket', $("diamonds").split("$time.timeBucket(P1D)", 'Split'))
      .apply('timeFloor', $("diamonds").split("$time.timeFloor(P1D)", 'Split'))
      .apply('timeShift', $("diamonds").split("$time.timeShift(P1D, 1)", 'Split'))
      .apply('timeRange', $("diamonds").split("$time.timeRange(P1D, 1)", 'Split'))
      .apply('timePart', $("diamonds").split("$time.timePart(P1D)", 'Split'))
      .apply('multiSplit', $("diamonds").split({ A: "$time.timePart(P1D)", B: "$time.timePart(P1D)" }));

    var environment = { timezone: 'America/Los_Angeles' };

    var ex2 = ply()
      .apply("diamonds", $("diamonds").filter($('color').is('D')))
      .apply('timeBucket', $("diamonds").split("$time.timeBucket(P1D, 'America/Los_Angeles')", 'Split'))
      .apply('timeFloor', $("diamonds").split("$time.timeFloor(P1D, 'America/Los_Angeles')", 'Split'))
      .apply('timeShift', $("diamonds").split("$time.timeShift(P1D, 1, 'America/Los_Angeles')", 'Split'))
      .apply('timeRange', $("diamonds").split("$time.timeRange(P1D, 1, 'America/Los_Angeles')", 'Split'))
      .apply('timePart', $("diamonds").split("$time.timePart(P1D, 'America/Los_Angeles')", 'Split'))
      .apply('multiSplit', $("diamonds").split({ A: "$time.timePart(P1D, 'America/Los_Angeles')", B: "$time.timePart(P1D, 'America/Los_Angeles')" }));

    expect(ex1.defineEnvironment(environment).toJS()).to.deep.equal(ex2.toJS());
  });

  it("does not override existing timezone", () => {
    var ex1 = ply()
      .apply("diamonds", $("diamonds").filter($('color').is('D')))
      .apply('timeBucket', $("diamonds").split("$time.timeBucket(P1D)", 'Split'))
      .apply('timeFloor', $("diamonds").split("$time.timeFloor(P1D)", 'Split'))
      .apply('timeShift', $("diamonds").split("$time.timeShift(P1D, 1, 'Etc/UTC')", 'Split'))
      .apply('timeRange', $("diamonds").split("$time.timeRange(P1D, 1, 'America/New_York')", 'Split'))
      .apply('timePart', $("diamonds").split("$time.timePart(P1D)", 'Split'))
      .apply('multiSplit', $("diamonds").split({ A: "$time.timePart(P1D, 'America/New_York')", B: "$time.timePart(P1D)" }));

    var environment = { timezone: 'America/Los_Angeles' };

    var ex2 = ply()
      .apply("diamonds", $("diamonds").filter($('color').is('D')))
      .apply('timeBucket', $("diamonds").split("$time.timeBucket(P1D, 'America/Los_Angeles')", 'Split'))
      .apply('timeFloor', $("diamonds").split("$time.timeFloor(P1D, 'America/Los_Angeles')", 'Split'))
      .apply('timeShift', $("diamonds").split("$time.timeShift(P1D, 1, 'Etc/UTC')", 'Split'))
      .apply('timeRange', $("diamonds").split("$time.timeRange(P1D, 1, 'America/New_York')", 'Split'))
      .apply('timePart', $("diamonds").split("$time.timePart(P1D, 'America/Los_Angeles')", 'Split'))
      .apply('multiSplit', $("diamonds").split({ A: "$time.timePart(P1D, 'America/New_York')", B: "$time.timePart(P1D, 'America/Los_Angeles')" }));

    expect(ex1.defineEnvironment(environment).toJS()).to.deep.equal(ex2.toJS());
  });

});
