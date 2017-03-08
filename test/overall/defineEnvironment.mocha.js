/*
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
let { External, Dataset, $, ply, r } = plywood;

describe("defineEnvironment", () => {
  it("adds Etc/UTC in trivial case", () => {
    let ex1 = $('time').timeBucket('P1D');

    let environment = { timezone: 'Etc/UTC' };

    let ex2 = $('time').timeBucket('P1D', 'Etc/UTC');

    expect(ex1.defineEnvironment(environment).toJS()).to.deep.equal(ex2.toJS());
  });

  it("adds Etc/UTC", () => {
    let ex1 = ply()
      .apply("diamonds", $("diamonds").filter($('color').is('D')))
      .apply('timeBucket', $("diamonds").split("$time.timeBucket(P1D)", 'Split'))
      .apply('timeFloor', $("diamonds").split("$time.timeFloor(P1D)", 'Split'))
      .apply('timeShift', $("diamonds").split("$time.timeShift(P1D, 1)", 'Split'))
      .apply('timeRange', $("diamonds").split("$time.timeRange(P1D, 1)", 'Split'))
      .apply('timePart', $("diamonds").split("$time.timePart(P1D)", 'Split'))
      .apply('multiSplit', $("diamonds").split({ A: "$time.timePart(P1D)", B: "$time.timePart(P1D)" }));

    let environment = { timezone: 'Etc/UTC' };

    let ex2 = ply()
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
    let ex1 = ply()
      .apply("diamonds", $("diamonds").filter($('color').is('D')))
      .apply('timeBucket', $("diamonds").split("$time.timeBucket(P1D)", 'Split'))
      .apply('timeFloor', $("diamonds").split("$time.timeFloor(P1D)", 'Split'))
      .apply('timeShift', $("diamonds").split("$time.timeShift(P1D, 1)", 'Split'))
      .apply('timeRange', $("diamonds").split("$time.timeRange(P1D, 1)", 'Split'))
      .apply('timePart', $("diamonds").split("$time.timePart(P1D)", 'Split'))
      .apply('multiSplit', $("diamonds").split({ A: "$time.timePart(P1D)", B: "$time.timePart(P1D)" }));

    let environment = { timezone: 'America/Los_Angeles' };

    let ex2 = ply()
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
    let ex1 = ply()
      .apply("diamonds", $("diamonds").filter($('color').is('D')))
      .apply('timeBucket', $("diamonds").split("$time.timeBucket(P1D)", 'Split'))
      .apply('timeFloor', $("diamonds").split("$time.timeFloor(P1D)", 'Split'))
      .apply('timeShift', $("diamonds").split("$time.timeShift(P1D, 1, 'Etc/UTC')", 'Split'))
      .apply('timeRange', $("diamonds").split("$time.timeRange(P1D, 1, 'America/New_York')", 'Split'))
      .apply('timePart', $("diamonds").split("$time.timePart(P1D)", 'Split'))
      .apply('multiSplit', $("diamonds").split({ A: "$time.timePart(P1D, 'America/New_York')", B: "$time.timePart(P1D)" }));

    let environment = { timezone: 'America/Los_Angeles' };

    let ex2 = ply()
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
