var { expect } = require("chai");

var plywood = require('../../build/plywood');
var { $, ply, r, MatchAction } = plywood;

describe("MatchAction", () => {
  it(".likeToRegExp", () => {
    expect(MatchAction.likeToRegExp('%David\\_R_ss%')).to.equal('^.*David_R.ss.*$');

    expect(MatchAction.likeToRegExp('%David|_R_ss||%', '|')).to.equal('^.*David_R.ss\\|.*$');
  });
});
