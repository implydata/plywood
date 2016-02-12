var { expect } = require("chai");

var plywood = require("../../build/plywood");
var { simpleLocator } = plywood.helper;

describe('Simple locator', () => {
  describe('shortcut function', () => {
    var locator = simpleLocator("localhost:8080");

    it("works", (testComplete) => {
      return locator()
        .then((location) => {
          expect(location).to.deep.equal({
            hostname: 'localhost',
            port: 8080
          });
          testComplete();
        })
        .done();
    });
  });

  describe('full option function', () => {
    var locator = simpleLocator({
      resource: "localhost;koalastothemax.com:80",
      defaultPort: 8181
    });

    it("works", (testComplete) => {
      return locator()
        .then((location) => {
          for (var i = 1; i <= 20; i++) {
            if (location.hostname === 'localhost') {
              expect(location).to.deep.equal({
                hostname: 'localhost',
                port: 8181
              });
            } else {
              expect(location).to.deep.equal({
                hostname: 'koalastothemax.com',
                port: 80
              });
            }
          }
          testComplete();
        })
        .done();
    });
  });
});
