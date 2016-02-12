var { expect } = require("chai");

var plywood = require("../../build/plywood");
var { simpleLocator } = plywood.helper;

describe('Simple locator', function() {
  describe('shortcut function', function() {
    var locator = simpleLocator("localhost:8080");

    return it("works", function(testComplete) {
      return locator()
      .then(function(location) {
        expect(location).to.deep.equal({
          hostname: 'localhost',
          port: 8080
        });
        return testComplete();
      }
      ).done();
    });
  });

  return describe('full option function', function() {
    var locator = simpleLocator({
      resource: "localhost;koalastothemax.com:80",
      defaultPort: 8181
    });

    return it("works", function(testComplete) {
      return locator()
      .then(function(location) {
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
        return testComplete();
      }
      ).done();
    });
  });
});
