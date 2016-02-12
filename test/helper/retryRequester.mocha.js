var { expect } = require("chai");

var Q = require('q');

var plywood = require("../../build/plywood");
var { retryRequesterFactory } = plywood.helper;

describe("Retry requester", function() {
  var makeRequester = function(failNumber, isTimeout) {
    return function(request) {
      if (failNumber > 0) {
        failNumber--;
        return Q.reject(new Error(isTimeout ? 'timeout' : 'some error'));
      } else {
        return Q([1, 2, 3]);
      }
    };
  };


  it("no retry needed (no fail)", function(testComplete) {
    var retryRequester = retryRequesterFactory({
      requester: makeRequester(0),
      delay: 20,
      retry: 2
    });

    return retryRequester({})
      .then(function(res) {
        expect(res).to.be.an('array');
        testComplete();
      })
      .done();
  });

  it("one fail", function(testComplete) {
    var retryRequester = retryRequesterFactory({
      requester: makeRequester(1),
      delay: 20,
      retry: 2
    });

    return retryRequester({})
      .then(function(res) {
        expect(res).to.be.an('array');
        testComplete();
      })
      .done();
  });

  it("two fails", function(testComplete) {
    var retryRequester = retryRequesterFactory({
      requester: makeRequester(2),
      delay: 20,
      retry: 2
    });

    return retryRequester({})
      .then(function(res) {
        expect(res).to.be.an('array');
        testComplete();
      })
      .done();
  });

  it("three fails", function(testComplete) {
    var retryRequester = retryRequesterFactory({
      requester: makeRequester(3),
      delay: 20,
      retry: 2
    });

    return retryRequester({})
      .then(function() {
        throw new Error('DID_NOT_THROW');
      })
      .catch(function(err) {
        expect(err.message).to.equal('some error');
        testComplete();
      })
      .done();
  });

  it("timeout", function(testComplete) {
    var retryRequester = retryRequesterFactory({
      requester: makeRequester(1, true),
      delay: 20,
      retry: 2
    });

    return retryRequester({})
      .then(function() {
        throw new Error('DID_NOT_THROW');
      })
      .catch(function(err) {
        expect(err.message).to.equal('timeout');
        testComplete();
      })
      .done();
  });
});
