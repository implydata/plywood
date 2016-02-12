var { expect } = require("chai");

var Q = require('q');

var plywood = require("../../build/plywood");
var { verboseRequesterFactory } = plywood.helper;

describe("Verbose requester", function() {
  var requester = function(request) {
    if (/^fail/.test(request.query)) {
      return Q.reject(new Error('some error'));
    } else {
      return Q([1, 2, 3]);
    }
  };

  it("works on success", function(testComplete) {
    var lines = [];
    var verboseRequester = verboseRequesterFactory({
      requester: requester,
      printLine(line) { return lines.push(line); }
    });

    return verboseRequester({ query: 'Query1' }).then(function(res) {
      expect(res).to.be.an('array');
      expect(lines.join('\n').replace(/\d+ms/, 'Xms')).to.equal(`vvvvvvvvvvvvvvvvvvvvvvvvvv
Sending query 1:
"Query1"
^^^^^^^^^^^^^^^^^^^^^^^^^^
vvvvvvvvvvvvvvvvvvvvvvvvvv
Got result from query 1: (in Xms)
[
  1,
  2,
  3
]
^^^^^^^^^^^^^^^^^^^^^^^^^^`);
      return testComplete();
    }
    ).done();
  });

  return it("works on failure", function(testComplete) {
    var lines = [];
    var verboseRequester = verboseRequesterFactory({
      requester: requester,
      printLine(line) { return lines.push(line); }
    });

    return verboseRequester({ query: 'failThis' }).then(function() { throw new Error('did not fail'); }).catch(function(error) {
      expect(lines.join('\n').replace(/\d+ms/, 'Xms')).to.equal(`vvvvvvvvvvvvvvvvvvvvvvvvvv
Sending query 1:
"failThis"
^^^^^^^^^^^^^^^^^^^^^^^^^^
vvvvvvvvvvvvvvvvvvvvvvvvvv
Got error in query 1: some error (in Xms)
^^^^^^^^^^^^^^^^^^^^^^^^^^`);
      return testComplete();
    }
    ).done();
  });
});
