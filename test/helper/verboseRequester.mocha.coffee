{ expect } = require("chai")

Q = require('q')

plywood = require("../../build/plywood")
{ verboseRequesterFactory } = plywood.helper

describe "Verbose requester", ->
  requester = (request) ->
    if /^fail/.test(request.query)
      return Q.reject(new Error('some error'))
    else
      return Q([1, 2, 3])

  it "works on success", (testComplete) ->
    lines = []
    verboseRequester = verboseRequesterFactory({
      requester: requester
      printLine: (line) -> lines.push(line)
    })

    verboseRequester({ query: 'Query1' })
      .then((res) ->
        expect(res).to.be.an('array')
        expect(lines.join('\n').replace(/\d+ms/, 'Xms')).to.equal("""
        vvvvvvvvvvvvvvvvvvvvvvvvvv
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
        ^^^^^^^^^^^^^^^^^^^^^^^^^^
        """)
        testComplete()
      ).done()

  it "works on failure", (testComplete) ->
    lines = []
    verboseRequester = verboseRequesterFactory({
      requester: requester
      printLine: (line) -> lines.push(line)
    })

    verboseRequester({ query: 'failThis' })
      .then(-> throw new Error('did not fail'))
      .catch((error) ->
        expect(lines.join('\n').replace(/\d+ms/, 'Xms')).to.equal("""
        vvvvvvvvvvvvvvvvvvvvvvvvvv
        Sending query 1:
        "failThis"
        ^^^^^^^^^^^^^^^^^^^^^^^^^^
        vvvvvvvvvvvvvvvvvvvvvvvvvv
        Got error in query 1: some error (in Xms)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^
        """)
        testComplete()
      ).done()
