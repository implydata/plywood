{ expect } = require("chai")

Q = require('q')

plywood = require("../../build/plywood")
{ concurrentLimitRequesterFactory } = plywood.helper

describe "Retry requester", ->
  makeRequester = () ->
    deferreds = {}

    requester = (request) ->
      deferred = Q.defer()
      deferreds[request.query] = deferred
      return deferred.promise

    requester.hasQuery = (query) -> Boolean(deferreds[query])
    requester.resolve =  (query) -> deferreds[query].resolve([1, 2, 3])
    requester.reject =   (query) -> deferreds[query].reject(new Error('fail'))
    return requester

  it "basic works", (testComplete) ->
    requester = makeRequester()
    concurrentLimitRequester = concurrentLimitRequesterFactory({
      requester
      concurrentLimit: 2
    })

    concurrentLimitRequester({ query: 'a' }).then((res) ->
      expect(res).to.be.an('array')
      testComplete()
    ).done()

    requester.resolve('a')

  it "limit works", (testComplete) ->
    requester = makeRequester()
    concurrentLimitRequester = concurrentLimitRequesterFactory({
      requester
      concurrentLimit: 2
    })

    nextQuery = 'a'
    concurrentLimitRequester({ query: 'a' }).then((res) ->
      expect(res).to.be.an('array')
      expect(nextQuery).to.equal('a')
      nextQuery = 'b'
    ).done()

    concurrentLimitRequester({ query: 'b' }).then((res) ->
      expect(res).to.be.an('array')
      expect(nextQuery).to.equal('b')
      nextQuery = 'c'
      expect(requester.hasQuery('c', 'has c')).to.equal(true)
      requester.resolve('c')
    ).done()

    concurrentLimitRequester({ query: 'c' }).then((res) ->
      expect(res).to.be.an('array')
      expect(nextQuery).to.equal('c')
      testComplete()
    ).done()

    expect(requester.hasQuery('a'), 'has a').to.equal(true)
    expect(requester.hasQuery('b'), 'has b').to.equal(true)
    expect(requester.hasQuery('c'), 'has c').to.equal(false)
    requester.resolve('a')
    requester.resolve('b')
