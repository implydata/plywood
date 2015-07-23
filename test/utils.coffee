{ expect } = require("chai")
Q = require('q')

plywood = require('../build/plywood')
{ FacetQuery, SegmentTree } = plywood.legacy

uniformizeResults = (result) ->
  if not result?.prop
    return result

  ret = {}
  for k, p of result
    continue unless result.hasOwnProperty(k)
    continue if k is 'split'
    if k is 'prop'
      propNames = []
      propNames.push(name) for name, value of p
      propNames.sort()

      prop = {}
      for name in propNames
        value = p[name]
        continue unless p.hasOwnProperty(name)
        if typeof value is 'number' and value isnt Math.floor(value)
          prop[name] = Number(value.toPrecision(5))
        else if Array.isArray(value) and
              typeof value[0] is 'number' and
              typeof value[1] is 'number' and
              (value[0] isnt Math.floor(value[0]) or value[1] isnt Math.floor(value[1]))
          prop[name] = [value[0].toFixed(3), value[1].toFixed(3)]
        else
          prop[name] = value
      p = prop

    ret[k] = p

  if result.splits
    ret.splits = result.splits.map(uniformizeResults)

  if result.loading
    ret.loading = true

  return ret

exports.wrapVerbose = (requester, name) ->
  return (request) ->
    console.log "Requesting #{name}:"
    console.log '', JSON.stringify(request.query, null, 2)
    startTime = Date.now()
    return requester(request).then(
      (result) ->
        console.log "GOT RESULT FROM #{name} (took #{Date.now() - startTime}ms)"
        return result
      (err) ->
        console.log "GOT #{name} ERROR", err
        throw err
    )

exports.makeEqualityTest = (driverFnMap) ->
  return ({drivers, query, verbose, before, after}) ->
    throw new Error("must have at least two drivers") if drivers.length < 2
    query = if FacetQuery.isFacetQuery(query) then query else new FacetQuery(query)

    driverFns = drivers.map (driverName) ->
      driverFn = driverFnMap[driverName]
      throw new Error("no such driver #{driverName}") unless driverFn
      return driverFn

    return (testComplete) ->
      before?()
      Q.all(
        driverFns.map((driverFn) ->
          return driverFn({
            query
            context: { priority: -3 }
          })
        )
      ).then(
        (results) ->
          after?(null, results[0], results)

          results = results.map((result) ->
            expect(result).to.be.instanceof(SegmentTree)
            return uniformizeResults(result.toJS())
          )

          if verbose
            console.log('vvvvvvvvvvvvvvvvvvvvvvv')
            console.log("From #{drivers[0]} I got:")
            console.log(JSON.stringify(results[0], null, 2))
            console.log('^^^^^^^^^^^^^^^^^^^^^^^')

          i = 1
          while i < drivers.length
            try
              expect(results[0]).to.deep.equal(results[i], "results of '#{drivers[0]}' and '#{drivers[i]}' must match")
            catch e
              console.log "results of '#{drivers[0]}' and '#{drivers[i]}' (expected) must match"
              throw e
            i++

          testComplete(null, results[0])
          return
        (err) ->
          after?(err)
          console.log "got error from driver"
          console.log err
          throw err
      ).done()


exports.makeErrorTest = (driverFnMap) ->
  return ({drivers, request, error, verbose}) ->
    throw new Error("must have at least one driver") if drivers.length < 1

    driverFns = drivers.map (driverName) ->
      driverFn = driverFnMap[driverName]
      throw new Error("no such driver #{driverName}") unless driverFn
      return driverFn

    return (testComplete) ->
      Q.allSettled(driverFns.map((driverFn) -> driverFn(request)))
      .then((results) ->
        for result, i in results
          if result.state is "fulfilled"
            throw new Error("#{drivers[i]} did not error")
          else
            expect(result.reason.message).to.equal(error, "#{drivers[i]} did not conform to error")
        testComplete()
      )
      .done()
