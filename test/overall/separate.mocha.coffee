{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, $, ply, r } = plywood

toJS = (sep) ->
  return sep unless sep
  return {
    included: sep.included.toJS()
    excluded: sep.excluded.toJS()
  }

describe "separate", ->
  it 'throws on bad input', ->
    expect(->
      Expression.TRUE.separateViaAnd()
    ).to.throw('must have refName')

  it 'works with TRUE expression', ->
    ex = Expression.TRUE

    expect(toJS(ex.separateViaAnd('venue'))).to.deep.equal(toJS({
      included: Expression.TRUE
      excluded: Expression.TRUE
    }))

  it 'works with FALSE expression', ->
    ex = Expression.FALSE

    expect(toJS(ex.separateViaAnd('venue'))).to.deep.equal(toJS({
      included: Expression.TRUE
      excluded: Expression.FALSE
    }))

  it 'works on a single included expression', ->
    ex = $('venue').is('Google')

    expect(toJS(ex.separateViaAnd('venue'))).to.deep.equal(toJS({
      included: ex
      excluded: Expression.TRUE
    }))

  it 'works on a single excluded expression', ->
    ex = $('venue').is('Google')

    expect(toJS(ex.separateViaAnd('make'))).to.deep.equal(toJS({
      included: Expression.TRUE
      excluded: ex
    }))

  it 'works on a small AND expression', ->
    ex = $('venue').is('Google').and($('country').is('USA'))

    expect(toJS(ex.separateViaAnd('country'))).to.deep.equal(toJS({
      included: $('country').is('USA')
      excluded: $('venue').is('Google')
    }))

  it 'works on an AND expression', ->
    ex = $('venue').is('Google').and($('country').is('USA'), $('state').is('California'))

    expect(toJS(ex.separateViaAnd('country'))).to.deep.equal(toJS({
      included: $('country').is('USA')
      excluded: $('venue').is('Google').and($('state').is('California'))
    }))

  it 'extracts a NOT expression', ->
    ex = $('venue').is('Google').and($('country').is('USA').not(), $('state').is('California'))

    expect(toJS(ex.separateViaAnd('country'))).to.deep.equal(toJS({
      included: $('country').is('USA').not()
      excluded: $('venue').is('Google').and($('state').is('California'))
    }))

  it 'does not work on mixed OR expression', ->
    ex = $('venue').is('Google').or($('country').is('USA'), $('state').is('California'))

    expect(toJS(ex.separateViaAnd('country'))).to.deep.equal(null)

  it 'works on mixed OR filter (all in)', ->
    ex = $('venue').is('Apple').or($('venue').is('Google').not())

    expect(toJS(ex.separateViaAnd('venue'))).to.deep.equal(toJS({
      included: ex
      excluded: Expression.TRUE
    }))

  it 'works on mixed OR filter (all out)', ->
    ex = $('venue').is('Google').or($('country').is('USA'), $('state').is('California'))

    expect(toJS(ex.separateViaAnd('model'))).to.deep.equal(toJS({
      included: Expression.TRUE
      excluded: ex
    }))
