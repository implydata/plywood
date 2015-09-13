{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, External, $, ply, r } = plywood

describe.skip "breakdown", ->
  context = {
    x: 1
    y: 2
    diamonds: External.fromJS({
      engine: 'druid',
      dataSource: 'diamonds',
      timeAttribute: 'time',
      context: null
      attributes: {
        time: { type: 'TIME' }
        color: { type: 'STRING' }
        cut: { type: 'STRING' }
        carat: { type: 'NUMBER' }
      }
    })
    diamonds2: External.fromJS({
      engine: 'druid',
      dataSource: 'diamonds2',
      timeAttribute: 'time',
      context: null
      attributes: {
        time: { type: 'TIME' }
        color: { type: 'STRING' }
        cut: { type: 'STRING' }
        carat: { type: 'NUMBER' }
      }
    })
  }

  it "errors on breakdown zero datasets", ->
    ex = Expression.parse('$x * $y + 2')

    ex = ex.referenceCheck(context)
    expect(-> ex.breakdownByDataset('b')).to.throw()

  it "errors on breakdown one datasets", ->
    ex = Expression.parse('$diamonds.count() * 2')

    ex = ex.referenceCheck(context)
    expect(-> ex.breakdownByDataset('b')).to.throw()


  it "breakdown two datasets correctly", ->
    ex = Expression.parse('$diamonds.count() * $diamonds2.count() + $diamonds.sum($carat)')

    ex = ex.referenceCheck(context)
    breakdown = ex.breakdownByDataset('b')
    expect(breakdown.singleDatasetActions.join(' | ')).to.equal(
      '.apply(b0, $diamonds:DATASET.count()) | .apply(b1, $diamonds2:DATASET.count()) | .apply(b2, $diamonds:DATASET.sum($carat:NUMBER))'
    )
    expect(breakdown.combineExpression.toString()).to.equal('(($b0 * $b1) + $b2)')

  it "breakdown two datasets correctly (and de-duplicates expression)", ->
    ex = Expression.parse('$diamonds.count() * $diamonds2.sum($carat) + $diamonds.count()')

    ex = ex.referenceCheck(context)
    breakdown = ex.breakdownByDataset('b')
    expect(breakdown.singleDatasetActions.join(' | ')).to.equal(
      '.apply(b0, $diamonds:DATASET.count()) | .apply(b1, $diamonds2:DATASET.sum($carat:NUMBER))'
    )
    expect(breakdown.combineExpression.toString()).to.equal('(($b0 * $b1) + $b0)')
