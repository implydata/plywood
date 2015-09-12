{ expect } = require("chai")

{ WallTime } = require('chronoshift')
if not WallTime.rules
  tzData = require("chronoshift/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

plywood = require('../../build/plywood')
{ Expression, $, ply } = plywood

describe "expression parser", ->
  it "should parse the mega definition", ->
    ex = ply()
      .filter('$color = "Red"')
      .filter('$price < 5')
      .filter('$country.is("USA")')
      .apply('parent_x', "$^x")
      .apply('typed_y', "$y:STRING")
      .apply('sub_typed_z', "$z:SET/STRING")
      .apply('or', '$a or $b or $c')
      .apply('and', '$a and $b and $c')
      .apply('addition1', "$x + 10 - $y")
      .apply('addition2', "$x.add(1)")
      .apply('multiplication1', "$x * 10 / $y")
      .apply('multiplication2', "$x.multiply($y)")
      .apply('negate', "-$x")
      .apply('identity', "+$x")
      .apply('agg_count', "$data.count()")
      .apply('agg_sum', "$data.sum($price)")
      .apply('agg_average', "$data.average($price)")
      .apply('agg_min', "$data.min($price)")
      .apply('agg_max', "$data.max($price)")
      .apply('agg_quantile', "$data.quantile($price, 0.5)")
      .apply('agg_split', "$data.split($carat, 'Carat')")
      .apply('agg_filter_count', "$data.filter($country = 'USA').count()")

    ex2 = ply()
      .filter($('color').is("Red"))
      .filter($('price').lessThan(5))
      .filter($('country').is("USA"))
      .apply('parent_x', $("x", 1))
      .apply('typed_y', { op: 'ref', name: 'y', type: 'STRING' })
      .apply('sub_typed_z', { op: 'ref', name: 'z', type: 'SET/STRING' })
      .apply('or', $('a').or($('b'), $('c')))
      .apply('and', $('a').and($('b'), $('c')))
      .apply('addition1', $("x").add(10).subtract($("y")))
      .apply('addition2', $("x").add(1))
      .apply('multiplication1', $("x").multiply(10).divide($("y")))
      .apply('multiplication2', $("x").multiply($('y')))
      .apply('negate', $("x").negate())
      .apply('identity', $("x"))
      .apply('agg_count', $("data").count())
      .apply('agg_sum', $("data").sum($('price')))
      .apply('agg_average', $("data").average($('price')))
      .apply('agg_min', $("data").min($('price')))
      .apply('agg_max', $("data").max($('price')))
      .apply('agg_quantile', $("data").quantile($('price'), 0.5))
      .apply('agg_split', $("data").split($('carat'), 'Carat'))
      .apply('agg_filter_count', $("data").filter($('country').is("USA")).count())

    expect(ex.toJS()).to.deep.equal(ex2.toJS())

  it "should parse a whole expression", ->
    ex = Expression.parse("""
      ply()
        .apply(num, 5)
        .apply(subData,
          ply()
            .apply(x, $num + 1)
            .apply(y, $foo * 2)
        )
      """)

    ex2 = ply()
      .apply('num', 5)
      .apply('subData',
        ply()
          .apply('x', '$num + 1')
          .apply('y', '$foo * 2')
      )

    expect(ex.toJS()).to.deep.equal(ex2.toJS())

  it "should parse a whole complex expression", ->
    ex = Expression.parse("""
      ply()
        .apply(wiki, $wiki.filter($language = 'en'))
        .apply(Count, $wiki.sum($count))
        .apply(TotalAdded, $wiki.sum($added))
        .apply(Pages,
          $wiki.split($page, Page)
            .apply(Count, $wiki.sum($count))
            .sort($Count, descending)
            .limit(2)
            .apply(Time,
              $wiki.split($time.timeBucket(PT1H, 'Etc/UTC'), Timestamp)
                .apply(TotalAdded, $wiki.sum($added))
                .sort($TotalAdded, descending)
                .limit(3)
            )
        )
      """)

    ex2 = ply()
      .apply("wiki", $('wiki').filter($("language").is('en')))
      .apply('Count', '$wiki.sum($count)')
      .apply('TotalAdded', '$wiki.sum($added)')
      .apply('Pages',
        $("wiki").split("$page", 'Page')
          .apply('Count', '$wiki.sum($count)')
          .sort('$Count', 'descending')
          .limit(2)
          .apply('Time',
            $("wiki").split($("time").timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
              .apply('TotalAdded', '$wiki.sum($added)')
              .sort('$TotalAdded', 'descending')
              .limit(3)
          )
      )

    expect(ex.toJS()).to.deep.equal(ex2.toJS())

  it "should complain on identity misuse (on non numbers)", ->
    expect(->
      Expression.parse("+'poo'")
    ).to.throw("Expression parse error: subtract must have expression of type NUMBER (is STRING) on `+\'poo\'`")
