{ expect } = require("chai")

{ WallTime } = require('chronoshift')
if not WallTime.rules
  tzData = require("chronoshift/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

plywood = require('../../build/plywood')
{ Expression, External, TimeRange, $, ply } = plywood

context = {
  diamonds: External.fromJS({
    engine: 'mysql',
    table: 'diamonds',
    attributes: {
      time: { type: 'TIME' }
      color: { type: 'STRING' }
      cut: { type: 'STRING' }
      tags: { type: 'SET/STRING' }
      carat: { type: 'NUMBER' }
      height_bucket: { special: 'range', separator: ';', rangeSize: 0.05, digitsAfterDecimal: 2 }
      price: { type: 'NUMBER' }
      tax: { type: 'NUMBER' }
    }
#    filter: $("time").in(TimeRange.fromJS({
#      start: new Date('2015-03-12T00:00:00')
#      end:   new Date('2015-03-19T00:00:00')
#    }))
  })
}

describe "simulate MySQL", ->
  it "works in advanced case", ->
    ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").is('D')))
      .apply('Count', '$diamonds.count()')
      .apply('TotalPrice', '$diamonds.sum($price)')
      .apply('PriceTimes2', '$diamonds.sum($price) * 2')
      .apply('PriceMinusTax', '$diamonds.sum($price) - $diamonds.sum($tax)')
      .apply('Crazy', '$diamonds.sum($price) - $diamonds.sum($tax) + 10 - $diamonds.sum($carat)')
      .apply('PriceAndTax', '$diamonds.sum($price) + $diamonds.sum($tax)')
      #.apply('PriceGoodCut', $('diamonds').filter($('cut').is('good')).sum('$price'))
      .apply('Cuts',
        $("diamonds").split("$cut", 'Cut')
          .apply('Count', $('diamonds').count())
          .apply('PercentOfTotal', '$^Count / $Count')
          .sort('$Count', 'descending')
          .limit(2)
          .apply('Time',
            $("diamonds").split($("time").timeBucket('P1D', 'America/Los_Angeles'), 'Timestamp')
              .apply('TotalPrice', $('diamonds').sum('$price'))
              .sort('$Timestamp', 'ascending')
              #.limit(10)
              .apply('Carats',
                $("diamonds").split($("carat").numberBucket(0.25), 'Carat')
                  .apply('Count', $('diamonds').count())
                  .sort('$Count', 'descending')
                  .limit(3)
              )
          )
      )

    queryPlan = ex.simulateQueryPlan(context)
    expect(queryPlan).to.have.length(4)

    expect(queryPlan[0]).to.equal("""
      SELECT
      COUNT(*) AS 'Count',
      SUM(`price`) AS 'TotalPrice',
      (SUM(`price`)*2) AS 'PriceTimes2',
      (SUM(`price`)-SUM(`tax`)) AS 'PriceMinusTax',
      (((SUM(`price`)-SUM(`tax`))+10)-SUM(`carat`)) AS 'Crazy',
      (SUM(`price`)+SUM(`tax`)) AS 'PriceAndTax'
      FROM `diamonds`
      WHERE (`color`="D")
      GROUP BY ''
      """)

    expect(queryPlan[1]).to.equal("""
      SELECT
      `cut` AS 'Cut',
      COUNT(*) AS 'Count',
      (4/`Count`) AS 'PercentOfTotal'
      FROM `diamonds`
      WHERE (`color`="D")
      GROUP BY `cut`
      ORDER BY `Count` DESC
      LIMIT 2
      """)

    expect(queryPlan[2]).to.equal("""
      SELECT
      DATE_FORMAT(CONVERT_TZ(`time`, '+0:00', 'America/Los_Angeles'), '%Y-%m-%dT00:00:00Z') AS 'Timestamp',
      SUM(`price`) AS 'TotalPrice'
      FROM `diamonds`
      WHERE ((`color`="D") AND (`cut`="some_cut"))
      GROUP BY DATE_FORMAT(CONVERT_TZ(`time`, '+0:00', 'America/Los_Angeles'), '%Y-%m-%dT00:00:00Z')
      ORDER BY `Timestamp` ASC
      """)

    expect(queryPlan[3]).to.equal("""
      SELECT
      FLOOR(`carat` / 0.25) * 0.25 AS 'Carat',
      COUNT(*) AS 'Count'
      FROM `diamonds`
      WHERE (((`color`="D") AND (`cut`="some_cut")) AND ('2015-03-13 07:00:00'<=`time` AND `time`<'2015-03-14 07:00:00'))
      GROUP BY FLOOR(`carat` / 0.25) * 0.25
      ORDER BY `Count` DESC
      LIMIT 3
      """)


  it "works with having filter", ->
    ex = $("diamonds").split("$cut", 'Cut')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending')
      .filter($('Count').greaterThan(100))
      .limit(10)

    queryPlan = ex.simulateQueryPlan(context)
    expect(queryPlan).to.have.length(1)

    expect(queryPlan[0]).to.equal("""
      SELECT
      `cut` AS 'Cut',
      COUNT(*) AS 'Count'
      FROM `diamonds`
      GROUP BY `cut`
      HAVING 100<`Count`
      ORDER BY `Count` DESC
      LIMIT 10
      """)

  it "works with range bucket", ->
    ex = ply()
      .apply('HeightBuckets',
        $("diamonds").split("$height_bucket", 'HeightBucket')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(10)
      )
      .apply('HeightUpBuckets',
        $("diamonds").split($('height_bucket').numberBucket(2, 0.5), 'HeightBucket')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(10)
      )

    queryPlan = ex.simulateQueryPlan(context)
    expect(queryPlan).to.have.length(2)

    expect(queryPlan[0]).to.equal("""
      SELECT
      `height_bucket` AS 'HeightBucket',
      COUNT(*) AS 'Count'
      FROM `diamonds`
      GROUP BY `height_bucket`
      ORDER BY `Count` DESC
      LIMIT 10
      """)

    expect(queryPlan[1]).to.equal("""
      SELECT
      FLOOR((`height_bucket` - 0.5) / 2) * 2 + 0.5 AS 'HeightBucket',
      COUNT(*) AS 'Count'
      FROM `diamonds`
      GROUP BY FLOOR((`height_bucket` - 0.5) / 2) * 2 + 0.5
      ORDER BY `Count` DESC
      LIMIT 10
      """)
