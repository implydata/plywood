var { expect } = require("chai");
var { sane } = require('../utils');

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { External, $, ply, r } = plywood;

var context = {
  diamonds: External.fromJS({
    engine: 'mysql',
    table: 'diamonds',
    attributes: [
      { name: 'time', type: 'TIME' },
      { name: 'color', type: 'STRING' },
      { name: 'cut', type: 'STRING' },
      { name: 'tags', type: 'SET/STRING' },
      { name: 'carat', type: 'NUMBER' },
      { name: 'height_bucket', special: 'range', separator: ';', rangeSize: 0.05, digitsAfterDecimal: 2 },
      { name: 'price', type: 'NUMBER' },
      { name: 'tax', type: 'NUMBER' }
    ]
  })
};

describe("simulate MySQL", () => {
  it("works in advanced case", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").is('D')))
      .apply('Count', '$diamonds.count()')
      .apply('TotalPrice', '$diamonds.sum($price)')
      .apply('PriceTimes2', '$diamonds.sum($price) * 2')
      .apply('PriceMinusTax', '$TotalPrice - $diamonds.sum($tax)')
      .apply('Crazy', '$diamonds.sum($price) - $diamonds.sum($tax) + 10 - $diamonds.sum($carat)')
      .apply('PriceAndTax', '$diamonds.sum($price) + $diamonds.sum($tax)')
      .apply('PriceGoodCut', '$diamonds.filter($cut == good).sum($price)')
      .apply(
        'Cuts',
        $("diamonds").split("$cut", 'Cut')
          .apply('Count', $('diamonds').count())
          .apply('PercentOfTotal', '$Count / $^Count')
          .sort('$Count', 'descending')
          .limit(2)
          .apply(
            'Time',
            $("diamonds").split($("time").timeBucket('P1D', 'America/Los_Angeles'), 'Timestamp')
              .apply('TotalPrice', $('diamonds').sum('$price'))
              .sort('$Timestamp', 'ascending')
              //.limit(10)
              .apply(
                'Carats',
                $("diamonds").split($("carat").numberBucket(0.25), 'Carat')
                  .apply('Count', $('diamonds').count().fallback(0))
                  .sort('$Count', 'descending')
                  .limit(3)
              )
          )
      );

    var queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(4);

    expect(queryPlan[0]).to.equal(sane`
      SELECT
      COUNT(*) AS \`Count\`,
      SUM(\`price\`) AS \`TotalPrice\`,
      (SUM(\`price\`)*2) AS \`PriceTimes2\`,
      (SUM(\`price\`)-SUM(\`tax\`)) AS \`PriceMinusTax\`,
      (((SUM(\`price\`)-SUM(\`tax\`))+10)-SUM(\`carat\`)) AS \`Crazy\`,
      (SUM(\`price\`)+SUM(\`tax\`)) AS \`PriceAndTax\`,
      SUM(IF((\`cut\`<=>"good"),\`price\`,0)) AS \`PriceGoodCut\`
      FROM \`diamonds\`
      WHERE (\`color\`<=>"D")
      GROUP BY ''
    `);

    expect(queryPlan[1]).to.equal(sane`
      SELECT
      \`cut\` AS \`Cut\`,
      COUNT(*) AS \`Count\`,
      (COUNT(*)/4) AS \`PercentOfTotal\`
      FROM \`diamonds\`
      WHERE (\`color\`<=>"D")
      GROUP BY 1
      ORDER BY \`Count\` DESC
      LIMIT 2
    `);

    expect(queryPlan[2]).to.equal(sane`
      SELECT
      CONVERT_TZ(DATE_FORMAT(CONVERT_TZ(\`time\`,'+0:00','America/Los_Angeles'),'%Y-%m-%d 00:00:00Z'),'America/Los_Angeles','+0:00') AS \`Timestamp\`,
      SUM(\`price\`) AS \`TotalPrice\`
      FROM \`diamonds\`
      WHERE ((\`color\`<=>"D") AND (\`cut\`<=>"some_cut"))
      GROUP BY 1
      ORDER BY \`Timestamp\` ASC
    `);

    expect(queryPlan[3]).to.equal(sane`
      SELECT
      FLOOR(\`carat\` / 0.25) * 0.25 AS \`Carat\`,
      COALESCE(COUNT(*), 0) AS \`Count\`
      FROM \`diamonds\`
      WHERE (((\`color\`<=>"D") AND (\`cut\`<=>"some_cut")) AND (TIMESTAMP('2015-03-13 07:00:00')<=\`time\` AND \`time\`<TIMESTAMP('2015-03-14 07:00:00')))
      GROUP BY 1
      ORDER BY \`Count\` DESC
      LIMIT 3
    `);
  });

  it("works with up reference", () => {
    var ex = ply()
      .apply('Count', '$diamonds.count()')
      .apply(
        'Cuts',
        $("diamonds").split("$cut", 'Cut')
          .apply('Count', $('diamonds').count())
          .apply('PercentOfTotal', '$Count / $^Count')
      );

    var queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(2);

    expect(queryPlan[0]).to.equal(sane`
      SELECT
      COUNT(*) AS \`Count\`
      FROM \`diamonds\`
      GROUP BY ''
    `);

    expect(queryPlan[1]).to.equal(sane`
      SELECT
      \`cut\` AS \`Cut\`,
      COUNT(*) AS \`Count\`,
      (COUNT(*)/4) AS \`PercentOfTotal\`
      FROM \`diamonds\`
      GROUP BY 1
    `);
  });

  it("works with having filter", () => {
    var ex = $("diamonds").split("$cut", 'Cut')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending')
      .filter($('Count').greaterThan(100))
      .limit(10);

    var queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(1);

    expect(queryPlan[0]).to.equal(sane`
      SELECT
      \`cut\` AS \`Cut\`,
      COUNT(*) AS \`Count\`
      FROM \`diamonds\`
      GROUP BY 1
      HAVING 100<\`Count\`
      ORDER BY \`Count\` DESC
      LIMIT 10
    `);
  });

  it("works with range bucket", () => {
    var ex = ply()
      .apply(
        'HeightBuckets',
        $("diamonds").split("$height_bucket", 'HeightBucket')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(10)
      )
      .apply(
        'HeightUpBuckets',
        $("diamonds").split($('height_bucket').numberBucket(2, 0.5), 'HeightBucket')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(10)
      );

    var queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(2);

    expect(queryPlan[0]).to.equal(sane`
      SELECT
      \`height_bucket\` AS \`HeightBucket\`,
      COUNT(*) AS \`Count\`
      FROM \`diamonds\`
      GROUP BY 1
      ORDER BY \`Count\` DESC
      LIMIT 10
    `);

    expect(queryPlan[1]).to.equal(sane`
      SELECT
      FLOOR((\`height_bucket\` - 0.5) / 2) * 2 + 0.5 AS \`HeightBucket\`,
      COUNT(*) AS \`Count\`
      FROM \`diamonds\`
      GROUP BY 1
      ORDER BY \`Count\` DESC
      LIMIT 10
    `);
  });

  it("works with SELECT query", () => {
    var ex = $('diamonds')
      .filter('$color == "D"')
      .sort('$cut', 'descending')
      .limit(10);

    var queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(1);

    expect(queryPlan[0]).to.equal(sane`
      SELECT
      \`time\`, \`color\`, \`cut\`, \`tags\`, \`carat\`, \`height_bucket\`, \`price\`, \`tax\`
      FROM \`diamonds\`
      WHERE (\`color\`<=>"D")
      ORDER BY \`cut\` DESC
      LIMIT 10
    `);
  });

  it("works with value query", () => {
    var ex = $('diamonds').filter('$color == "D"').sum('$price');

    var queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(1);

    expect(queryPlan[0]).to.equal(sane`
      SELECT
      SUM(\`price\`) AS \`__VALUE__\`
      FROM \`diamonds\`
      WHERE (\`color\`<=>"D")
      GROUP BY ''
    `);
  });

  it("works with BOOLEAN bucket", () => {
    var ex = $("diamonds").split("$color == A", 'ColorIsA')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending');

    var queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(1);

    expect(queryPlan[0]).to.equal(sane`
      SELECT
      (\`color\`<=>"A") AS \`ColorIsA\`,
      COUNT(*) AS \`Count\`
      FROM \`diamonds\`
      GROUP BY 1
      ORDER BY \`Count\` DESC
    `);
  });

  it("works multi-dimensional GROUP BYs", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").in(['A', 'B', 'some_color'])))
      .apply(
        'Cuts',
        $("diamonds").split({ 'Cut': "$cut", 'Color': '$color' })
          .apply('Count', $('diamonds').count())
          .limit(3)
          .apply(
            'Carats',
            $("diamonds").split($("carat").numberBucket(0.25), 'Carat')
              .apply('Count', $('diamonds').count())
              .sort('$Count', 'descending')
              .limit(3)
          )
      );

    var queryPlan = ex.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(2);

    expect(queryPlan[0]).to.equal(sane`
      SELECT
      \`color\` AS \`Color\`,
      \`cut\` AS \`Cut\`,
      COUNT(*) AS \`Count\`
      FROM \`diamonds\`
      WHERE \`color\` IN ("A","B","some_color")
      GROUP BY 1, 2
      LIMIT 3
    `);

    expect(queryPlan[1]).to.equal(sane`
      SELECT
      FLOOR(\`carat\` / 0.25) * 0.25 AS \`Carat\`,
      COUNT(*) AS \`Count\`
      FROM \`diamonds\`
      WHERE ((\`color\`<=>"some_color") AND (\`cut\`<=>"some_cut"))
      GROUP BY 1
      ORDER BY \`Count\` DESC
      LIMIT 3
    `);
  });
});
