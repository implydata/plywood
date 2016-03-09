var { expect } = require("chai");
var { sane } = require('../utils');

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { Expression, $, ply, r } = plywood;

describe("SQL parser", () => {
  describe("basic expression", () => {
    var $data = $('data');

    it("works with a literal expression", () => {
      var parse = Expression.parseSQL("1");
      var ex2 = r(1);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a fancy number expression", () => {
      var parse = Expression.parseSQL("-5e-2");
      var ex2 = r(-5e-2);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a COUNT expression", () => {
      var parse = Expression.parseSQL("COUNT()");

      var ex2 = $('data').count();

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with an arithmetic expression", () => {
      var parse = Expression.parseSQL("SUM(`added`) + 5 + SUM(deleted) / 2");

      var ex2 = $data.sum('$added').add(5, '$data.sum($deleted) / 2');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should handle --", () => {
      var parse = Expression.parseSQL("x--3");

      var ex2 = $('x').subtract(-3);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should handle fallback --", () => {
      var parse = Expression.parseSQL("IFNULL(null,'fallback')");
      var parse2 = Expression.parseSQL("IFNULL(null, SUM(deleted))");
      var parse3 = Expression.parseSQL("IFNULL(SUM(`added`), SUM(deleted))");
      var parse4 = Expression.parseSQL("FALLBACK(SUM(`added`), SUM(deleted))");

      var ex = r(null).fallback('fallback');
      var ex2 = r(null).fallback('$data.sum($deleted)');
      var ex3 = $data.sum('$added').fallback('$data.sum($deleted)');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
      expect(parse2.expression.toJS()).to.deep.equal(ex2.toJS());
      expect(parse3.expression.toJS()).to.deep.equal(ex3.toJS());
      expect(parse4.expression.toJS()).to.deep.equal(ex3.toJS());
    });

    it("works with NOW()", () => {
      var parse = Expression.parseSQL("NOW( )");

      var js = parse.expression.toJS();
      expect(js.op).to.equal('literal');
      expect(Math.abs(js.value.valueOf() - Date.now())).to.be.lessThan(1000);
    });

    it("it works with raw aggregate", () => {
      var parse = Expression.parseSQL("(SUM(added) + 1000) / 2");

      var ex2 = $('data').sum('$added').add(1000).divide(2);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    describe("date literals", () => {
      it('works with inferred literals', () => {
        var tests = sane`
          '2015-01-01T00:00:00.000' <= t AND t < '2016-01-01T00:00:00.000'
          '2015-01-01T00:00:00.00' <= t AND t < '2016-01-01T00:00:00.00'
          '2015-01-01T00:00:00.0' <= t AND t < '2016-01-01T00:00:00.0'
          '2015-01-01T00:00:00' <= t AND t < '2016-01-01T00:00:00'
          '2015-01-01T00:00' <= t AND t < '2016-01-01T00:00'
          '2015-01-01T00' <= t AND t < '2016-01-01T00'
          '2015-01-01' <= t AND t < '2016-01-01'
        `;

        var ex = r(new Date('2015-01-01T00:00:00Z')).lessThanOrEqual('$t').and($('t').lessThan(new Date('2016-01-01T00:00:00Z')));

        tests.split('\n').forEach(test => {
          var parse = Expression.parseSQL(test);
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
        });
      });

      it('works with DATE', () => {
        var tests = sane`
          DATE "2016-02-09"
          DATE "20160209"
          DATE "160209"
          DATE "2016/02$09"
          DATE'2016/02$09'
          {d'2016-02-09'}
          { d '2016-02-09' }
          { d "2016-02-09" }
          { d "2016/02/09" }
          { d "2016/02$09"}
          { D "2016/02$09" }
        `;

        var ex = r(new Date('2016-02-09Z'));
        tests.split('\n').forEach(test => {
          var parse = Expression.parseSQL(test);
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
        });
      });

      it('works with TIMESTAMP', () => {
        var tests = sane`
          TIMESTAMP "20160209010203"
          TIMESTAMP "160209010203"
          TIMESTAMP "2016-02-09 01:02:03"
          TIMESTAMP "2016/02$09 01:02:03"
          TIMESTAMP'2016/02$09 01:02:03'
          {ts'2016-02-09 01:02:03'}
          { ts '2016-2-9 1:2:3' }
          { Ts "2016-02-09T01:02:03" }
          { tS "16/02/09 01:02:03" }
          { ts "2016/02$09 01:02:03"}
          { TS "2016/02$09 01:02:03" }
        `;

        var ex = r(new Date('2016-02-09T01:02:03Z'));
        tests.split('\n').forEach(test => {
          var parse = Expression.parseSQL(test);
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
        });
      });

      it('works with TIMESTAMP.ms', () => {
        var tests = sane`
          TIMESTAMP "2016-02-09 01:02:03.456"
          TIMESTAMP "2016/02$09 01:02:03.456789"
        `;

        var ex = r(new Date('2016-02-09T01:02:03.456Z'));
        tests.split('\n').forEach(test => {
          var parse = Expression.parseSQL(test);
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
        });
      });

      it('errors on TIME', () => {
        expect(() => {
          Expression.parseSQL("{t '01:02:03'}");
        }).to.throw('time literals are not supported');

        expect(() => {
          Expression.parseSQL("TIME '01:02:03'");
        }).to.throw('time literals are not supported');
      });

      it('works inside BETWEEN', () => {
        var tests = sane`
          t BETWEEN TIMESTAMP '2015-09-12T10:30:00' AND TIMESTAMP '2015-09-12T12:30:00'
          t BETWEEN '2015-09-12T10:30:00' AND '2015-09-12T12:30:00'
          t BETWEEN '2015-09-12T10:30' AND '2015-09-12T12:30'
        `;

        var ex = $('t').in({
          bounds: "[]",
          start: new Date('2015-09-12T10:30:00Z'),
          end: new Date('2015-09-12T12:30:00Z')
        });

        tests.split('\n').forEach(test => {
          var parse = Expression.parseSQL(test);
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
        });
      });

    });

  });

  describe("other query types", () => {
    it("works with UPDATE expression", () => {
      var parse = Expression.parseSQL("UPDATE this is the end of the road");

      expect(parse).to.deep.equal({
        verb: 'UPDATE',
        rest: 'this is the end of the road'
      });
    });

    it("works with SET query", () => {
      var parse = Expression.parseSQL("SET this is the end of the road");

      expect(parse).to.deep.equal({
        verb: 'SET',
        rest: 'this is the end of the road'
      });
    });
  });


  describe("DESCRIBE", () => {
    it("works with DESCRIBE query", () => {
      var parse = Expression.parseSQL("DESCRIBE wikipedia");

      expect(parse).to.deep.equal({
        verb: 'DESCRIBE',
        table: 'wikipedia'
      });
    });

    it("works with another DESCRIBE query", () => {
      var parse = Expression.parseSQL("DESCRIBE `my-table` ; ");

      expect(parse).to.deep.equal({
        verb: 'DESCRIBE',
        table: 'my-table'
      });
    });
  });

  describe("SELECT", () => {
    it("should fail on a expression with no columns", () => {
      expect(() => {
        Expression.parseSQL("SELECT FROM wiki");
      }).to.throw('SQL parse error: Can not have empty column list on `SELECT FROM wiki`');
    });

    it("should have a good error for incorrect numeric GROUP BYs", () => {
      expect(() => {
        Expression.parseSQL("SELECT page, COUNT() AS 'Count' FROM wiki GROUP BY 12");
      }).to.throw("Unknown column '12' in group by statement");
    });

    it("should have a good error SELECT * ... GROUP BY ...", () => {
      expect(() => {
        Expression.parseSQL("SELECT * FROM wiki GROUP BY 12");
      }).to.throw("can not SELECT * with a GROUP BY");
    });

    it("should fail gracefully on expressions with multi-column sort", () => {
      expect(() => {
        Expression.parseSQL("SELECT page, COUNT() AS 'Count' FROM wiki GROUP BY page ORDER BY page DESC, `Count` ASC");
      }).to.throw('plywood does not currently support multi-column ORDER BYs');
    });

    it("should fail gracefully on COUNT(DISTINCT)", () => {
      expect(() => {
        Expression.parseSQL("COUNT(DISTINCT)");
      }).to.throw('COUNT DISTINCT must have expression');
    });

    it("should fail gracefully on SUM(DISTINCT blah)", () => {
      expect(() => {
        Expression.parseSQL("SUM(DISTINCT blah)");
      }).to.throw('can not use DISTINCT for sum aggregator');
    });

    it("should parse a simple expression", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
        COUNT() AS 'Count'
        FROM \`wiki\`
      `);

      var ex2 = ply()
        .apply('data', '$wiki')
        .apply('Count', '$data.count()');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.table).to.equal('wiki');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a total expression with all sorts of applies", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
        COUNT()  AS Count1,
        COUNT(*) AS Count2,
        COUNT(1) AS Count3,
        COUNT(\`visitor\`) AS Count4,
        MATCH(\`visitor\`, "[0-9A-F]") AS 'Match',
        SUM(added) AS 'TotalAdded',
        '2014-01-02' AS 'Date',
        SUM(\`wiki\`.\`added\`) / 4 AS TotalAddedOver4,
        NOT(true) AS 'False',
        -SUM(added) AS MinusAdded,
        ABS(MinusAdded) AS AbsAdded,
        ABSOLUTE(MinusAdded) AS AbsoluteAdded,
        POWER(MinusAdded, 0.5) AS SqRtAdded,
        POW(MinusAdded, 0.5) AS SqRtAdded2,
        SQRT(TotalAddedOver4) AS SquareRoot,
        EXP(0) AS One,
        +SUM(added) AS SimplyAdded,
        QUANTILE(added, 0.5) AS Median,
        COUNT_DISTINCT(visitor) AS 'Unique1',
        COUNT(DISTINCT visitor) AS 'Unique2',
        COUNT(DISTINCT(visitor)) AS 'Unique3',
        TIME_BUCKET(time, PT1H) AS 'TimeBucket',
        TIME_FLOOR(time, PT1H) AS 'TimeFloor',
        TIME_SHIFT(time, PT1H, 3) AS 'TimeShift3',
        TIME_RANGE(time, PT1H, 3) AS 'TimeRange3',
        OVERLAP(x, y) AS 'Overlap',
        CUSTOM('blah') AS 'Custom1'
        FROM \`wiki\`
        WHERE \`language\`="en"  ;  -- This is just some comment
      `);

      var ex2 = ply()
        .apply('data', '$wiki.filter($language == "en")')
        .apply('Count1', '$data.count()')
        .apply('Count2', '$data.count()')
        .apply('Count3', '$data.filter(1 != null).count()')
        .apply('Count4', '$data.filter($visitor != null).count()')
        .apply('Match', $('visitor').match("[0-9A-F]"))
        .apply('TotalAdded', '$data.sum($added)')
        .apply('Date', new Date('2014-01-02T00:00:00.000Z'))
        .apply('TotalAddedOver4', '$data.sum($added) / 4')
        .apply('False', r(true).not())
        .apply('MinusAdded', '-$data.sum($added)')
        .apply('AbsAdded', '$MinusAdded.absolute()')
        .apply('AbsoluteAdded', '$MinusAdded.absolute()')
        .apply('SqRtAdded', '$MinusAdded.power(0.5)')
        .apply('SqRtAdded2', '$MinusAdded.power(0.5)')
        .apply('SquareRoot', '$TotalAddedOver4.power(0.5)')
        .apply('One', r(Math.E).power(0))
        .apply('SimplyAdded', '$data.sum($added)')
        .apply('Median', $('data').quantile('$added', 0.5))
        .apply('Unique1', $('data').countDistinct('$visitor'))
        .apply('Unique2', $('data').countDistinct('$visitor'))
        .apply('Unique3', $('data').countDistinct('$visitor'))
        .apply('TimeBucket', $('time').timeBucket('PT1H'))
        .apply('TimeFloor', $('time').timeFloor('PT1H'))
        .apply('TimeShift3', $('time').timeShift('PT1H', 3))
        .apply('TimeRange3', $('time').timeRange('PT1H', 3))
        .apply('Overlap', $('x').overlap('$y'))
        .apply('Custom1', $('data').custom('blah'));

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a total expression without GROUP BY clause", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
        SUM(added) AS TotalAdded
        FROM \`wiki\`
        WHERE \`language\`="en"
      `);

      var ex2 = ply()
        .apply('data', '$wiki.filter($language == "en")')
        .apply('TotalAdded', '$data.sum($added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with all sorts of comments", () => {
      var parse = Expression.parseSQL(`/*
        Multiline comments
        can exist
        at the start ...
        */
        SELECT
        SUM(added)--1 AS /* Inline comment */ TotalAdded -- This is just some comment
        FROM \`wiki\` # Another comment
        /*
        ... and in the
        middle...
        */
        WHERE \`language\`="en"
        /*
        ... and at the
        end.
        */
      `);

      var ex2 = ply()
        .apply('data', '$wiki.filter($language == "en")')
        .apply('TotalAdded', '$data.sum($added) - -1');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work without a FROM", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
        SUM(added) AS 'TotalAdded'
        WHERE \`language\`="en"
      `);

      var ex2 = ply()
        .apply('data', '$data.filter($language == "en")')
        .apply('TotalAdded', '$data.sum($added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a BETWEEN", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
        SUM(added) AS 'TotalAdded'
        WHERE \`language\`="en" AND \`time\` BETWEEN '2015-01-01T10:30:00' AND '2015-01-02T12:30:00'
      `);

      var ex2 = ply()
        .apply('data', $('data').filter(
          $('language').is("en").and($('time').in({
            start: new Date('2015-01-01T10:30:00'),
            end: new Date('2015-01-02T12:30:00'),
            bounds: '[]'
          }))
        ))
        .apply('TotalAdded', '$data.sum($added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with <= <", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
        SUM(added) AS 'TotalAdded'
        WHERE \`language\`="en" AND '2015-01-01T10:30:00' <= \`time\` AND \`time\` < '2015-01-02T12:30:00'
      `);

      var ex2 = ply()
        .apply('data', $('data').filter(
          $('language').is("en")
            .and(r(new Date('2015-01-01T10:30:00')).lessThanOrEqual($('time')))
            .and($('time').lessThan(new Date('2015-01-02T12:30:00')))
        ))
        .apply('TotalAdded', '$data.sum($added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());

      //var ex2s = ply()
      //  .apply('data', $('data').filter(
      //    $('language').is("en").and($('time').in({
      //      start: new Date('2015-01-01T10:30:00'),
      //      end: new Date('2015-01-02T12:30:00')
      //    }))
      //  ))
      //  .apply('TotalAdded', '$data.sum($added)');
      //
      //expect(parse.expression.simplify().toJS()).to.deep.equal(ex2s.toJS());
    });

    it("should work without top level GROUP BY", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
        \`page\` AS 'Page',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        WHERE \`language\`="en" AND \`time\` BETWEEN '2015-01-01T10:30:00' AND '2015-01-02T12:30:00'
        GROUP BY 1
      `);

      var ex2 = $('wiki').filter(
        $('language').is("en").and($('time').in({
          start: new Date('2015-01-01T10:30:00'),
          end: new Date('2015-01-02T12:30:00'),
          bounds: '[]'
        }))
      ).split('$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum($added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work without top level GROUP BY with ORDER BY and LIMIT", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
        \`page\` AS 'Page',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY \`page\`
        ORDER BY TotalAdded
        LIMIT 5
      `);

      var ex2 = $('wiki').split('$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum($added)')
        .sort('$TotalAdded', 'ascending')
        .limit(5);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work without top level GROUP BY with LIMIT only", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
        \`page\` AS 'Page',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY \`page\`
        LIMIT 5
      `);

      var ex2 = $('wiki').split('$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum($added)')
        .limit(5);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with multi-dimensional GROUP BYs", () => {
      var parse = Expression.parseSQL(sane`
        SELECT \`page\`, \`user\` FROM \`wiki\` GROUP BY \`page\`, \`user\`
      `);

      var ex2 = $('wiki').split({ page: '$page', user: '$user' }, 'data');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SELECT DISTINCT", () => {
      var parse = Expression.parseSQL(sane`
        SELECT DISTINCT \`page\`, \`user\` FROM \`wiki\`
      `);

      var ex2 = $('wiki').split({ page: '$page', user: '$user' }, 'data');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with few spaces", () => {
      var parse = Expression.parseSQL(sane`
        SELECT\`page\`AS'Page'FROM\`wiki\`GROUP BY\`page\`ORDER BY\`Page\`LIMIT 5
      `);

      var ex2 = $('wiki').split('$page', 'Page', 'data')
        .sort('$Page', 'ascending')
        .limit(5);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a TIME_BUCKET function", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
        TIME_BUCKET(\`time\`, 'PT1H', 'Etc/UTC') AS 'TimeByHour',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      var ex2 = $('wiki').split($('time').timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour', 'data')
        .apply('TotalAdded', '$data.sum($added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SELECT *", () => {
      var parse = Expression.parseSQL(sane`
        SELECT * FROM \`wiki\`
      `);

      var ex2 = $('wiki');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SELECT * WHERE ...", () => {
      var parse = Expression.parseSQL(sane`
        SELECT * FROM \`wiki\` WHERE language = 'en'
      `);

      var ex2 = $('wiki')
        .filter('$language == "en"');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a SELECT query with renames", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
          page,
          isNew AS IsNew,
          added AS Added
        FROM \`wiki\`
        WHERE \`language\`="en"
      `);

      var ex2 = $('wiki')
        .filter('$language == en')
        .apply('page', '$page')
        .apply('IsNew', '$isNew')
        .apply('Added', '$added')
        .select('page', 'IsNew', 'Added');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SELECT and stuff", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
          CONCAT(page, "lol") AS Page,
          added + 1 as added
        FROM \`wiki\`
        WHERE language = 'en'
        HAVING added > 100
        ORDER BY \`page\` DESC
        LIMIT 10
      `);

      var ex2 = $('wiki')
        .filter('$language == "en"')
        .apply('Page', '$page.concat(lol)')
        .apply('added', '$added + 1')
        .select('Page', 'added')
        .filter('$added > 100')
        .sort('$page', 'descending')
        .limit(10);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a NUMBER_BUCKET function", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
        NUMBER_BUCKET(added, 10, 1 ) AS 'AddedBucket',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      var ex2 = $('wiki').split($('added').numberBucket(10, 1), 'AddedBucket', 'data')
        .apply('TotalAdded', '$data.sum($added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a TIME_PART function", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
        TIME_PART(\`time\`, DAY_OF_WEEK, 'Etc/UTC' ) AS 'DayOfWeek',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      var ex2 = $('wiki').split($('time').timePart('DAY_OF_WEEK', 'Etc/UTC'), 'DayOfWeek', 'data')
        .apply('TotalAdded', '$data.sum($added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a SUBSTR and CONCAT function", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
          CONCAT('[', SUBSTRING(SUBSTR(\`page\`, 0, 3 ), 1, 2), ']') AS 'Crazy',
          SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      var ex2 = $('wiki').split("'[' ++ $page.substr(0, 3).substr(1, 2) ++ ']'", 'Crazy', 'data')
        .apply('TotalAdded', '$data.sum($added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with EXTRACT function", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
          EXTRACT(\`page\`, '^Wiki(.+)$') AS 'Extract',
          SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      var ex2 = $('wiki').split("$page.extract('^Wiki(.+)$')", 'Extract', 'data')
        .apply('TotalAdded', '$data.sum($added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with LOOKUP function", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
          LOOKUP(\`language\`, 'language-lookup') AS 'Lookup',
          SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      var ex2 = $('wiki').split("$language.lookup('language-lookup')", 'Lookup', 'data')
        .apply('TotalAdded', '$data.sum($added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a complex filter", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
          SUM(added) AS 'TotalAdded'
        FROM \`wiki\`    -- Filters can have ANDs and all sorts of stuff!
        WHERE user="Bot Master 1" AND
          page<>"Hello World" AND
          added < 5 AND
          \`wiki\`.\`language\` IN ('ca', 'cs', 'da', 'el') AND
          \`namespace\` NOT IN ('simple', 'dict') AND
          geo IS NOT NULL AND
          page CONTAINS 'World' AND
          page LIKE '%Hello\\_World%' AND
          page LIKE '%Hello!_World%' ESCAPE '!' AND
          page REGEXP 'W[od]'
         `);

      var ex2 = ply()
        .apply(
          'data',
          $('wiki').filter(
            $('user').is(r("Bot Master 1"))
              .and($('page').isnt(r("Hello World")))
              .and($('added').lessThan(5))
              .and($('language').in(['ca', 'cs', 'da', 'el']))
              .and($('namespace').in(['simple', 'dict']).not())
              .and($('geo').isnt(null))
              .and($('page').contains('World', 'ignoreCase'))
              .and($('page').match('^.*Hello_World.*$'))
              .and($('page').match('^.*Hello_World.*$'))
              .and($('page').match('W[od]'))
          )
        )
        .apply('TotalAdded', '$data.sum($added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a total + split expression", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
          SUM(\`added\`) AS 'TotalAdded',
          (
            SELECT
            \`page\` AS 'Page',
            COUNT() AS 'Count',
            SUM(\`added\`) AS 'TotalAdded',
            min(\`added\`) AS 'MinAdded',
            mAx(\`added\`) AS 'MaxAdded'
            GROUP BY 1
            HAVING \`TotalAdded\` > 100
            ORDER BY \`Count\` DESC
            LIMIT 10
          ) AS 'Pages'
        FROM \`wiki\`
        WHERE \`language\`="en"
      `);

      var ex2 = ply()
        .apply('data', '$wiki.filter($language == "en")')
        .apply('TotalAdded', '$data.sum($added)')
        .apply(
          'Pages',
          $('data').split('$page', 'Page')
            .apply('Count', '$data.count()')
            .apply('TotalAdded', '$data.sum($added)')
            .apply('MinAdded', '$data.min($added)')
            .apply('MaxAdded', '$data.max($added)')
            .filter('$TotalAdded > 100')
            .sort('$Count', 'descending')
            .limit(10)
        );

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with fancy names", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
          \`page or else?\` AS 'Page',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki-tiki:taki\`
        WHERE \`language\`="en" AND \`time\` BETWEEN '2015-01-01T10:30:00' AND '2015-01-02T12:30:00'
        GROUP BY \`page or else?\`
      `);

      var ex2 = $('wiki-tiki:taki').filter(
        $('language').is("en").and($('time').in({
          start: new Date('2015-01-01T10:30:00'),
          end: new Date('2015-01-02T12:30:00'),
          bounds: '[]'
        }))
      ).split('${page or else?}', 'Page', 'data')
        .apply('TotalAdded', '$data.sum($added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });
  });
});
