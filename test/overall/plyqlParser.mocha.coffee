{ expect } = require("chai")

{ WallTime } = require('chronoshift')
if not WallTime.rules
  tzData = require("chronoshift/lib/walltime/walltime-data.js")
  WallTime.init(tzData.rules, tzData.zones)

plywood = require('../../build/plywood')
{ Expression, $, ply, r } = plywood

describe "SQL parser", ->

  describe "basic expression", ->
    $data = $('data');

    it "works with a COUNT expression", ->
      parse = Expression.parseSQL("COUNT()")

      ex2 = $('data').count()

      expect(parse.verb).to.equal(null)
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "works with an arithmetic expression", ->
      parse = Expression.parseSQL("SUM(`added`) + 5 + SUM(deleted) / 2")

      ex2 = $data.sum('$added').add(5, '$data.sum($deleted) / 2')

      expect(parse.verb).to.equal(null)
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should handle --", ->
      parse = Expression.parseSQL("x--3")

      ex2 = $('x').subtract(-3)

      expect(parse.verb).to.equal(null)
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "works with NOW()", ->
      parse = Expression.parseSQL("NOW( )")

      js = parse.expression.toJS();
      expect(js.op).to.equal('literal')
      expect(Math.abs(js.value.valueOf() - Date.now())).to.be.lessThan(1000)


  describe "other query types", ->
    it "works with UPDATE expression", ->
      parse = Expression.parseSQL("UPDATE this is the end of the road")

      expect(parse).to.deep.equal({
        verb: 'UPDATE'
        rest: 'this is the end of the road'
      })

    it "works with SET query", ->
      parse = Expression.parseSQL("SET this is the end of the road")

      expect(parse).to.deep.equal({
        verb: 'SET'
        rest: 'this is the end of the road'
      })


  describe "DESCRIBE", ->
    it "works with DESCRIBE query", ->
      parse = Expression.parseSQL("DESCRIBE wikipedia")

      expect(parse).to.deep.equal({
        verb: 'DESCRIBE'
        table: 'wikipedia'
      })

    it "works with another DESCRIBE query", ->
      parse = Expression.parseSQL("DESCRIBE `my-table` ; ")

      expect(parse).to.deep.equal({
        verb: 'DESCRIBE'
        table: 'my-table'
      })

  describe "SELECT", ->
    it "should fail on a expression with no columns", ->
      expect(->
        Expression.parseSQL("SELECT FROM wiki")
      ).to.throw('SQL parse error: Can not have empty column list on `SELECT FROM wiki`')

    it "should have a good error for incorrect numeric GROUP BYs", ->
      expect(->
        Expression.parseSQL("SELECT page, COUNT() AS 'Count' FROM wiki GROUP BY 12")
      ).to.throw("Unknown column '12' in group by statement")

    it "should have a good error SELECT * ... GROUP BY ...", ->
      expect(->
        Expression.parseSQL("SELECT * FROM wiki GROUP BY 12")
      ).to.throw("can not SELECT * with a GROUP BY")

    it "should fail gracefully on expressions with multi-column sort", ->
      expect(->
        Expression.parseSQL("SELECT page, COUNT() AS 'Count' FROM wiki GROUP BY page ORDER BY page DESC, `Count` ASC")
      ).to.throw('plywood does not currently support multi-column ORDER BYs')

    it "should fail gracefully on COUNT(DISTINCT)", ->
      expect(->
        Expression.parseSQL("COUNT(DISTINCT)")
      ).to.throw('COUNT DISTINCT must have expression')

    it "should fail gracefully on SUM(DISTINCT blah)", ->
      expect(->
        Expression.parseSQL("SUM(DISTINCT blah)")
      ).to.throw('can not use DISTINCT for sum aggregator')

    it "should parse a simple expression", ->
      parse = Expression.parseSQL("""
        SELECT
        COUNT() AS 'Count'
        FROM `wiki`;
        """)

      ex2 = ply()
        .apply('data', '$wiki')
        .apply('Count', '$data.count()')

      expect(parse.verb).to.equal('SELECT')
      expect(parse.table).to.equal('wiki')
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should parse a total expression with all sorts of applies", ->
      parse = Expression.parseSQL("""
        SELECT
        COUNT()  AS Count1,
        COUNT(*) AS Count2,
        COUNT(1) AS Count3,
        COUNT(`visitor`) AS Count4,
        MATCH(`visitor`, "[0-9A-F]") AS 'Match',
        SUM(added) AS 'TotalAdded',
        '2014-01-02' AS 'Date',
        SUM(`wiki`.`added`) / 4 AS TotalAddedOver4,
        NOT(true) AS 'False',
        -SUM(added) AS MinusAdded,
        +SUM(added) AS SimplyAdded,
        QUANTILE(added, 0.5) AS Median,
        COUNT_DISTINCT(visitor) AS 'Unique1',
        COUNT(DISTINCT visitor) AS 'Unique2',
        COUNT(DISTINCT(visitor)) AS 'Unique3',
        TIME_BUCKET(time, PT1H) AS 'TimeBucket',
        TIME_FLOOR(time, PT1H) AS 'TimeFloor',
        TIME_SHIFT(time, PT1H, 3) AS 'TimeShift3',
        TIME_RANGE(time, PT1H, 3) AS 'TimeRange3',
        CUSTOM('blah') AS 'Custom1'
        FROM `wiki`
        WHERE `language`="en"  ;  -- This is just some comment
        """)

      ex2 = ply()
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
        .apply('SimplyAdded', '$data.sum($added)')
        .apply('Median', $('data').quantile('$added', 0.5))
        .apply('Unique1', $('data').countDistinct('$visitor'))
        .apply('Unique2', $('data').countDistinct('$visitor'))
        .apply('Unique3', $('data').countDistinct('$visitor'))
        .apply('TimeBucket', $('time').timeBucket('PT1H'))
        .apply('TimeFloor', $('time').timeFloor('PT1H'))
        .apply('TimeShift3', $('time').timeShift('PT1H', 3))
        .apply('TimeRange3', $('time').timeRange('PT1H', 3))
        .apply('Custom1', $('data').custom('blah'))

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should parse a total expression without group by clause", ->
      parse = Expression.parseSQL("""
        SELECT
        SUM(added) AS TotalAdded
        FROM `wiki`
        WHERE `language`="en"
        """)

      ex2 = ply()
      .apply('data', '$wiki.filter($language == "en")')
      .apply('TotalAdded', '$data.sum($added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with all sorts of comments", ->
      parse = Expression.parseSQL("""
        /*
        Multiline comments
        can exist
        at the start ...
        */
        SELECT
        SUM(added)--1 AS /* Inline comment */ TotalAdded -- This is just some comment
        FROM `wiki` # Another comment
        /*
        ... and in the
        middle...
        */
        WHERE `language`="en"
        /*
        ... and at the
        end.
        */
        """)

      ex2 = ply()
        .apply('data', '$wiki.filter($language == "en")')
        .apply('TotalAdded', '$data.sum($added) - -1')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work without a FROM", ->
      parse = Expression.parseSQL("""
        SELECT
        SUM(added) AS 'TotalAdded'
        WHERE `language`="en"
        """)

      ex2 = ply()
        .apply('data', '$data.filter($language == "en")')
        .apply('TotalAdded', '$data.sum($added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with a BETWEEN", ->
      parse = Expression.parseSQL("""
        SELECT
        SUM(added) AS 'TotalAdded'
        WHERE `language`="en" AND `time` BETWEEN '2015-01-01T10:30:00' AND '2015-01-02T12:30:00'
        """)

      ex2 = ply()
        .apply('data', $('data').filter(
          $('language').is("en").and($('time').in({
            start: new Date('2015-01-01T10:30:00'),
            end: new Date('2015-01-02T12:30:00'),
            bounds: '[]'
          }))
        ))
        .apply('TotalAdded', '$data.sum($added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with <= <", ->
      parse = Expression.parseSQL("""
        SELECT
        SUM(added) AS 'TotalAdded'
        WHERE `language`="en" AND '2015-01-01T10:30:00' <= `time` AND `time` < '2015-01-02T12:30:00'
        """)

      ex2 = ply()
        .apply('data', $('data').filter(
          $('language').is("en")
            .and(r(new Date('2015-01-01T10:30:00')).lessThanOrEqual($('time')))
            .and($('time').lessThan(new Date('2015-01-02T12:30:00')))
        ))
        .apply('TotalAdded', '$data.sum($added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

      return

      ex2s = ply()
        .apply('data', $('data').filter(
          $('language').is("en").and($('time').in({
            start: new Date('2015-01-01T10:30:00'),
            end: new Date('2015-01-02T12:30:00')
          }))
        ))
        .apply('TotalAdded', '$data.sum($added)')

      expect(parse.expression.simplify().toJS()).to.deep.equal(ex2s.toJS())

    it "should work without top level GROUP BY", ->
      parse = Expression.parseSQL("""
        SELECT
        `page` AS 'Page',
        SUM(added) AS 'TotalAdded'
        FROM `wiki`
        WHERE `language`="en" AND `time` BETWEEN '2015-01-01T10:30:00' AND '2015-01-02T12:30:00'
        GROUP BY 1
        """)

      ex2 = $('wiki').filter(
        $('language').is("en").and($('time').in({
          start: new Date('2015-01-01T10:30:00'),
          end: new Date('2015-01-02T12:30:00'),
          bounds: '[]'
        }))
      ).split('$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum($added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work without top level GROUP BY with ORDER BY and LIMIT", ->
      parse = Expression.parseSQL("""
        SELECT
        `page` AS 'Page',
        SUM(added) AS 'TotalAdded'
        FROM `wiki`
        GROUP BY `page`
        ORDER BY TotalAdded
        LIMIT 5
        """)

      ex2 = $('wiki').split('$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum($added)')
        .sort('$TotalAdded', 'ascending')
        .limit(5)

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work without top level GROUP BY with LIMIT only", ->
      parse = Expression.parseSQL("""
        SELECT
        `page` AS 'Page',
        SUM(added) AS 'TotalAdded'
        FROM `wiki`
        GROUP BY `page`
        LIMIT 5
        """)

      ex2 = $('wiki').split('$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum($added)')
        .limit(5)

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with multi-dimensional GROUP BYs", ->
      parse = Expression.parseSQL("""
        SELECT `page`, `user` FROM `wiki` GROUP BY `page`, `user`
        """)

      ex2 = $('wiki').split({ page: '$page', user: '$user' }, 'data')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with few spaces", ->
      parse = Expression.parseSQL("""
        SELECT`page`AS'Page'FROM`wiki`GROUP BY`page`ORDER BY`Page`LIMIT 5
        """)

      ex2 = $('wiki').split('$page', 'Page', 'data')
        .sort('$Page', 'ascending')
        .limit(5)

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with a TIME_BUCKET function", ->
      parse = Expression.parseSQL("""
        SELECT
        TIME_BUCKET(`time`, 'PT1H', 'Etc/UTC') AS 'TimeByHour',
        SUM(added) AS 'TotalAdded'
        FROM `wiki`
        GROUP BY 1
        """)

      ex2 = $('wiki').split($('time').timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour', 'data')
        .apply('TotalAdded', '$data.sum($added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with SELECT *", ->
      parse = Expression.parseSQL("""
        SELECT * FROM `wiki`
        """)

      ex2 = $('wiki')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with SELECT * WHERE ...", ->
      parse = Expression.parseSQL("""
        SELECT * FROM `wiki` WHERE language = 'en'
        """)

      ex2 = $('wiki')
        .filter('$language == "en"')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with SELECT stuff", ->
      parse = Expression.parseSQL("""
        SELECT `page`, `added` -- these are completly ignored for now
        FROM `wiki`
        WHERE language = 'en'
        HAVING added > 100
        ORDER BY `page` DESC
        LIMIT 10
        """)

      ex2 = $('wiki')
        .filter('$language == "en"')
        .filter('$added > 100')
        .sort('$page', 'descending')
        .limit(10)

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with a NUMBER_BUCKET function", ->
      parse = Expression.parseSQL("""
        SELECT
        NUMBER_BUCKET(added, 10, 1 ) AS 'AddedBucket',
        SUM(added) AS 'TotalAdded'
        FROM `wiki`
        GROUP BY 1
        """)

      ex2 = $('wiki').split($('added').numberBucket(10, 1), 'AddedBucket', 'data')
        .apply('TotalAdded', '$data.sum($added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with a TIME_PART function", ->
      parse = Expression.parseSQL("""
        SELECT
        TIME_PART(`time`, DAY_OF_WEEK, 'Etc/UTC' ) AS 'DayOfWeek',
        SUM(added) AS 'TotalAdded'
        FROM `wiki`
        GROUP BY 1
        """)

      ex2 = $('wiki').split($('time').timePart('DAY_OF_WEEK', 'Etc/UTC'), 'DayOfWeek', 'data')
        .apply('TotalAdded', '$data.sum($added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with a SUBSTR and CONCAT function", ->
      parse = Expression.parseSQL("""
        SELECT
        CONCAT('[', SUBSTRING(SUBSTR(`page`, 0, 3 ), 1, 2), ']') AS 'Crazy',
        SUM(added) AS 'TotalAdded'
        FROM `wiki`
        GROUP BY 1
        """)

      ex2 = $('wiki').split("'[' ++ $page.substr(0, 3).substr(1, 2) ++ ']'", 'Crazy', 'data')
        .apply('TotalAdded', '$data.sum($added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with EXTRACT function", ->
      parse = Expression.parseSQL("""
        SELECT
        EXTRACT(`page`, '^Wiki(.+)$') AS 'Extract',
        SUM(added) AS 'TotalAdded'
        FROM `wiki`
        GROUP BY 1
        """)

      ex2 = $('wiki').split("$page.extract('^Wiki(.+)$')", 'Extract', 'data')
        .apply('TotalAdded', '$data.sum($added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with LOOKUP function", ->
      parse = Expression.parseSQL("""
        SELECT
        LOOKUP(`language`, 'language-lookup') AS 'Lookup',
        SUM(added) AS 'TotalAdded'
        FROM `wiki`
        GROUP BY 1
        """)

      ex2 = $('wiki').split("$language.lookup('language-lookup')", 'Lookup', 'data')
        .apply('TotalAdded', '$data.sum($added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should parse a complex filter", ->
      parse = Expression.parseSQL("""
        SELECT
        SUM(added) AS 'TotalAdded'
        FROM `wiki`    -- Filters can have ANDs and all sorts of stuff!
        WHERE user="Bot Master 1" AND
          page<>"Hello World" AND
          added < 5 AND
          `wiki`.`language` IN ('ca', 'cs', 'da', 'el') AND
          `namespace` NOT IN ('simple', 'dict') AND
          geo IS NOT NULL AND
          page CONTAINS 'World' AND
          page LIKE '%Hello\\_World%' AND
          page LIKE '%Hello!_World%' ESCAPE '!' AND
          page REGEXP 'W[od]'
        """)

      ex2 = ply()
        .apply('data',
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
        .apply('TotalAdded', '$data.sum($added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should parse a total + split expression", ->
      parse = Expression.parseSQL("""
        SELECT
        SUM(`added`) AS 'TotalAdded',
        (
          SELECT
          `page` AS 'Page',
          COUNT() AS 'Count',
          SUM(`added`) AS 'TotalAdded',
          min(`added`) AS 'MinAdded',
          mAx(`added`) AS 'MaxAdded'
          GROUP BY 1
          HAVING `TotalAdded` > 100
          ORDER BY `Count` DESC
          LIMIT 10
        ) AS 'Pages'
        FROM `wiki`
        WHERE `language`="en"
        """)

      ex2 = ply()
        .apply('data', '$wiki.filter($language == "en")')
        .apply('TotalAdded', '$data.sum($added)')
        .apply('Pages',
          $('data').split('$page', 'Page')
            .apply('Count', '$data.count()')
            .apply('TotalAdded', '$data.sum($added)')
            .apply('MinAdded', '$data.min($added)')
            .apply('MaxAdded', '$data.max($added)')
            .filter('$TotalAdded > 100')
            .sort('$Count', 'descending')
            .limit(10)
        )

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should work with fancy names", ->
      parse = Expression.parseSQL("""
        SELECT
        `page or else?` AS 'Page',
        SUM(added) AS 'TotalAdded'
        FROM `wiki-tiki:taki`
        WHERE `language`="en" AND `time` BETWEEN '2015-01-01T10:30:00' AND '2015-01-02T12:30:00'
        GROUP BY `page or else?`
        """)

      ex2 = $('wiki-tiki:taki').filter(
        $('language').is("en").and($('time').in({
          start: new Date('2015-01-01T10:30:00'),
          end: new Date('2015-01-02T12:30:00'),
          bounds: '[]'
        }))
      ).split('${page or else?}', 'Page', 'data')
        .apply('TotalAdded', '$data.sum($added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())
