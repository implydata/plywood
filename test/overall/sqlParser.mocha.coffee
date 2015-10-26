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


  describe "non SELECT expression", ->
    it "works with UPDATE expression", ->
      parse = Expression.parseSQL("UPDATE this is the end of the road")

      expect(parse).to.deep.equal({
        verb: 'UPDATE'
        rest: 'this is the end of the road'
      })

    it "works with SET expression", ->
      parse = Expression.parseSQL("SET this is the end of the road")

      expect(parse).to.deep.equal({
        verb: 'SET'
        rest: 'this is the end of the road'
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

    it "should fail gracefully on expressions with multi-column sort", ->
      expect(->
        Expression.parseSQL("SELECT page, COUNT() AS 'Count' FROM wiki GROUP BY page ORDER BY page DESC, `Count` ASC")
      ).to.throw('plywood does not currently support multi-column ORDER BYs')

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
        CUSTOM('blah') AS 'Custom1'
        FROM `wiki`
        WHERE `language`="en"  ;  -- This is just some comment
        """)

      ex2 = ply()
        .apply('data', '$wiki.filter($language == "en")')
        .apply('Count1', '$data.count()')
        .apply('Count2', '$data.count()')
        .apply('Count3', '$data.count()')
        .apply('Count4', '$data.count()')
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
        .apply('Custom1', $('data').custom('blah'))

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS())

    it "should parse a total expression without group by clause", ->
      parse = Expression.parseSQL("""
        SELECT
        SUM(added) AS TotalAdded
        FROM `wiki`
        WHERE `language`="en"    -- This is just some comment
        """)

      ex2 = ply()
        .apply('data', '$wiki.filter($language == "en")')
        .apply('TotalAdded', '$data.sum($added)')

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

    it "should work with a NUMBER_BUCKET function", ->
      parse = Expression.parseSQL("""
        SELECT
        NUMBER_BUCKET(added, 10, 1) AS 'AddedBucket',
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
        TIME_PART(`time`, DAY_OF_WEEK, 'Etc/UTC') AS 'DayOfWeek',
        SUM(added) AS 'TotalAdded'
        FROM `wiki`
        GROUP BY 1
        """)

      ex2 = $('wiki').split($('time').timePart('DAY_OF_WEEK', 'Etc/UTC'), 'DayOfWeek', 'data')
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
          page LIKE 'World'
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
