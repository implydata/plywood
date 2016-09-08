/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2016 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var { expect } = require("chai");
var { sane } = require('../utils');

var { Timezone } = require('chronoshift');

var plywood = require('../../build/plywood');
var { Expression, i$, $, ply, r, Set, Dataset, External, ExternalExpression } = plywood;

function resolvesProperly(parse) {
  var resolveString = parse.expression.resolve({ t: 'STR' });
  expect(resolveString.expression.type).to.deep.equal("STRING");
  var resolveTime = parse.expression.resolve({ t: new Date() });
  expect(resolveTime.expression.type).to.deep.equal("TIME");
}

describe("SQL parser", () => {
  describe("basic expression", () => {
    var $data = $('data');

    it("works with a literal expression", () => {
      var parse = Expression.parseSQL(" 1 ");
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

    it("works with an empty set", () => {
      var parse = Expression.parseSQL("{}");
      var ex2 = r(Set.fromJS([]));

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a basic set", () => {
      var parse = Expression.parseSQL("{'a', 'b'}");
      var ex2 = r(Set.fromJS(['a', 'b']));

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a set containing null", () => {
      var parse = Expression.parseSQL("{'a', 'b', NULL}");
      var ex2 = r(Set.fromJS(['a', 'b', null]));

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a set that is only null", () => {
      var parse = Expression.parseSQL("{NULL}");
      var ex2 = r(Set.fromJS([null]));

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a COUNT expression", () => {
      var parse = Expression.parseSQL("COUNT()");

      var ex2 = $('data').count();

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a filtered COUNT expression", () => {
      var parse = Expression.parseSQL("COUNT(* WHERE cityName = 'San Francisco')");

      var ex2 = $('data').filter("i$cityName == 'San Francisco'").count();

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a LENGTH expression", () => {
      var parse = Expression.parseSQL("LENGTH(`page`)");
      var parse2 = Expression.parseSQL("LEN(`page`)");
      var parse3 = Expression.parseSQL("CHAR_LENGTH(`page`)");

      var ex2 = i$('page').length();

      expect(parse.verb).to.equal(null);
      expect(parse2.verb).to.equal(null);
      expect(parse3.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
      expect(parse2.expression.toJS()).to.deep.equal(ex2.toJS());
      expect(parse3.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a YEAR expression", () => {
      var parse = Expression.parseSQL("YEAR(time)");

      var ex2 = i$('time').timePart('YEAR');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with NULL arguments", () => {
      var parse = Expression.parseSQL("CONCAT(NULL, 'lol')");

      var ex2 = r(null).concat('"lol"');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a filtered SUM 1 expression", () => {
      var parse = Expression.parseSQL("SUM(1 WHERE cityName = 'San Francisco')");

      var ex2 = $('data').filter("i$cityName == 'San Francisco'").sum(1);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a filtered SUM expression", () => {
      var parse = Expression.parseSQL("SUM(added WHERE cityName = 'San Francisco')");

      var ex2 = $('data').filter("i$cityName == 'San Francisco'").sum('i$added');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a filtered QUANTILE expression", () => {
      var parse = Expression.parseSQL("QUANTILE(added WHERE cityName = 'San Francisco', 0.99)");

      var ex2 = $('data').filter("i$cityName == 'San Francisco'").quantile('i$added', 0.99);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a trivial arithmetic expression", () => {
      var parse = Expression.parseSQL("(1 + 7) + 5 / 2");

      var ex2 = Expression.parse('(1 + 7) + 5 / 2');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with an arithmetic expression", () => {
      var parse = Expression.parseSQL("SUM(`added`) + 5 + SUM(deleted) / 2");

      var ex2 = $data.sum('i$added').add(5, '$data.sum(i$deleted) / 2');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with IN expression (value list)", () => {
      var parse = Expression.parseSQL("language IN ( 'ca', 'cs', 'da', 'el' )");

      var ex2 = i$('language').in(['ca', 'cs', 'da', 'el']);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with IN expression (list literal)", () => {
      var parse = Expression.parseSQL("language IN { 'ca', 'cs', 'da', 'el' }");

      var ex2 = i$('language').in(['ca', 'cs', 'da', 'el']);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with IN expression (variable)", () => {
      var parse = Expression.parseSQL("language IN languages");

      var ex2 = i$('language').in('i$languages');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should handle --", () => {
      var parse = Expression.parseSQL("x--3");

      var ex2 = i$('x').subtract(-3);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should handle fallback --", () => {
      var parse = Expression.parseSQL("IFNULL(null,'fallback')");
      var parse2 = Expression.parseSQL("IFNULL(null, SUM(deleted))");
      var parse3 = Expression.parseSQL("IFNULL(SUM(`added`), SUM(deleted))");
      var parse4 = Expression.parseSQL("FALLBACK(SUM(`added`), SUM(deleted))");

      var ex = r(null).fallback('fallback');
      var ex2 = r(null).fallback('$data.sum(i$deleted)');
      var ex3 = $data.sum('i$added').fallback('$data.sum(i$deleted)');

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

      var ex2 = $('data').sum('i$added').add(1000).divide(2);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    describe("date literals - parse and resolve", () => {
      it('works with inferred literals', () => {
        var tests = sane`
          '2015-01-01T00:00:00.000' <= t AND t < '2016-01-01T00:00:00.000'
          '2015-01-01T00:00:00.00' <= t AND t < '2016-01-01T00:00:00.00'
          '2015-01-01T00:00:00.0' <= t AND t < '2016-01-01T00:00:00.0'
          '2015-01-01T000000.0' <= t AND t < '2016-01-01T000000.0'
          '2015-01-01T00:00:00' <= t AND t < '2016-01-01T00:00:00'
          '2015-01-01T000000' <= t AND t < '2016-01-01T000000'
          '2015-01-01T00:00' <= t AND t < '2016-01-01T00:00'
          '2015-01-01T0000' <= t AND t < '2016-01-01T0000'
          '2015-01-01T00' <= t AND t < '2016-01-01T00'
          '2015-01-01' <= t AND t < '2016-01-01'
          '20150101' <= t AND t < '20160101'
          '2015-01' <= t AND t < '2016-01'
          '2015' <= t AND t < '2016'
        `;

        tests.split('\n').forEach(test => {
          var parse = Expression.parseSQL(test);
          var left = test.substring(1, test.indexOf("' <="));
          var right = test.substring(test.indexOf('< ') + 3, test.length -1);
          var ex = r(left).lessThanOrEqual('i$t').and(i$('t').lessThan(r(right)));
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
          resolvesProperly(parse)
        });
      });

      it('works with a custom Timezone in inferred literals', () => {
        var tests = sane`
          '2015-01-01T00:00:00.000' <= t AND t < '2016-01-01T00:00:00.000'
          '2015-01-01T00:00:00.00' <= t AND t < '2016-01-01T00:00:00.00'
          '2015-01-01T00:00:00.0' <= t AND t < '2016-01-01T00:00:00.0'
          '2015-01-01T000000.0' <= t AND t < '2016-01-01T000000.0'
          '2015-01-01T00:00:00' <= t AND t < '2016-01-01T00:00:00'
          '2015-01-01T000000' <= t AND t < '2016-01-01T000000'
          '2015-01-01T00:00' <= t AND t < '2016-01-01T00:00'
          '2015-01-01T0000' <= t AND t < '2016-01-01T0000'
          '2015-01-01T00' <= t AND t < '2016-01-01T00'
          '2015-01-01' <= t AND t < '2016-01-01'
          '20150101' <= t AND t < '20160101'
          '2015-01' <= t AND t < '2016-01'
          '2015' <= t AND t < '2016'
        `;

        tests.split('\n').forEach(test => {
          var parse = Expression.parseSQL(test, Timezone.fromJS('America/New_York'));
          var left = test.substring(1, test.indexOf("' <="));
          var right = test.substring(test.indexOf('< ') + 3, test.length -1);
          var ex = r(left).lessThanOrEqual('i$t').and(i$('t').lessThan(r(right)));
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
          resolvesProperly(parse)
        });
      });

      it('works with DATE', () => {
        var tests = sane`
          DATE "2016-02-09Z"
          DATE "2016-02-09T01:01:01Z"
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

      it('works with DATE()', () => {
        var tests = sane`
          DATE("2016-02-09")
          DATE("2016-02-09 00:00:00")
        `;

        var ex = r(new Date('2016-02-09Z')).timeFloor('P1D');
        tests.split('\n').forEach(test => {
          var parse = Expression.parseSQL(test);
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
        });
      });

      it('works with TIMESTAMP', () => {
        var tests = sane`
          TIMESTAMP("20160209010203")
          TIMESTAMP "20160209010203"
          TIMESTAMP "160209010203"
          TIMESTAMP "2016-02-09T01:02:03Z"
          TIMESTAMP "2016-02-09T03:02:03+02:00"
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
          TIMESTAMP "2016-02-09T01:02:03.456Z"
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
          Expression.parseSQL("TIME('01:02:03')");
        }).to.throw('time literals are not supported');

        expect(() => {
          Expression.parseSQL("{t '01:02:03'}");
        }).to.throw('time literals are not supported');

        expect(() => {
          Expression.parseSQL("TIME '01:02:03'");
        }).to.throw('time literals are not supported');
      });

      it('works inside BETWEEN of timestamp', () => {
        var tests = sane`
          t BETWEEN TIMESTAMP '2015-09-12T10:30:00' AND TIMESTAMP '2015-09-12T12:30:00'
        `;

        var ex = i$('t').greaterThan(new Date('2015-09-12T10:30:00Z')).and(i$('t').lessThan(new Date('2015-09-12T12:30:00Z')));
        tests.split('\n').forEach(test => {
          var parse = Expression.parseSQL(test);
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
        });
      });

      it('works inside BETWEEN of literals', () => {
        var tests = sane`
          t BETWEEN '2015-09-12T10:30:00' AND '2015-09-12T12:30:00'
          t BETWEEN '2015-09-12T10:30' AND '2015-09-12T12:30'
        `;

        var exs = [
          i$('t').greaterThan(r('2015-09-12T10:30:00')).and(i$('t').lessThan(r('2015-09-12T12:30:00'))),
          i$('t').greaterThan(r('2015-09-12T10:30')).and(i$('t').lessThan(r('2015-09-12T12:30')))
        ];

        tests.split('\n').forEach((test, i) => {
          var parse = Expression.parseSQL(test);
          expect(parse.expression.toJS()).to.deep.equal(exs[i].toJS());
          resolvesProperly(parse);
        });
      });

      it('works inside BETWEEN of years', () => {
        var tests = sane`
          t BETWEEN '2015-01-01T00:00:00+00:00' AND '2016-01-01T00:00:00+00:00'
          t BETWEEN '2015-01-01T00:00:00-00:00' AND '2016-01-01T00:00:00-00:00'
          t BETWEEN '2015-01-01T00:00:00Z' AND '2016-01-01T00:00:00Z'
          t BETWEEN '2015-01-01T00:00:00.000' AND '2016-01-01T00:00:00.000'
          t BETWEEN '2015-01-01T00:00:00' AND '2016-01-01T00:00:00'
          t BETWEEN '2015-01-01T000000' AND '2016-01-01T000000'
          t BETWEEN '2015-01-01T00:00' AND '2016-01-01T00:00'
          t BETWEEN '2015-01-01T0000' AND '2016-01-01T0000'
          t BETWEEN '2015-01-01T00' AND '2016-01-01T00'
          t BETWEEN '2015-01-01 00' AND '2016-01-01 00'
          t BETWEEN '2015-01-01' AND '2016-01-01'
          t BETWEEN '2015-0101' AND '2016-0101'
          t BETWEEN '201501' AND '201601'
          t BETWEEN '2015' AND '2016'
        `;

        tests.split('\n').forEach((test, i) => {
          var parse = Expression.parseSQL(test);
          var left = /(?:BETWEEN ')([0-9-+:TZ.\s]+)/g.exec(test)[1];
          var right = /(?:AND ')([0-9-+:TZ.\s]+)/g.exec(test)[1];
          var ex = i$('t').greaterThan(r(left)).and(i$('t').lessThan(r(right)));
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
          resolvesProperly(parse);
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


  describe("SELECT", () => {
    it("should fail on a expression with no columns", () => {
      expect(() => {
        Expression.parseSQL("SELECT FROM wiki");
      }).to.throw("SQL parse error: Can not have empty column list on 'SELECT FROM wiki'");
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
      }).to.throw('COUNT DISTINCT must have an expression');
    });

    it("should fail gracefully on COUNT(DISTINCT *)", () => {
      expect(() => {
        Expression.parseSQL("COUNT(DISTINCT *)");
      }).to.throw('COUNT DISTINCT can not be used with *');
    });

    it("should fail gracefully on SUM(DISTINCT blah)", () => {
      expect(() => {
        Expression.parseSQL("SUM(DISTINCT blah)");
      }).to.throw('can not use DISTINCT for sum aggregator');
    });

    it("should have a good error for unterminated single quote strings", () => {
      expect(() => {
        Expression.parseSQL("SELECT page, COUNT() AS 'Count FROM wiki");
      }).to.throw('Unmatched single quote on');
    });

    it("should have a good error for unterminated double quote strings", () => {
      expect(() => {
        Expression.parseSQL("SELECT page, COUNT() AS \"Count FROM wiki");
      }).to.throw('Unmatched double quote on');
    });

    it("should parse a trivial expression", () => {
      var parse = Expression.parseSQL(sane`
        SELECT 1+1 AS Two, 1+3 AS Three;
      `);

      var ex2 = ply()
        .apply('Two', '1+1')
        .apply('Three', '1+3');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.table).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
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

    it("should parse a simple expression 2", () => {
      var parse = Expression.parseSQL(sane`
        SELECT  COUNT(page)
      `);

      var ex2 = ply()
        .apply('data', '$data')
        .apply('COUNT(page)', '$data.filter(i$page.isnt(null)).count()');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.table).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a relaxed table name", () => {
      var parse = Expression.parseSQL(sane`
        SELECT * FROM my-table*is:the/best_table;
      `);

      var ex2 = $('my-table*is:the/best_table');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.table).to.equal('my-table*is:the/best_table');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a table alias", () => {
      var parse1 = Expression.parseSQL('SELECT * FROM my-table AS t;');
      var parse2 = Expression.parseSQL('SELECT * FROM my-table t;');

      var ex2 = $('my-table');

      expect(parse1.verb).to.equal('SELECT');
      expect(parse1.table).to.equal('my-table');
      expect(parse1.expression.toJS()).to.deep.equal(ex2.toJS());
      expect(parse2.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a total expression with all sorts of applies", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
        a = b AS aISb1,
        a IS b AS aISb2,
        a <=> b AS aISb3,
        COUNT() Count1,
        COUNT(*) AS Count2,
        COUNT(1) AS Count3,
        COUNT(\`visitor\`) AS Count4,
        MATCH(\`visitor\`, "[0-9A-F]") AS 'Match',
        CUSTOM_TRANSFORM(\`visitor\`, "visitor_custom") AS 'CustomTransform',
        SUM(added) AS 'TotalAdded',
        DATE '2014-01-02' AS 'Date',
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
        COUNT( DISTINCT visitor) AS 'Unique2',
        COUNT(DISTINCT(visitor)) AS 'Unique3',
        TIME_BUCKET(time, PT1H) AS 'TimeBucket',
        TIME_FLOOR(time, PT1H) AS 'TimeFloor',
        TIME_SHIFT(time, PT1H, 3) AS 'TimeShift3',
        TIME_RANGE(time, PT1H, 3) AS 'TimeRange3',
        OVERLAP(x, y) AS 'Overlap',
        CUSTOM('blah') AS 'Custom1',
        CUSTOM_AGGREGATE('blah') AS 'Custom2'
        FROM \`wiki\`
        WHERE \`language\`="en"  ;  -- This is just some comment
      `);

      var ex2 = ply()
        .apply('data', '$wiki.filter(i$language == "en")')
        .apply('aISb1', 'i$a.is(i$b)')
        .apply('aISb2', 'i$a.is(i$b)')
        .apply('aISb3', 'i$a.is(i$b)')
        .apply('Count1', '$data.count()')
        .apply('Count2', '$data.count()')
        .apply('Count3', '$data.filter(1 != null).count()')
        .apply('Count4', '$data.filter(i$visitor != null).count()')
        .apply('Match', i$('visitor').match("[0-9A-F]"))
        .apply('CustomTransform', i$('visitor').customTransform("visitor_custom"))
        .apply('TotalAdded', '$data.sum(i$added)')
        .apply('Date', new Date('2014-01-02T00:00:00.000Z'))
        .apply('TotalAddedOver4', '$data.sum(i$added) / 4')
        .apply('False', r(true).not())
        .apply('MinusAdded', '-$data.sum(i$added)')
        .apply('AbsAdded', 'i$MinusAdded.absolute()')
        .apply('AbsoluteAdded', 'i$MinusAdded.absolute()')
        .apply('SqRtAdded', 'i$MinusAdded.power(0.5)')
        .apply('SqRtAdded2', 'i$MinusAdded.power(0.5)')
        .apply('SquareRoot', 'i$TotalAddedOver4.power(0.5)')
        .apply('One', r(Math.E).power(0))
        .apply('SimplyAdded', '$data.sum(i$added)')
        .apply('Median', $('data').quantile('i$added', 0.5))
        .apply('Unique1', $('data').countDistinct('i$visitor'))
        .apply('Unique2', $('data').countDistinct('i$visitor'))
        .apply('Unique3', $('data').countDistinct('i$visitor'))
        .apply('TimeBucket', i$('time').timeBucket('PT1H'))
        .apply('TimeFloor', i$('time').timeFloor('PT1H'))
        .apply('TimeShift3', i$('time').timeShift('PT1H', 3))
        .apply('TimeRange3', i$('time').timeRange('PT1H', 3))
        .apply('Overlap', i$('x').overlap('i$y'))
        .apply('Custom1', $('data').customAggregate('blah'))
        .apply('Custom2', $('data').customAggregate('blah'))
        .select("aISb1", "aISb2", "aISb3", "Count1", "Count2", "Count3", "Count4", "Match", "CustomTransform",
          "TotalAdded", "Date", "TotalAddedOver4", "False", "MinusAdded", "AbsAdded", "AbsoluteAdded", "SqRtAdded",
          "SqRtAdded2", "SquareRoot", "One", "SimplyAdded", "Median", "Unique1", "Unique2", "Unique3",
          "TimeBucket", "TimeFloor", "TimeShift3", "TimeRange3", "Overlap", "Custom1", "Custom2");

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
        .apply('data', '$wiki.filter(i$language == "en")')
        .apply('TotalAdded', '$data.sum(i$added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse ISNULL", () => {
      var parse = Expression.parseSQL(sane`
        SELECT * FROM wikipedia WHERE NOT ISNULL(cityName) LIMIT 10
      `);

      var ex2 = $('wikipedia')
        .filter('i$cityName.is(null).not()')
        .limit(10);

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
        .apply('data', '$wiki.filter(i$language == "en")')
        .apply('TotalAdded', '$data.sum(i$added) - -1');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work without a FROM", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
        SUM(added) AS 'TotalAdded'
        WHERE \`language\`="en"
      `);

      var ex2 = ply()
        .apply('data', '$data.filter(i$language == "en")')
        .apply('TotalAdded', '$data.sum(i$added)');

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
          i$('language').is("en")
            .and(i$('time').greaterThan(r('2015-01-01T10:30:00'))
              .and(i$('time').lessThan(r('2015-01-02T12:30:00')))
            )
        ))
        .apply('TotalAdded', '$data.sum(i$added)');

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
          i$('language').is("en")
            .and(r('2015-01-01T10:30:00').lessThanOrEqual(i$('time')))
            .and(i$('time').lessThan(r('2015-01-02T12:30:00')))
        ))
        .apply('TotalAdded', '$data.sum(i$added)');

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
        i$('language').is("en")
          .and(i$('time').greaterThan(r('2015-01-01T10:30:00'))
            .and(i$('time').lessThan(r('2015-01-02T12:30:00')))
          )
      ).split('i$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'TotalAdded');

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

      var ex2 = $('wiki').split('i$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'TotalAdded')
        .sort('i$TotalAdded', 'ascending')
        .limit(5);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with fancy limit LIMIT 0,x", () => {
      var parse = Expression.parseSQL(sane`
        SELECT SUM(added) AS 'TotalAdded' FROM \`wiki\` LIMIT 0, 5
      `);

      var ex2 = ply()
        .apply('data', '$wiki')
        .apply('TotalAdded', '$data.sum(i$added)')
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

      var ex2 = $('wiki').split('i$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'TotalAdded')
        .limit(5);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with multi-dimensional GROUP BYs", () => {
      var parse = Expression.parseSQL(sane`
        SELECT \`page\`, \`user\` FROM \`wiki\` GROUP BY \`page\`, \`user\`
      `);

      var ex2 = $('wiki').split({ page: 'i$page', user: 'i$user' }, 'data').select('page', 'user');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SELECT DISTINCT", () => {
      var parse = Expression.parseSQL(sane`
        SELECT DISTINCT \`page\`, \`user\` FROM \`wiki\`
      `);

      var ex2 = $('wiki').split({ page: 'i$page', user: 'i$user' }, 'data').select('page', 'user');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with few spaces", () => {
      var parse = Expression.parseSQL(sane`
        SELECT\`page\`AS'Page'FROM\`wiki\`GROUP BY\`page\`ORDER BY\`Page\`LIMIT 5
      `);

      var ex2 = $('wiki').split('i$page', 'Page', 'data')
        .sort('i$Page', 'ascending')
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

      var ex2 = $('wiki').split(i$('time').timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('TimeByHour', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a DATE_FORMAT function (PT1S)", () => {
      var parse = Expression.parseSQL("DATE_FORMAT(`wikipedia`.`time`, '%Y-%m-%d %H:%i:%s' )");
      var ex2 = i$('time').timeFloor('PT1S');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a DATE_FORMAT function (PT1M)", () => {
      var parse = Expression.parseSQL("DATE_FORMAT(`wikipedia`.`time`, '%Y-%m-%d %H:%i:00' )");
      var ex2 = i$('time').timeFloor('PT1M');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a DATE_FORMAT function (PTD)", () => {
      var parse = Expression.parseSQL("DATE_FORMAT(`wikipedia`.`time`, '%Y-%m-%d' )");
      var ex2 = i$('time').timeFloor('P1D');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a DATE_FORMAT function (PTD)", () => {
      var parse = Expression.parseSQL("DATE_FORMAT(`wikipedia`.`time`, '%Y-%m-%d' )");
      var ex2 = i$('time').timeFloor('P1D');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a ADDDATE(DATE_FORMAT) function (PTD)", () => {
      var parse = Expression.parseSQL("ADDDATE(DATE_FORMAT(`wikipedia`.`time`, '%Y-%m-%d' ), INTERVAL 0 SECOND )");
      var ex2 = i$('time').timeFloor('P1D');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("throws on non zero interval", () => {
      expect(() => {
        Expression.parseSQL("ADDDATE(DATE_FORMAT(`wikipedia`.`time`, '%Y-%m-%d' ), INTERVAL 1 SECOND )");
      }).to.throw('only zero intervals supported for now');
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
        .filter('i$language == "en"');

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
        .filter('i$language == en')
        .apply('page', 'i$page')
        .apply('IsNew', 'i$isNew')
        .apply('Added', 'i$added')
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
        .filter('i$language == "en"')
        .apply('Page', 'i$page.concat(lol)')
        .apply('added', 'i$added + 1')
        .select('Page', 'added')
        .filter('i$added > 100')
        .sort('i$page', 'descending')
        .limit(10);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should add a select to the end of group by queries ()", () => {
      var parse = Expression.parseSQL(sane`
          SELECT isRobot as "isRobot", isNew as "isNew", COUNT() AS "Count" from data group by isNew, isRobot, page limit 2
      `);

      var ex2 = $('data')
        .split({ 'isNew': 'i$isNew', 'isRobot': 'i$isRobot', 'split2' : 'i$page' })
        .apply('Count', $('data').count())
        .select('isRobot', 'isNew', 'Count')
        .limit(2);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });


    it("should work a select to the end of group by queries", () => {
      var parse = Expression.parseSQL(sane`
          SELECT COUNT() AS "Count", isRobot as "isRobot", page as "Page", isNew as "isNew" from data group by isNew, isRobot, page limit 2
      `);

      var ex2 = $('data')
        .split({ 'isNew': 'i$isNew', 'isRobot': 'i$isRobot', 'Page' : 'i$page' })
        .apply('Count', $('data').count())
        .select('Count', 'isRobot', 'Page', 'isNew')
        .limit(2);

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

      var ex2 = $('wiki').split(i$('added').numberBucket(10, 1), 'AddedBucket', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('AddedBucket', 'TotalAdded')

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

      var ex2 = $('wiki').split(i$('time').timePart('DAY_OF_WEEK', 'Etc/UTC'), 'DayOfWeek', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('DayOfWeek', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with time casting function", () => {
      var parse = Expression.parseSQL(sane`
        SELECT UNIX_TIMESTAMP(\`time\`) as Unix FROM \`wiki\`
        WHERE \`time\` BETWEEN FROM_UNIXTIME(1447430881) AND FROM_UNIXTIME(1547430881)
      `);

      var ex2 = $('wiki')
        .filter(i$('time').greaterThan(r(1447430881).multiply(1000).cast('TIME')).and(i$('time').lessThan(r(1547430881).multiply(1000).cast('TIME'))))
        .apply('Unix', i$('time').cast('NUMBER').divide(1000))
        .select('Unix');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work number casting function", () => {
      var parse = Expression.parseSQL(sane`
        SELECT * FROM \`wiki\`
        WHERE FROM_UNIXTIME(1447430881) < \`time\` AND \`time\` < FROM_UNIXTIME(1547430881)
      `);

      var ex2 = $('wiki').filter(
        r(1447430881).multiply(1000).cast('TIME').lessThan(i$('time')).and(i$('time').lessThan(r(1547430881).multiply(1000).cast('TIME')))
      );

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with number cast and string cast function", () => {
      var parse = Expression.parseSQL(sane`
        SELECT CAST(\`commentLength\` AS CHAR) as castedString, CAST(\`commentLengthStr\` AS SIGNED) as castedNumber FROM \`wiki\`
      `);

      var ex2 = $('wiki')
        .apply('castedString', i$('commentLength').cast('STRING'))
        .apply('castedNumber', i$('commentLengthStr').cast('NUMBER'))
        .select('castedString', 'castedNumber');

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

      var ex2 = $('wiki').split("'[' ++ i$page.substr(0, 3).substr(1, 2) ++ ']'", 'Crazy', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Crazy', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a LOWER and UPPER function", () => {
      var parse = Expression.parseSQL(sane`
        SELECT 
          CONCAT(UPPER(SUBSTRING(\`user\`, 1, 1)), LCASE(SUBSTRING(\`user\`, 2, 5))) AS 'ProperName',
          UPPER(\`page\`) AS 'Upper',
          UCASE(\`channel\`) AS 'UCase',
          SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      var ex2 = $('wiki').split("(i$user.substr(1, 1).transformCase('upperCase')).concat(i$user.substr(2,5).transformCase('lowerCase'))", 'ProperName', 'data')
        .apply('Upper', 'i$page.transformCase("upperCase")')
        .apply('UCase', 'i$channel.transformCase("upperCase")')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('ProperName', 'Upper', 'UCase', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a LOCATE function greater than 0", () => {
      var parse = Expression.parseSQL(sane`
        SELECT \`page\` AS 'Page',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki\` WHERE LOCATE(\`page\`, 'title') > 0
        GROUP BY 1
      `);

      var ex2 = $('wiki').filter(i$('page').indexOf('title').add(1).in({start: 0, end: null, bounds: '()'})).split("i$page", 'Page', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'TotalAdded');

      expect(parse.expression.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with EXTRACT function", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
          EXTRACT(\`page\`, '^Wiki(.+)$') AS 'Extract',
          SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      var ex2 = $('wiki').split("i$page.extract('^Wiki(.+)$')", 'Extract', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Extract', 'TotalAdded');

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

      var ex2 = $('wiki').split("i$language.lookup('language-lookup')", 'Lookup', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Lookup', 'TotalAdded');

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
          \`wiki\`.\`language\` IN {'ca', 'cs', 'da', 'el'} AND
          \`wiki\`.\`language\` IN languages AND
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
            i$('user').is(r("Bot Master 1"))
              .and(i$('page').isnt(r("Hello World")))
              .and(i$('added').lessThan(5))
              .and(i$('language').in(['ca', 'cs', 'da', 'el']))
              .and(i$('language').in(['ca', 'cs', 'da', 'el']))
              .and(i$('language').in('i$languages'))
              .and(i$('namespace').in(['simple', 'dict']).not())
              .and(i$('geo').isnt(null))
              .and(i$('page').contains('World', 'ignoreCase'))
              .and(i$('page').match('^.*Hello_World.*$'))
              .and(i$('page').match('^.*Hello_World.*$'))
              .and(i$('page').match('W[od]'))
          )
        )
        .apply('TotalAdded', '$data.sum(i$added)');

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
        .apply('data', '$wiki.filter(i$language == "en")')
        .apply('TotalAdded', '$data.sum(i$added)')
        .apply(
          'Pages',
          $('data').split('i$page', 'Page')
            .apply('Count', '$data.count()')
            .apply('TotalAdded', '$data.sum(i$added)')
            .apply('MinAdded', '$data.min(i$added)')
            .apply('MaxAdded', '$data.max(i$added)')
            .select('Page', 'Count', 'TotalAdded', 'MinAdded', 'MaxAdded')
            .filter('i$TotalAdded > 100')
            .sort('i$Count', 'descending')
            .limit(10)
        )
        .select('TotalAdded', 'Pages');

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
        i$('language').is("en")
            .and(i$('time').greaterThan(r('2015-01-01T10:30:00'))
              .and(i$('time').lessThan(r('2015-01-02T12:30:00')))
            )
      ).split('i${page or else?}', 'Page', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with FROM (sub query)", () => {
      var parse = Expression.parseSQL(sane`
        SELECT * FROM (
          SELECT
            \`page\` AS 'Page',
            SUM(added) AS 'TotalAdded'
          FROM \`wiki\`
          WHERE \`language\`="en"
          GROUP BY \`page\`
        )
      `);

      var ex2 = $('wiki')
        .filter(i$('language').is("en"))
        .split('i$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with FROM (sub query) + alias", () => {
      var parse = Expression.parseSQL(sane`
        SELECT * FROM (
          SELECT
            \`page\` AS 'Page',
            SUM(added) AS 'TotalAdded'
          FROM \`wiki\`
          WHERE \`language\`="en"
          GROUP BY \`page\`
        ) AS T
      `);

      var ex2 = $('wiki')
        .filter(i$('language').is("en"))
        .split('i$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with FROM (sub query) complex in total", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
          SUM(\`TotalAdded\`) AS 'SumTotalAdded'
        FROM (
          SELECT
            \`page\` AS 'Page',
            SUM(added) AS 'TotalAdded'
          FROM \`wiki\`
          WHERE \`language\`="en"
          GROUP BY \`page\`
        ) AS T
      `);

      var ex2 = ply()
        .apply(
          'data',
          $('wiki')
            .filter(i$('language').is("en"))
            .split('i$page', 'Page', 'data')
            .apply('TotalAdded', '$data.sum(i$added)')
            .select('Page', 'TotalAdded')
        )
        .apply('SumTotalAdded', '$data.sum(i$TotalAdded)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with FROM (sub query) complex in split", () => {
      var parse = Expression.parseSQL(sane`
        SELECT
          \`Page\` AS 'Page',
          SUM(\`TotalAdded\`) AS 'SumTotalAdded'
        FROM (
          SELECT
            \`page\` AS 'Page',
            \`user\` AS 'User',
            SUM(added) AS 'TotalAdded'
          FROM \`wiki\`
          WHERE \`language\`="en"
          GROUP BY 1, 2
        )
        WHERE TotalAdded != 5
        GROUP BY \`Page\`
        LIMIT 5
      `);

      var ex2 = $('wiki')
        .filter(i$('language').is("en"))
        .split({ Page: 'i$page', User: 'i$user' }, 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'User', 'TotalAdded')
        .filter(i$('TotalAdded').isnt(5))
        .split('i$Page', 'Page', 'data')
          .apply('SumTotalAdded', '$data.sum(i$TotalAdded)')
          .select('Page', 'SumTotalAdded')
          .limit(5);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

  });


  describe("SELECT (Tableau specific)", () => {
    it("should work with this filter query", () => {
      var parse = Expression.parseSQL(sane`
        SELECT SUM(\`wikipedia\`.\`added\`) AS \`sum_added_ok\`,
          ADDDATE( DATE_FORMAT( \`wikipedia\`.\`time\`, '%Y-%m-%d %H:%i:%s' ), INTERVAL 0 SECOND ) AS \`tsc_time_ok\`
        FROM \`wikipedia\`
        WHERE ((ADDDATE( DATE_FORMAT( \`wikipedia\`.\`time\`, '%Y-%m-%d %H:00:00' ), INTERVAL 0 SECOND ) >= TIMESTAMP('2015-09-12 00:00:00'))
          AND (ADDDATE( DATE_FORMAT( \`wikipedia\`.\`time\`, '%Y-%m-%d %H:00:00' ), INTERVAL 0 SECOND ) <= TIMESTAMP('2015-09-12 23:00:00')))
        GROUP BY 2
      `);

      var ex2 = $('wikipedia').filter("i$time.timeFloor(PT1H) >= '2015-09-12 00:00:00' and i$time.timeFloor(PT1H) <= '2015-09-12 23:00:00'")
        .split('i$time.timeFloor(PT1S)', 'tsc_time_ok', 'data')
        .apply('sum_added_ok', '$data.sum(i$added)')
        .select("sum_added_ok", "tsc_time_ok");

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

  });


  describe("SHOW", () => {
    it("should work with SHOW VARIABLES like", () => {
      var parse = Expression.parseSQL(sane`
        SHOW VARIABLES like 'collation_server'
      `);

      var ex2 = i$('GLOBAL_VARIABLES')
        .filter(i$('VARIABLE_NAME').match('^collation.server$'))
        .apply('Variable_name', i$('VARIABLE_NAME'))
        .apply('Value', i$('VARIABLE_VALUE'))
        .select('Variable_name', 'Value');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SHOW VARIABLES WHERE", () => {
      var parse = Expression.parseSQL(sane`
        SHOW SESSION VARIABLES WHERE Variable_name ='language' OR Variable_name = 'net_write_timeout'
      `);

      var ex2 = i$('GLOBAL_VARIABLES')
        .filter(i$('Variable_name').is(r('language')).or(i$('Variable_name').is(r('net_write_timeout'))))
        .apply('Variable_name', i$('VARIABLE_NAME'))
        .apply('Value', i$('VARIABLE_VALUE'))
        .select('Variable_name', 'Value');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SHOW DATABASES", () => {
      var parse = Expression.parseSQL(sane`
        SHOW DATABASES
      `);

      var ex2 = i$('SCHEMATA')
        .apply('Database', i$('SCHEMA_NAME'))
        .select('Database');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SHOW SCHEMAS", () => {
      var parse = Expression.parseSQL(sane`
        SHOW SCHEMAS LIKE "Vannie%"
      `);

      var ex2 = i$('SCHEMATA')
        .filter(i$('SCHEMA_NAME').match('^Vannie.*$'))
        .apply('Database', i$('SCHEMA_NAME'))
        .select('Database');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SHOW TABLES", () => {
      var parse = Expression.parseSQL(sane`
        SHOW TABLES IN my_db
      `);

      var ex2 = i$('TABLES')
        .filter(i$('TABLE_SCHEMA').is('my_db'))
        .apply('Tables_in_database', i$('TABLE_NAME'))
        .select('Tables_in_database');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SHOW FULL TABLES", () => {
      var parse = Expression.parseSQL(sane`
        SHOW FULL TABLES FROM \`my_db\` LIKE '%'
      `);

      var ex2 = i$('TABLES')
        .filter(i$('TABLE_SCHEMA').is('my_db'))
        .filter(i$('TABLE_NAME').match('^.*$'))
        .apply('Tables_in_database', i$('TABLE_NAME'))
        .apply('Table_type', i$('TABLE_TYPE'))
        .select('Tables_in_database', 'Table_type');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SHOW COLUMNS", () => {
      var parse = Expression.parseSQL(sane`
        SHOW COLUMNS IN my_table IN my_db
      `);

      var ex2 = i$('COLUMNS')
        .filter(i$('TABLE_NAME').is('my_table'))
        .filter(i$('TABLE_SCHEMA').is('my_db'))
        .apply('Field', i$('COLUMN_NAME'))
        .apply('Type', i$('COLUMN_TYPE'))
        .apply('Null', i$('IS_NULLABLE'))
        .apply('Key', i$('COLUMN_KEY'))
        .apply('Default', i$('COLUMN_DEFAULT'))
        .apply('Extra', i$('EXTRA'))
        .select('Field', 'Type', 'Null', 'Key', 'Default', 'Extra');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SHOW FULL COLUMNS", () => {
      var parse = Expression.parseSQL(sane`
        SHOW FULL COLUMNS IN my_table IN my_db LIKE "T%"
      `);

      var ex2 = i$('COLUMNS')
        .filter(i$('TABLE_NAME').is('my_table'))
        .filter(i$('TABLE_SCHEMA').is('my_db'))
        .filter(i$('COLUMN_NAME').match('^T.*$'))
        .apply('Field', i$('COLUMN_NAME'))
        .apply('Type', i$('COLUMN_TYPE'))
        .apply('Null', i$('IS_NULLABLE'))
        .apply('Key', i$('COLUMN_KEY'))
        .apply('Default', i$('COLUMN_DEFAULT'))
        .apply('Extra', i$('EXTRA'))
        .apply('Collation', i$('COLLATION_NAME'))
        .apply('Privileges', i$('PRIVILEGES'))
        .apply('Comment', i$('COLUMN_COMMENT'))
        .select('Field', 'Type', 'Null', 'Key', 'Default', 'Extra', 'Collation', 'Privileges', 'Comment');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

  });


  describe("DESCRIBE", () => {
    it("works with DESCRIBE query", () => {
      var parse = Expression.parseSQL("DESCRIBE wikipedia");

      var ex2 = i$('COLUMNS')
        .filter(i$('TABLE_NAME').is('wikipedia'))
        //.filter($('TABLE_SCHEMA').is('my_db'))
        .apply('Field', i$('COLUMN_NAME'))
        .apply('Type', i$('COLUMN_TYPE'))
        .apply('Null', i$('IS_NULLABLE'))
        .apply('Key', i$('COLUMN_KEY'))
        .apply('Default', i$('COLUMN_DEFAULT'))
        .apply('Extra', i$('EXTRA'))
        .select('Field', 'Type', 'Null', 'Key', 'Default', 'Extra');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('DESCRIBE');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a relaxed table name DESCRIBE query", () => {
      var parse = Expression.parseSQL("DESCRIBE my_db.my-table*is:the/best_table; ");

      var ex2 = i$('COLUMNS')
        .filter(i$('TABLE_NAME').is(r('my-table*is:the/best_table')))
        .filter(i$('TABLE_SCHEMA').is('my_db'))
        .apply('Field', i$('COLUMN_NAME'))
        .apply('Type', i$('COLUMN_TYPE'))
        .apply('Null', i$('IS_NULLABLE'))
        .apply('Key', i$('COLUMN_KEY'))
        .apply('Default', i$('COLUMN_DEFAULT'))
        .apply('Extra', i$('EXTRA'))
        .select('Field', 'Type', 'Null', 'Key', 'Default', 'Extra');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('DESCRIBE');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a relaxed column name", () => {
      var parse = Expression.parseSQL("DESCRIBE my_db.my-table cityName; ");

      var ex2 = i$('COLUMNS')
        .filter(i$('TABLE_NAME').is(r('my-table')))
        .filter(i$('TABLE_SCHEMA').is('my_db'))
        .filter(i$('COLUMN_NAME').is('cityName'))
        .apply('Field', i$('COLUMN_NAME'))
        .apply('Type', i$('COLUMN_TYPE'))
        .apply('Null', i$('IS_NULLABLE'))
        .apply('Key', i$('COLUMN_KEY'))
        .apply('Default', i$('COLUMN_DEFAULT'))
        .apply('Extra', i$('EXTRA'))
        .select('Field', 'Type', 'Null', 'Key', 'Default', 'Extra');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('DESCRIBE');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a wild card (also EXPLAIN)", () => {
      var parse = Expression.parseSQL("EXPLAIN my_db.my-table 'city%'; ");

      var ex2 = i$('COLUMNS')
        .filter(i$('TABLE_NAME').is(r('my-table')))
        .filter(i$('TABLE_SCHEMA').is('my_db'))
        .filter(i$('COLUMN_NAME').match('^city.*$'))
        .apply('Field', i$('COLUMN_NAME'))
        .apply('Type', i$('COLUMN_TYPE'))
        .apply('Null', i$('IS_NULLABLE'))
        .apply('Key', i$('COLUMN_KEY'))
        .apply('Default', i$('COLUMN_DEFAULT'))
        .apply('Extra', i$('EXTRA'))
        .select('Field', 'Type', 'Null', 'Key', 'Default', 'Extra');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('DESCRIBE');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

  });


  describe("other queries", () => {
    it("knows of USE query", () => {
      var parse = Expression.parseSQL("USE plyql1;");

      expect(parse.verb).to.equal('USE');
      expect(parse.database).to.equal('plyql1');
    });

    it("knows of USE query with back-ticks", () => {
      var parse = Expression.parseSQL("USE `plyql2`");

      expect(parse.verb).to.equal('USE');
      expect(parse.database).to.equal('plyql2');
    });

    it("works information query", () => {
      var parse = Expression.parseSQL("SELECT current_user();");

      var ex2 = ply()
        .apply('current_user()', r('plyql@localhost'));

      expect(parse.verb).to.equal('SELECT');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

  });

});
