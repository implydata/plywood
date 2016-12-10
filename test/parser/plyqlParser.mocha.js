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

let { expect } = require("chai");
let { sane } = require('../utils');

let { Timezone } = require('chronoshift');

let plywood = require('../plywood');
let { Expression, i$, $, ply, r, Set, Dataset, External, ExternalExpression } = plywood;

function resolvesProperly(parse) {
  let resolveString = parse.expression.resolve({ t: 'STR' });
  expect(resolveString.operand.operand.type).to.deep.equal("STRING");
  let resolveTime = parse.expression.resolve({ t: new Date() });
  expect(resolveTime.operand.operand.type).to.deep.equal("TIME");
}

describe("SQL parser", () => {
  describe("basic expression", () => {
    let $data = $('data');

    it("works with a literal expression", () => {
      let parse = Expression.parseSQL(" 1 ");
      let ex2 = r(1);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a fancy number expression", () => {
      let parse = Expression.parseSQL("-5e-2");
      let ex2 = r(-5e-2);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with an empty set", () => {
      let parse = Expression.parseSQL("{}");
      let ex2 = r(Set.fromJS([]));

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a basic set", () => {
      let parse = Expression.parseSQL("{'a', 'b'}");
      let ex2 = r(Set.fromJS(['a', 'b']));

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a set containing null", () => {
      let parse = Expression.parseSQL("{'a', 'b', NULL}");
      let ex2 = r(Set.fromJS(['a', 'b', null]));

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a set that is only null", () => {
      let parse = Expression.parseSQL("{NULL}");
      let ex2 = r(Set.fromJS([null]));

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a COUNT expression", () => {
      let parse = Expression.parseSQL("COUNT()");

      let ex2 = $('data').count();

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a filtered COUNT expression", () => {
      let parse = Expression.parseSQL("COUNT(* WHERE cityName = 'San Francisco')");

      let ex2 = $('data').filter("i$cityName == 'San Francisco'").count();

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a LENGTH expression", () => {
      let parse = Expression.parseSQL("LENGTH(`page`)");
      let parse2 = Expression.parseSQL("LEN(`page`)");
      let parse3 = Expression.parseSQL("CHAR_LENGTH(`page`)");

      let ex2 = i$('page').length();

      expect(parse.verb).to.equal(null);
      expect(parse2.verb).to.equal(null);
      expect(parse3.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
      expect(parse2.expression.toJS()).to.deep.equal(ex2.toJS());
      expect(parse3.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a YEAR expression", () => {
      let parse = Expression.parseSQL("YEAR(time)");

      let ex2 = i$('time').timePart('YEAR');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with NULL arguments", () => {
      let parse = Expression.parseSQL("CONCAT(NULL, 'lol')");

      let ex2 = r(null).concat('"lol"');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a filtered SUM 1 expression", () => {
      let parse = Expression.parseSQL("SUM(1 WHERE cityName = 'San Francisco')");

      let ex2 = $('data').filter("i$cityName == 'San Francisco'").sum(1);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a filtered SUM expression", () => {
      let parse = Expression.parseSQL("SUM(added WHERE cityName = 'San Francisco')");

      let ex2 = $('data').filter("i$cityName == 'San Francisco'").sum('i$added');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a filtered QUANTILE expression", () => {
      let parse = Expression.parseSQL("QUANTILE(added WHERE cityName = 'San Francisco', 0.99)");

      let ex2 = $('data').filter("i$cityName == 'San Francisco'").quantile('i$added', 0.99);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a trivial arithmetic expression", () => {
      let parse = Expression.parseSQL("(1 + 7) + 5 / 2");

      let ex2 = Expression.parse('(1 + 7) + 5 / 2');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with an arithmetic expression", () => {
      let parse = Expression.parseSQL("SUM(`added`) + 5 + SUM(deleted) / 2");

      let ex2 = $data.sum('i$added').add(5, '$data.sum(i$deleted) / 2');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with IN expression (value list)", () => {
      let parse = Expression.parseSQL("language IN ( 'ca', 'cs', 'da', 'el' )");

      let ex2 = i$('language').in(['ca', 'cs', 'da', 'el']);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with IN expression (list literal)", () => {
      let parse = Expression.parseSQL("language IN { 'ca', 'cs', 'da', 'el' }");

      let ex2 = i$('language').in(['ca', 'cs', 'da', 'el']);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with IN expression (variable)", () => {
      let parse = Expression.parseSQL("language IN languages");

      let ex2 = i$('language').in('i$languages');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should handle --", () => {
      let parse = Expression.parseSQL("x--3");

      let ex2 = i$('x').subtract(-3);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should handle fallback --", () => {
      let parse = Expression.parseSQL("IFNULL(null,'fallback')");
      let parse2 = Expression.parseSQL("IFNULL(null, SUM(deleted))");
      let parse3 = Expression.parseSQL("IFNULL(SUM(`added`), SUM(deleted))");
      let parse4 = Expression.parseSQL("FALLBACK(SUM(`added`), SUM(deleted))");

      let ex = r(null).fallback('fallback');
      let ex2 = r(null).fallback('$data.sum(i$deleted)');
      let ex3 = $data.sum('i$added').fallback('$data.sum(i$deleted)');

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
      expect(parse2.expression.toJS()).to.deep.equal(ex2.toJS());
      expect(parse3.expression.toJS()).to.deep.equal(ex3.toJS());
      expect(parse4.expression.toJS()).to.deep.equal(ex3.toJS());
    });

    it("works with NOW()", () => {
      let parse = Expression.parseSQL("NOW( )");

      let js = parse.expression.toJS();
      expect(js.op).to.equal('literal');
      expect(Math.abs(js.value.valueOf() - Date.now())).to.be.lessThan(1000);
    });

    it("it works with raw aggregate", () => {
      let parse = Expression.parseSQL("(SUM(added) + 1000) / 2");

      let ex2 = $('data').sum('i$added').add(1000).divide(2);

      expect(parse.verb).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    describe("date literals - parse and resolve", () => {
      it('works with inferred literals', () => {
        let tests = sane`
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
          let parse = Expression.parseSQL(test);
          let left = test.substring(1, test.indexOf("' <="));
          let right = test.substring(test.indexOf('< ') + 3, test.length -1);
          let ex = r(left).lessThanOrEqual('i$t').and(i$('t').lessThan(r(right)));
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
          resolvesProperly(parse);
        });
      });

      it('works with a custom Timezone in inferred literals', () => {
        let tests = sane`
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
          let parse = Expression.parseSQL(test, Timezone.fromJS('America/New_York'));
          let left = test.substring(1, test.indexOf("' <="));
          let right = test.substring(test.indexOf('< ') + 3, test.length -1);
          let ex = r(left).lessThanOrEqual('i$t').and(i$('t').lessThan(r(right)));
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
          resolvesProperly(parse);
        });
      });

      it('works with DATE', () => {
        let tests = sane`
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

        let ex = r(new Date('2016-02-09Z'));
        tests.split('\n').forEach(test => {
          let parse = Expression.parseSQL(test);
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
        });
      });

      it('works with DATE()', () => {
        let tests = sane`
          DATE("2016-02-09")
          DATE("2016-02-09 00:00:00")
        `;

        let ex = r(new Date('2016-02-09Z')).timeFloor('P1D');
        tests.split('\n').forEach(test => {
          let parse = Expression.parseSQL(test);
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
        });
      });

      it('works with TIMESTAMP', () => {
        let tests = sane`
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

        let ex = r(new Date('2016-02-09T01:02:03Z'));
        tests.split('\n').forEach(test => {
          let parse = Expression.parseSQL(test);
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
        });
      });

      it('works with TIMESTAMP.ms', () => {
        let tests = sane`
          TIMESTAMP "2016-02-09T01:02:03.456Z"
          TIMESTAMP "2016-02-09 01:02:03.456"
          TIMESTAMP "2016/02$09 01:02:03.456789"
        `;

        let ex = r(new Date('2016-02-09T01:02:03.456Z'));
        tests.split('\n').forEach(test => {
          let parse = Expression.parseSQL(test);
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
        let tests = sane`
          t BETWEEN TIMESTAMP '2015-09-12T10:30:00' AND TIMESTAMP '2015-09-12T12:30:00'
        `;

        let ex = i$('t').greaterThanOrEqual(new Date('2015-09-12T10:30:00Z')).and(i$('t').lessThanOrEqual(new Date('2015-09-12T12:30:00Z')));
        tests.split('\n').forEach(test => {
          let parse = Expression.parseSQL(test);
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
        });
      });

      it('works inside BETWEEN of literals', () => {
        let tests = sane`
          t BETWEEN '2015-09-12T10:30:00' AND '2015-09-12T12:30:00'
          t BETWEEN '2015-09-12T10:30' AND '2015-09-12T12:30'
        `;

        let exs = [
          i$('t').greaterThanOrEqual(r('2015-09-12T10:30:00')).and(i$('t').lessThanOrEqual(r('2015-09-12T12:30:00'))),
          i$('t').greaterThanOrEqual(r('2015-09-12T10:30')).and(i$('t').lessThanOrEqual(r('2015-09-12T12:30')))
        ];

        tests.split('\n').forEach((test, i) => {
          let parse = Expression.parseSQL(test);
          expect(parse.expression.toJS()).to.deep.equal(exs[i].toJS());
          resolvesProperly(parse);
        });
      });

      it('works inside BETWEEN of years', () => {
        let tests = sane`
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
          let parse = Expression.parseSQL(test);
          let left = /(?:BETWEEN ')([0-9-+:TZ.\s]+)/g.exec(test)[1];
          let right = /(?:AND ')([0-9-+:TZ.\s]+)/g.exec(test)[1];
          let ex = i$('t').greaterThanOrEqual(r(left)).and(i$('t').lessThanOrEqual(r(right)));
          expect(parse.expression.toJS()).to.deep.equal(ex.toJS());
          resolvesProperly(parse);
        });
      });

    });

  });

  describe("other query types", () => {
    it("works with UPDATE expression", () => {
      let parse = Expression.parseSQL("UPDATE this is the end of the road");

      expect(parse).to.deep.equal({
        verb: 'UPDATE',
        rest: 'this is the end of the road'
      });
    });

    it("works with SET query", () => {
      let parse = Expression.parseSQL("SET this is the end of the road");

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
      let parse = Expression.parseSQL(sane`
        SELECT 1+1 AS Two, 1+3 AS Three;
      `);

      let ex2 = ply()
        .apply('Two', '1+1')
        .apply('Three', '1+3');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.table).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a simple expression", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
        COUNT() AS 'Count'
        FROM \`wiki\`
      `);

      let ex2 = ply()
        .apply('data', '$wiki')
        .apply('Count', '$data.count()');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.table).to.equal('wiki');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a simple expression 2", () => {
      let parse = Expression.parseSQL(sane`
        SELECT  COUNT(page)
      `);

      let ex2 = ply()
        .apply('data', '$data')
        .apply('COUNT(page)', '$data.filter(i$page.isnt(null)).count()');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.table).to.equal(null);
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a relaxed table name", () => {
      let parse = Expression.parseSQL(sane`
        SELECT * FROM my-table*is:the/best_table;
      `);

      let ex2 = $('my-table*is:the/best_table');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.table).to.equal('my-table*is:the/best_table');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a table alias", () => {
      let parse1 = Expression.parseSQL('SELECT * FROM my-table AS t;');
      let parse2 = Expression.parseSQL('SELECT * FROM my-table t;');

      let ex2 = $('my-table');

      expect(parse1.verb).to.equal('SELECT');
      expect(parse1.table).to.equal('my-table');
      expect(parse1.expression.toJS()).to.deep.equal(ex2.toJS());
      expect(parse2.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a total expression with all sorts of applies", () => {
      let parse = Expression.parseSQL(sane`
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
        TIME_SHIFT(time, PT1H) AS 'TimeShift1',
        TIME_SHIFT(time, PT1H, 3) AS 'TimeShift3',
        TIME_RANGE(time, PT1H) AS 'TimeRange1',
        TIME_RANGE(time, PT1H, 3) AS 'TimeRange3',
        OVERLAP(x, y) AS 'Overlap',
        CUSTOM('blah') AS 'Custom1',
        CUSTOM_AGGREGATE('blah') AS 'Custom2'
        FROM \`wiki\`
        WHERE \`language\`="en"  ;  -- This is just some comment
      `);

      let ex2 = ply()
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
        .apply('TimeShift1', i$('time').timeShift('PT1H'))
        .apply('TimeShift3', i$('time').timeShift('PT1H', 3))
        .apply('TimeRange1', i$('time').timeRange('PT1H'))
        .apply('TimeRange3', i$('time').timeRange('PT1H', 3))
        .apply('Overlap', i$('x').overlap('i$y'))
        .apply('Custom1', $('data').customAggregate('blah'))
        .apply('Custom2', $('data').customAggregate('blah'))
        .select("aISb1", "aISb2", "aISb3", "Count1", "Count2", "Count3", "Count4", "Match", "CustomTransform",
          "TotalAdded", "Date", "TotalAddedOver4", "False", "MinusAdded", "AbsAdded", "AbsoluteAdded", "SqRtAdded",
          "SqRtAdded2", "SquareRoot", "One", "SimplyAdded", "Median", "Unique1", "Unique2", "Unique3",
          "TimeBucket", "TimeFloor", "TimeShift1", "TimeShift3", "TimeRange1", "TimeRange3", "Overlap", "Custom1", "Custom2");

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a total expression without GROUP BY clause", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
        SUM(added) AS TotalAdded
        FROM \`wiki\`
        WHERE \`language\`="en"
      `);

      let ex2 = ply()
        .apply('data', '$wiki.filter(i$language == "en")')
        .apply('TotalAdded', '$data.sum(i$added)')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse ISNULL", () => {
      let parse = Expression.parseSQL(sane`
        SELECT * FROM wikipedia WHERE NOT ISNULL(cityName) LIMIT 10
      `);

      let ex2 = $('wikipedia')
        .filter('i$cityName.is(null).not()')
        .limit(10);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with all sorts of comments", () => {
      let parse = Expression.parseSQL(`/*
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

      let ex2 = ply()
        .apply('data', '$wiki.filter(i$language == "en")')
        .apply('TotalAdded', '$data.sum(i$added) - -1');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work without a FROM", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
        SUM(added) AS 'TotalAdded'
        WHERE \`language\`="en"
      `);

      let ex2 = ply()
        .apply('data', '$data.filter(i$language == "en")')
        .apply('TotalAdded', '$data.sum(i$added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a BETWEEN", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
        SUM(added) AS 'TotalAdded'
        WHERE \`language\`="en" AND \`time\` BETWEEN '2015-01-01T10:30:00' AND '2015-01-02T12:30:00'
      `);

      let ex2 = ply()
        .apply('data', $('data').filter(
          i$('language').is("en")
            .and(i$('time').greaterThanOrEqual(r('2015-01-01T10:30:00'))
              .and(i$('time').lessThanOrEqual(r('2015-01-02T12:30:00')))
            )
        ))
        .apply('TotalAdded', '$data.sum(i$added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with <= <", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
        SUM(added) AS 'TotalAdded'
        WHERE \`language\`="en" AND '2015-01-01T10:30:00' <= \`time\` AND \`time\` < '2015-01-02T12:30:00'
      `);

      let ex2 = ply()
        .apply('data', $('data').filter(
          i$('language').is("en")
            .and(r('2015-01-01T10:30:00').lessThanOrEqual(i$('time')))
            .and(i$('time').lessThan(r('2015-01-02T12:30:00')))
        ))
        .apply('TotalAdded', '$data.sum(i$added)');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());

      //let ex2s = ply()
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
      let parse = Expression.parseSQL(sane`
        SELECT
        \`page\` AS 'Page',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        WHERE \`language\`="en" AND \`time\` BETWEEN '2015-01-01T10:30:00' AND '2015-01-02T12:30:00'
        GROUP BY 1
      `);

      let ex2 = $('wiki').filter(
        i$('language').is("en")
          .and(i$('time').greaterThanOrEqual(r('2015-01-01T10:30:00'))
            .and(i$('time').lessThanOrEqual(r('2015-01-02T12:30:00')))
          )
      ).split('i$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work without top level GROUP BY with ORDER BY and LIMIT", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
        \`page\` AS 'Page',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY \`page\`
        ORDER BY TotalAdded
        LIMIT 5
      `);

      let ex2 = $('wiki').split('i$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'TotalAdded')
        .sort('i$TotalAdded', 'ascending')
        .limit(5);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with fancy limit LIMIT 0,x", () => {
      let parse = Expression.parseSQL(sane`
        SELECT SUM(added) AS 'TotalAdded' FROM \`wiki\` LIMIT 0, 5
      `);

      let ex2 = ply()
        .apply('data', '$wiki')
        .apply('TotalAdded', '$data.sum(i$added)')
        .limit(5);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work without top level GROUP BY with LIMIT only", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
        \`page\` AS 'Page',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY \`page\`
        LIMIT 5
      `);

      let ex2 = $('wiki').split('i$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'TotalAdded')
        .limit(5);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with multi-dimensional GROUP BYs", () => {
      let parse = Expression.parseSQL(sane`
        SELECT \`page\`, \`user\` FROM \`wiki\` GROUP BY \`page\`, \`user\`
      `);

      let ex2 = $('wiki').split({ page: 'i$page', user: 'i$user' }, 'data').select('page', 'user');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SELECT DISTINCT", () => {
      let parse = Expression.parseSQL(sane`
        SELECT DISTINCT \`page\`, \`user\` FROM \`wiki\`
      `);

      let ex2 = $('wiki').split({ page: 'i$page', user: 'i$user' }, 'data').select('page', 'user');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with few spaces", () => {
      let parse = Expression.parseSQL(sane`
        SELECT\`page\`AS'Page'FROM\`wiki\`GROUP BY\`page\`ORDER BY\`Page\`LIMIT 5
      `);

      let ex2 = $('wiki').split('i$page', 'Page', 'data')
        .sort('i$Page', 'ascending')
        .limit(5);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a TIME_BUCKET function", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
        TIME_BUCKET(\`time\`, 'PT1H', 'Etc/UTC') AS 'TimeByHour',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      let ex2 = $('wiki').split(i$('time').timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('TimeByHour', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a DATE_FORMAT function (PT1S)", () => {
      let parse = Expression.parseSQL("DATE_FORMAT(`wikipedia`.`time`, '%Y-%m-%d %H:%i:%s' )");
      let ex2 = i$('time').timeFloor('PT1S');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a DATE_FORMAT function (PT1M)", () => {
      let parse = Expression.parseSQL("DATE_FORMAT(`wikipedia`.`time`, '%Y-%m-%d %H:%i:00' )");
      let ex2 = i$('time').timeFloor('PT1M');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a DATE_FORMAT function (PTD)", () => {
      let parse = Expression.parseSQL("DATE_FORMAT(`wikipedia`.`time`, '%Y-%m-%d' )");
      let ex2 = i$('time').timeFloor('P1D');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a DATE_FORMAT function (PTD)", () => {
      let parse = Expression.parseSQL("DATE_FORMAT(`wikipedia`.`time`, '%Y-%m-%d' )");
      let ex2 = i$('time').timeFloor('P1D');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a ADDDATE(DATE_FORMAT) function (PTD)", () => {
      let parse = Expression.parseSQL("ADDDATE(DATE_FORMAT(`wikipedia`.`time`, '%Y-%m-%d' ), INTERVAL 0 SECOND )");
      let ex2 = i$('time').timeFloor('P1D');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("throws on non zero interval", () => {
      expect(() => {
        Expression.parseSQL("ADDDATE(DATE_FORMAT(`wikipedia`.`time`, '%Y-%m-%d' ), INTERVAL 1 SECOND )");
      }).to.throw(`SQL parse error: only zero intervals supported for now on 'ADDDATE(DATE_FORMAT(\`wikipedia\`.\`time\`, '%Y-%m-%d' ), INTERVAL 1 SECOND )'`);
    });

    it("should work with SELECT *", () => {
      let parse = Expression.parseSQL(sane`
        SELECT * FROM \`wiki\`
      `);

      let ex2 = $('wiki');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SELECT character set introducer", () => {
      let parse = Expression.parseSQL("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = N'plyql1'");
      let parse2 = Expression.parseSQL("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = n'plyql1'");
      let parse3 = Expression.parseSQL("SELECT COUNT(*)FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = _utf8'plyql1'");

      let ex2 = ply()
        .apply('data', $('TABLES').filter(i$("TABLE_SCHEMA").is(r("plyql1"))))
        .apply('COUNT(*)', $('data').count())
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
      expect(parse2.expression.toJS()).to.deep.equal(ex2.toJS());
      expect(parse3.expression.toJS()).to.deep.equal(ex2.toJS());

      expect(() => {
        Expression.parseSQL("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = _uStf8'plyql1'");
      }, "assume charsets are all lower case or numbers").to.throw("SQL parse error")

      expect(() => {
        Expression.parseSQL("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = _uStf8'plyql1");
      }, "unmatched quote, assume charsets are all lower case or numbers").to.throw("SQL parse error")

    });

    it("should work with SELECT * WHERE ...", () => {
      let parse = Expression.parseSQL(sane`
        SELECT * FROM \`wiki\` WHERE language = 'en'
      `);

      let ex2 = $('wiki')
        .filter('i$language == "en"');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a SELECT query with renames", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
          page,
          isNew AS IsNew,
          added AS Added
        FROM \`wiki\`
        WHERE \`language\`="en"
      `);

      let ex2 = $('wiki')
        .filter('i$language == en')
        .apply('page', 'i$page')
        .apply('IsNew', 'i$isNew')
        .apply('Added', 'i$added')
        .select('page', 'IsNew', 'Added');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SELECT and stuff", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
          CONCAT(page, "lol") AS Page,
          added + 1 as added
        FROM \`wiki\`
        WHERE language = 'en'
        HAVING added > 100
        ORDER BY \`page\` DESC
        LIMIT 10
      `);

      let ex2 = $('wiki')
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
      let parse = Expression.parseSQL(sane`
          SELECT isRobot as "isRobot", isNew as "isNew", COUNT() AS "Count" from data group by isNew, isRobot, page limit 2
      `);

      let ex2 = $('data')
        .split({ 'isNew': 'i$isNew', 'isRobot': 'i$isRobot', 'split2' : 'i$page' })
        .apply('Count', $('data').count())
        .select('isRobot', 'isNew', 'Count')
        .limit(2);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });


    it("should work a select to the end of group by queries", () => {
      let parse = Expression.parseSQL(sane`
          SELECT COUNT() AS "Count", isRobot as "isRobot", page as "Page", isNew as "isNew" from data group by isNew, isRobot, page limit 2
      `);

      let ex2 = $('data')
        .split({ 'isNew': 'i$isNew', 'isRobot': 'i$isRobot', 'Page' : 'i$page' })
        .apply('Count', $('data').count())
        .select('Count', 'isRobot', 'Page', 'isNew')
        .limit(2);

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a NUMBER_BUCKET function", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
        NUMBER_BUCKET(added, 10, 1 ) AS 'AddedBucket',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      let ex2 = $('wiki').split(i$('added').numberBucket(10, 1), 'AddedBucket', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('AddedBucket', 'TotalAdded')

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a TIME_PART function", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
        TIME_PART(\`time\`, DAY_OF_WEEK, 'Etc/UTC' ) AS 'DayOfWeek',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      let ex2 = $('wiki').split(i$('time').timePart('DAY_OF_WEEK', 'Etc/UTC'), 'DayOfWeek', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('DayOfWeek', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with time casting function", () => {
      let parse = Expression.parseSQL(sane`
        SELECT UNIX_TIMESTAMP(\`time\`) as Unix FROM \`wiki\`
        WHERE \`time\` BETWEEN FROM_UNIXTIME(1447430881) AND FROM_UNIXTIME(1547430881)
      `);

      let ex2 = $('wiki')
        .filter(i$('time').greaterThanOrEqual(r(1447430881).multiply(1000).cast('TIME')).and(i$('time').lessThanOrEqual(r(1547430881).multiply(1000).cast('TIME'))))
        .apply('Unix', i$('time').cast('NUMBER').divide(1000))
        .select('Unix');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work number casting function", () => {
      let parse = Expression.parseSQL(sane`
        SELECT * FROM \`wiki\`
        WHERE FROM_UNIXTIME(1447430881) < \`time\` AND \`time\` < FROM_UNIXTIME(1547430881)
      `);

      let ex2 = $('wiki').filter(
        r(1447430881).multiply(1000).cast('TIME').lessThan(i$('time')).and(i$('time').lessThan(r(1547430881).multiply(1000).cast('TIME')))
      );

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with number cast and string cast function", () => {
      let parse = Expression.parseSQL(sane`
        SELECT CAST(\`commentLength\` AS CHAR) as castedString, CAST(\`commentLengthStr\` AS SIGNED) as castedNumber FROM \`wiki\`
      `);

      let ex2 = $('wiki')
        .apply('castedString', i$('commentLength').cast('STRING'))
        .apply('castedNumber', i$('commentLengthStr').cast('NUMBER'))
        .select('castedString', 'castedNumber');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a SUBSTR and CONCAT function", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
          CONCAT('[', SUBSTRING(SUBSTR(\`page\`, 0, 3 ), 1, 2), ']') AS 'Crazy',
          SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      let ex2 = $('wiki').split("'[' ++ i$page.substr(0, 3).substr(1, 2) ++ ']'", 'Crazy', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Crazy', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a LOWER and UPPER function", () => {
      let parse = Expression.parseSQL(sane`
        SELECT 
          CONCAT(UPPER(SUBSTRING(\`user\`, 1, 1)), LCASE(SUBSTRING(\`user\`, 2, 5))) AS 'ProperName',
          UPPER(\`page\`) AS 'Upper',
          UCASE(\`channel\`) AS 'UCase',
          SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      let ex2 = $('wiki').split("(i$user.substr(1, 1).transformCase('upperCase')).concat(i$user.substr(2,5).transformCase('lowerCase'))", 'ProperName', 'data')
        .apply('Upper', 'i$page.transformCase("upperCase")')
        .apply('UCase', 'i$channel.transformCase("upperCase")')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('ProperName', 'Upper', 'UCase', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with a LOCATE function greater than 0", () => {
      let parse = Expression.parseSQL(sane`
        SELECT \`page\` AS 'Page',
        SUM(added) AS 'TotalAdded'
        FROM \`wiki\` WHERE LOCATE(\`page\`, 'title') > 0
        GROUP BY 1
      `);

      let ex2 = $('wiki').filter(i$('page').indexOf('title').add(1).in({start: 0, end: null, bounds: '()'})).split("i$page", 'Page', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'TotalAdded');

      expect(parse.expression.simplify().toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with EXTRACT function", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
          EXTRACT(\`page\`, '^Wiki(.+)$') AS 'Extract',
          SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      let ex2 = $('wiki').split("i$page.extract('^Wiki(.+)$')", 'Extract', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Extract', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with LOOKUP function", () => {
      let parse = Expression.parseSQL(sane`
        SELECT
          LOOKUP(\`language\`, 'language-lookup') AS 'Lookup',
          SUM(added) AS 'TotalAdded'
        FROM \`wiki\`
        GROUP BY 1
      `);

      let ex2 = $('wiki').split("i$language.lookup('language-lookup')", 'Lookup', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Lookup', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a complex filter", () => {
      let parse = Expression.parseSQL(sane`
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

      let ex2 = ply()
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
      let parse = Expression.parseSQL(sane`
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

      let ex2 = ply()
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
      let parse = Expression.parseSQL(sane`
        SELECT
          \`page or else?\` AS 'Page',
          SUM(added) AS 'TotalAdded'
        FROM \`wiki-tiki:taki\`
        WHERE \`language\`="en" AND \`time\` BETWEEN '2015-01-01T10:30:00' AND '2015-01-02T12:30:00'
        GROUP BY \`page or else?\`
      `);

      let ex2 = $('wiki-tiki:taki').filter(
        i$('language').is("en")
            .and(i$('time').greaterThanOrEqual(r('2015-01-01T10:30:00'))
              .and(i$('time').lessThanOrEqual(r('2015-01-02T12:30:00')))
            )
      ).split('i${page or else?}', 'Page', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with FROM (sub query)", () => {
      let parse = Expression.parseSQL(sane`
        SELECT * FROM (
          SELECT
            \`page\` AS 'Page',
            SUM(added) AS 'TotalAdded'
          FROM \`wiki\`
          WHERE \`language\`="en"
          GROUP BY \`page\`
        )
      `);

      let ex2 = $('wiki')
        .filter(i$('language').is("en"))
        .split('i$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with FROM (sub query) + alias", () => {
      let parse = Expression.parseSQL(sane`
        SELECT * FROM (
          SELECT
            \`page\` AS 'Page',
            SUM(added) AS 'TotalAdded'
          FROM \`wiki\`
          WHERE \`language\`="en"
          GROUP BY \`page\`
        ) AS T
      `);

      let ex2 = $('wiki')
        .filter(i$('language').is("en"))
        .split('i$page', 'Page', 'data')
        .apply('TotalAdded', '$data.sum(i$added)')
        .select('Page', 'TotalAdded');

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with FROM (sub query) complex in total", () => {
      let parse = Expression.parseSQL(sane`
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

      let ex2 = ply()
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
      let parse = Expression.parseSQL(sane`
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

      let ex2 = $('wiki')
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
      let parse = Expression.parseSQL(sane`
        SELECT SUM(\`wikipedia\`.\`added\`) AS \`sum_added_ok\`,
          ADDDATE( DATE_FORMAT( \`wikipedia\`.\`time\`, '%Y-%m-%d %H:%i:%s' ), INTERVAL 0 SECOND ) AS \`tsc_time_ok\`
        FROM \`wikipedia\`
        WHERE ((ADDDATE( DATE_FORMAT( \`wikipedia\`.\`time\`, '%Y-%m-%d %H:00:00' ), INTERVAL 0 SECOND ) >= TIMESTAMP('2015-09-12 00:00:00'))
          AND (ADDDATE( DATE_FORMAT( \`wikipedia\`.\`time\`, '%Y-%m-%d %H:00:00' ), INTERVAL 0 SECOND ) <= TIMESTAMP('2015-09-12 23:00:00')))
        GROUP BY 2
      `);

      let ex2 = $('wikipedia').filter("i$time.timeFloor(PT1H) >= '2015-09-12 00:00:00' and i$time.timeFloor(PT1H) <= '2015-09-12 23:00:00'")
        .split('i$time.timeFloor(PT1S)', 'tsc_time_ok', 'data')
        .apply('sum_added_ok', '$data.sum(i$added)')
        .select("sum_added_ok", "tsc_time_ok");

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with this quarter query", () => {
      let parse = Expression.parseSQL(sane`
        SELECT QUARTER(wikipedia.__time) AS qr___time_ok,
        SUM(wikipedia.added) AS sum_added_ok
        FROM wikipedia
        GROUP BY 1
      `);

      let ex2 = $('wikipedia')
        .split('i$__time.timePart(QUARTER)', 'qr___time_ok', 'data')
        .apply('sum_added_ok', '$data.sum(i$added)')
        .select("qr___time_ok", "sum_added_ok");

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with this fancy quarter query", () => {
      let parse = Expression.parseSQL(sane`
        SELECT SUM(wikipedia.added) AS sum_added_ok,
        ADDDATE( CONCAT( 
                  DATE_FORMAT( wikipedia.__time, '%Y-' ), 
                  (3*(QUARTER(wikipedia.__time)-1)+1), 
                  '-01 00:00:00' ), 
        INTERVAL 0 SECOND ) 
          AS tqr___time_ok
        FROM wikipedia
        GROUP BY 2
      `);

      let ex2 = $('wikipedia')
        .split(i$('__time').timeFloor('P3M'), 'tqr___time_ok', 'data')
        .apply('sum_added_ok', '$data.sum(i$added)')
        .select("sum_added_ok", "tqr___time_ok");

      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

  });


  describe("SHOW", () => {
    it("should work with SHOW VARIABLES like", () => {
      let parse = Expression.parseSQL(sane`
        SHOW VARIABLES like 'collation_server'
      `);

      let ex2 = i$('GLOBAL_VARIABLES')
        .filter(i$('VARIABLE_NAME').match('^collation.server$'))
        .apply('Variable_name', i$('VARIABLE_NAME'))
        .apply('Value', i$('VARIABLE_VALUE'))
        .select('Variable_name', 'Value');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SHOW VARIABLES WHERE", () => {
      let parse = Expression.parseSQL(sane`
        SHOW SESSION VARIABLES WHERE Variable_name ='language' OR Variable_name = 'net_write_timeout'
      `);

      let ex2 = i$('GLOBAL_VARIABLES')
        .filter(i$('Variable_name').is(r('language')).or(i$('Variable_name').is(r('net_write_timeout'))))
        .apply('Variable_name', i$('VARIABLE_NAME'))
        .apply('Value', i$('VARIABLE_VALUE'))
        .select('Variable_name', 'Value');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SHOW STATUS LIKE", () => {
      let parse = Expression.parseSQL(sane`
        SHOW SESSION STATUS LIKE 'Ssl_cipher'
      `);

      let ex2 = i$('GLOBAL_STATUS')
        .filter(i$('VARIABLE_NAME').match('^Ssl.cipher$'))
        .apply('Variable_name', i$('VARIABLE_NAME'))
        .apply('Value', i$('VARIABLE_VALUE'))
        .select('Variable_name', 'Value');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SHOW DATABASES", () => {
      let parse = Expression.parseSQL(sane`
        SHOW DATABASES
      `);

      let ex2 = i$('SCHEMATA')
        .apply('Database', i$('SCHEMA_NAME'))
        .select('Database');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SHOW SCHEMAS", () => {
      let parse = Expression.parseSQL(sane`
        SHOW SCHEMAS LIKE "Vannie%"
      `);

      let ex2 = i$('SCHEMATA')
        .filter(i$('SCHEMA_NAME').match('^Vannie.*$'))
        .apply('Database', i$('SCHEMA_NAME'))
        .select('Database');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SHOW TABLES", () => {
      let parse = Expression.parseSQL(sane`
        SHOW TABLES IN my_db
      `);

      let ex2 = i$('TABLES')
        .filter(i$('TABLE_SCHEMA').is('my_db'))
        .apply('Tables_in_database', $('TABLE_NAME'))
        .select('Tables_in_database');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SHOW FULL TABLES", () => {
      let parse = Expression.parseSQL(sane`
        SHOW FULL TABLES FROM \`my_db\` LIKE '%'
      `);

      let ex2 = i$('TABLES')
        .filter(i$('TABLE_SCHEMA').is('my_db'))
        .filter(i$('TABLE_NAME').match('^.*$'))
        .apply('Tables_in_database', $('TABLE_NAME'))
        .apply('Table_type', i$('TABLE_TYPE'))
        .select('Tables_in_database', 'Table_type');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SHOW COLUMNS", () => {
      let parse = Expression.parseSQL(sane`
        SHOW COLUMNS IN my_table IN my_db
      `);

      let ex2 = $('COLUMNS')
        .filter($('TABLE_NAME').is('my_table'))
        .filter($('TABLE_SCHEMA').is('my_db'))
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
      let parse = Expression.parseSQL(sane`
        SHOW FULL COLUMNS IN my_table IN my_db LIKE "T%"
      `);

      let ex2 = $('COLUMNS')
        .filter($('TABLE_NAME').is('my_table'))
        .filter($('TABLE_SCHEMA').is('my_db'))
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

    it("should work with SHOW character set", () => {

      let parse = Expression.parseSQL(sane`
        SHOW CHARACTER SET LIKE '%wild%'
      `);

      let ex2 = i$('CHARACTER_SETS')
        .filter(i$('CHARACTER_SET_NAME').match(r('^.*wild.*$')))
        .apply('Charset', i$('CHARACTER_SET_NAME'))
        .apply('Default collation', i$('DEFAULT_COLLATE_NAME'))
        .apply('Description', i$('DESCRIPTION'))
        .apply('Maxlen', i$('MAXLEN'))
        .select('Charset', 'Default collation', 'Description', 'Maxlen');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
   });

    it("should work with SHOW collations", () => {

      let parse = Expression.parseSQL(sane`
        SHOW COLLATION like '%wild%'
      `);

      let ex2 = i$('COLLATIONS')
        .filter(i$('COLLATION_NAME').match(r('^.*wild.*$')))
        .apply('Collation', i$('COLLATION_NAME'))
        .apply('Charset', i$('CHARACTER_SET_NAME'))
        .apply('Id', i$('ID'))
        .apply('Default', i$('IS_DEFAULT'))
        .apply('Compiled', i$('IS_COMPILED'))
        .apply('Sortlen', i$('SORTLEN'))
        .select('Collation', 'Charset', 'Id', 'Default', 'Compiled', 'Sortlen');

      expect(parse.verb).to.equal('SELECT');
      expect(parse.rewrite).to.equal('SHOW');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });
});


  describe("DESCRIBE", () => {
    it("works with DESCRIBE query", () => {
      let parse = Expression.parseSQL("DESCRIBE wikipedia");

      let ex2 = $('COLUMNS')
        .filter($('TABLE_NAME').is('wikipedia'))
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
      let parse = Expression.parseSQL("DESCRIBE my_db.my-table*is:the/best_table; ");

      let ex2 = $('COLUMNS')
        .filter($('TABLE_NAME').is(r('my-table*is:the/best_table')))
        .filter($('TABLE_SCHEMA').is('my_db'))
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
      let parse = Expression.parseSQL("DESCRIBE my_db.my-table cityName; ");

      let ex2 = $('COLUMNS')
        .filter($('TABLE_NAME').is(r('my-table')))
        .filter($('TABLE_SCHEMA').is('my_db'))
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
      let parse = Expression.parseSQL("EXPLAIN my_db.my-table 'city%'; ");

      let ex2 = $('COLUMNS')
        .filter($('TABLE_NAME').is(r('my-table')))
        .filter($('TABLE_SCHEMA').is('my_db'))
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
      let parse = Expression.parseSQL("USE plyql1;");

      expect(parse.verb).to.equal('USE');
      expect(parse.database).to.equal('plyql1');
    });

    it("knows of USE query with back-ticks", () => {
      let parse = Expression.parseSQL("USE `plyql2`");

      expect(parse.verb).to.equal('USE');
      expect(parse.database).to.equal('plyql2');
    });

    it("works information query", () => {
      let parse = Expression.parseSQL("SELECT current_user();");

      let ex2 = ply()
        .apply('current_user()', r('plyql@localhost'));

      expect(parse.verb).to.equal('SELECT');
      expect(parse.expression.toJS()).to.deep.equal(ex2.toJS());
    });

  });

});
