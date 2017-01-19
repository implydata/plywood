/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2017 Imply Data, Inc.
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

const { expect } = require("chai");

let plywood = require('../plywood');
let { Expression, $, ply, r } = plywood;

describe("expression parser", () => {
  describe("errors", () => {
    it("should not get confused with parsable expressions in strange places", () => {
      expect(() => {
        Expression.parse("ply().apply($x + 1, $x + 1)");
      }).to.throw('could not extract a string out of');
    });
  });


  describe("parses", () => {
    it("should parse the mega definition", () => {
      let ex1 = ply()
        .apply('is1', '$color == "Red"')
        .apply('is2', '$country.is("USA")')
        .apply('isnt1', '$color != "Red"')
        .apply('isnt2', '$country.isnt("USA")')
        .apply('less_than_number', '$price < 5')
        .apply('less_than_time', '$time < "2015-03-03Z"')
        .apply('contains', '$city.contains("San")')
        .apply('match', '$city.match("^San")')
        .apply('constant_negative', '-7')
        .apply('parent_x', "$^x")
        .apply('typed_y', "$y:STRING")
        .apply('sub_typed_z', "$z:SET/STRING")
        .apply('or', '$a or $b or $c')
        .apply('and', '$a and $b and $c')
        .apply('addition1', "$x + 10 - $y")
        .apply('addition2', "$x.add(1)")
        .apply('multiplication1', "$x * 10 / $y")
        .apply('multiplication2', "$x.multiply($y)")
        .apply('negate1', "-$x")
        .apply('negate2', "$x.negate()")
        .apply('length', "$x.length()")
        .apply('abs', "$x.absolute()")
        .apply('absolute', "$x.absolute()")
        .apply('absolute_pipes', "|$x|")
        .apply('absolute_pipes2', "| -2 |")
        .apply('x_power_of_22', "$x.power(22)")
        .apply('x_power_of_2', "$x ^ 2")
        .apply('x_power_of_2_nosp', "$x^2")
        .apply('sqrt', "$x.sqrt()")
        .apply('power_respects_orderOfOperations', "$x^3^4")
        .apply('identity', "+$x")
        .apply('string_set', "['a', 'b', 'c']")
        .apply('number_set', "[1, 2, 3]")
        .apply('in_string_set', "$x.in(['a', 'b', 'c'])")
        .apply('in_number_set', "$x.in([1, 2, 3])")
        .apply('in_number_range', "$x.in(1, 2)")
        .apply('in_date_range', "$x.in(['2015-03-03Z', '2015-10-10Z'])")
        .apply('overlap_set', "$x.overlap(['a', 'b', 'c'])")
        .apply('substr', "$x.substr(1, 5)")
        .apply('upper', "$x.transformCase('upperCase')")
        .apply('lower', "$x.transformCase('lowerCase')")
        .apply('extract', "$x.extract('_([^_]+)_')")
        .apply('lookup', "$x.lookup('some_lookup')")
        .apply('another_lookup', "$x.lookup(another_lookup)")
        .apply('extract_fallback', "$x.extract('_([^_]+)_').fallback('none')")
        .apply('lookup_fallback', "$x.lookup('some_lookup').fallback('none')")
        .apply('agg_count', "$data.count()")
        .apply('agg_sum', "$data.sum($price)")
        .apply('agg_average', "$data.average($price)")
        .apply('agg_min', "$data.min($price)")
        .apply('agg_max', "$data.max($price)")
        .apply('agg_quantile', "$data.quantile($price, 0.5)")
        .apply('custom_transform', '$city.customTransform("myExtractionFn")')
        .apply('agg_custom', "$data.custom(blah)")
        .apply('agg_customAggregate', "$data.customAggregate(blah)")
        .apply('agg_split', "$data.split($carat, 'Carat')")
        .apply('agg_filter_count', "$data.filter($country == 'USA').count()")
        .apply('time_bucket', "$time.timeBucket(P1D)")
        .apply('time_bucket_timezone', "$time.timeBucket(P1D, 'America/Los_Angeles')")
        .apply('time_floor', "$time.timeFloor(P1D)")
        .apply('time_floor_timezone', "$time.timeFloor(P1D, 'America/Los_Angeles')")
        .apply('time_part', "$time.timePart(DAY_OF_WEEK)")
        .apply('time_part_timezone', "$time.timePart(DAY_OF_WEEK, 'America/Los_Angeles')")
        .apply('time_shift', "$time.timeShift(P1D, -3)")
        .apply('time_shift_timezone', "$time.timeShift(P1D, -3, 'America/Los_Angeles')")
        .apply('time_range', "$time.timeRange(P1D, -3)")
        .apply('time_range_timezone', "$time.timeRange(P1D, -3, 'America/Los_Angeles')");

      let ex2 = ply()
        .apply('is1', $('color').is("Red"))
        .apply('is2', $('country').is("USA"))
        .apply('isnt1', $('color').is("Red").not())
        .apply('isnt2', $('country').isnt("USA"))
        .apply('less_than_number', $('price').lessThan(5))
        .apply('less_than_time', $('time').lessThan("'2015-03-03Z'"))
        .apply('contains', $('city').contains("San"))
        .apply('match', $('city').match("^San"))
        .apply('constant_negative', -7)
        .apply('parent_x', $("x", 1))
        .apply('typed_y', { op: 'ref', name: 'y', type: 'STRING' })
        .apply('sub_typed_z', { op: 'ref', name: 'z', type: 'SET/STRING' })
        .apply('or', $('a').or($('b'), $('c')))
        .apply('and', $('a').and($('b'), $('c')))
        .apply('addition1', $("x").add(10).subtract($("y")))
        .apply('addition2', $("x").add(1))
        .apply('multiplication1', $("x").multiply(10).divide($("y")))
        .apply('multiplication2', $("x").multiply($('y')))
        .apply('negate1', $("x").negate())
        .apply('negate2', $("x").negate())
        .apply('length', $("x").length())
        .apply('abs', $("x").absolute())
        .apply('absolute', $("x").absolute())
        .apply('absolute_pipes', $("x").absolute())
        .apply('absolute_pipes2', r(-2).absolute())
        .apply('x_power_of_22', $("x").power(22))
        .apply('x_power_of_2', $("x").power(2))
        .apply('x_power_of_2_nosp', $("x").power(2))
        .apply('sqrt', $("x").power(0.5))
        .apply('power_respects_orderOfOperations', $("x").power(r(3).power(4)))
        .apply('identity', $("x"))
        .apply('string_set', r(['a', 'b', 'c']))
        .apply('number_set', r([1, 2, 3]))
        .apply('in_string_set', $("x").in(['a', 'b', 'c']))
        .apply('in_number_set', $("x").in([1, 2, 3]))
        .apply('in_number_range', $("x").in(1, 2))
        .apply('in_date_range', $("x").in(["2015-03-03Z", "2015-10-10Z"]))
        .apply('overlap_set', $("x").overlap(['a', 'b', 'c']))
        .apply('substr', $("x").substr(1, 5))
        .apply('upper', $("x").transformCase('upperCase'))
        .apply('lower', $("x").transformCase('lowerCase'))
        .apply('extract', $("x").extract('_([^_]+)_'))
        .apply('lookup', $("x").lookup('some_lookup'))
        .apply('another_lookup', $("x").lookup('another_lookup'))
        .apply('extract_fallback', $("x").extract('_([^_]+)_').fallback('none'))
        .apply('lookup_fallback', $("x").lookup('some_lookup').fallback('none'))
        .apply('agg_count', $("data").count())
        .apply('agg_sum', $("data").sum($('price')))
        .apply('agg_average', $("data").average($('price')))
        .apply('agg_min', $("data").min($('price')))
        .apply('agg_max', $("data").max($('price')))
        .apply('agg_quantile', $("data").quantile($('price'), 0.5))
        .apply('custom_transform', $('city').customTransform("myExtractionFn"))
        .apply('agg_custom', $("data").customAggregate('blah'))
        .apply('agg_customAggregate', $("data").customAggregate('blah'))
        .apply('agg_split', $("data").split($('carat'), 'Carat'))
        .apply('agg_filter_count', $("data").filter($('country').is("USA")).count())
        .apply('time_bucket', $("time").timeBucket("P1D"))
        .apply('time_bucket_timezone', $("time").timeBucket("P1D", 'America/Los_Angeles'))
        .apply('time_floor', $("time").timeFloor("P1D"))
        .apply('time_floor_timezone', $("time").timeFloor("P1D", 'America/Los_Angeles'))
        .apply('time_part', $("time").timePart("DAY_OF_WEEK"))
        .apply('time_part_timezone', $("time").timePart("DAY_OF_WEEK", 'America/Los_Angeles'))
        .apply('time_shift', $("time").timeShift("P1D", -3))
        .apply('time_shift_timezone', $("time").timeShift("P1D", -3, 'America/Los_Angeles'))
        .apply('time_range', $("time").timeRange("P1D", -3))
        .apply('time_range_timezone', $("time").timeRange("P1D", -3, 'America/Los_Angeles'));

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with pure JSON", () => {
      let ex1 = Expression.parse('{ "op": "ref", "name": "authors" }');
      let ex2 = $('authors');

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });

    it("works with a fancy number expression", () => {
      let ex1 = Expression.parse(" -5e-2 ");
      let ex2 = r(-5e-2);

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should not get confused with parsable strings in strange places", () => {
      let ex1 = Expression.parse("ply().apply('$x + 1', $x +1)");
      let ex2 = ply().apply('$x + 1', $("x").add(1));

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with fancy ref name type", () => {
      let ex1 = Expression.parse("${!T_0}");
      let ex2 = $("!T_0");

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with NUMBER type", () => {
      let ex1 = Expression.parse("$x:NUMBER");
      let ex2 = $("x", "NUMBER");

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SET/STRING type", () => {
      let ex1 = Expression.parse("$tags:SET/STRING");
      let ex2 = $("tags", "SET/STRING");

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with SET/STRING type within IN", () => {
      let ex1 = Expression.parse("$tag.in($tags:SET/STRING)");
      let ex2 = $('tag').in($("tags", "SET/STRING"));

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should handle --", () => {
      let ex1 = Expression.parse("$x--3");
      let ex2 = $("x").subtract(-3);

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with lots of keywords 1", () => {
      let ex1 = Expression.parse('$y and true and $z');
      let ex2 = $('y').and(r(true), $('z'));

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with lots of keywords 2", () => {
      let ex1 = Expression.parse('true and $y and true and $z');
      let ex2 = r(true).and($('y'), r(true), $('z'));

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should work with : in is", () => {
      let ex1 = Expression.parse('$x.is(":hello")');
      let ex2 = $('x').is(r(":hello"));

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a whole expression", () => {
      let ex1 = Expression.parse(`ply()
  .apply(num, 5)
  .apply(subData,
    ply()
      .apply(x, $num + 1)
      .apply(y, $foo * 2)
  )`);

      let ex2 = ply()
        .apply('num', 5)
        .apply(
          'subData',
          ply()
            .apply('x', '$num + 1')
            .apply('y', '$foo * 2')
        );

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should parse a whole complex expression", () => {
      let ex1 = Expression.parse(`ply()
  .apply(wiki, $wiki.filter($language == 'en'))
  .apply(Count, $wiki.sum($count))
  .apply(TotalAdded, $wiki.sum($added))
  .apply(Pages,
    $wiki.split($page, Page)
      .apply(Count, $wiki.sum($count))
      .sort($Count, descending)
      .limit(2)
      .apply(TimeBucket,
        $wiki.split($time.timeBucket(PT1H, 'Etc/UTC'), Timestamp)
          .apply(TotalAdded, $wiki.sum($added))
          .sort($TotalAdded, descending)
          .limit(3)
      )
      .apply(TimePart,
        $wiki.split($time.timePart(DAY_OF_YEAR, 'Etc/UTC'), Timestamp)
          .apply(TotalAdded, $wiki.sum($added))
          .sort($TotalAdded, descending)
          .limit(3)
      )
  )`);

      let ex2 = ply()
        .apply("wiki", $('wiki').filter($("language").is('en')))
        .apply('Count', '$wiki.sum($count)')
        .apply('TotalAdded', '$wiki.sum($added)')
        .apply(
          'Pages',
          $("wiki").split("$page", 'Page')
            .apply('Count', '$wiki.sum($count)')
            .sort('$Count', 'descending')
            .limit(2)
            .apply(
              'TimeBucket',
              $("wiki").split($("time").timeBucket('PT1H', 'Etc/UTC'), 'Timestamp')
                .apply('TotalAdded', '$wiki.sum($added)')
                .sort('$TotalAdded', 'descending')
                .limit(3)
            )
            .apply(
              'TimePart',
              $("wiki").split($("time").timePart('DAY_OF_YEAR', 'Etc/UTC'), 'Timestamp')
                .apply('TotalAdded', '$wiki.sum($added)')
                .sort('$TotalAdded', 'descending')
                .limit(3)
            )
        );

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });

    it("should complain on identity misuse (on non numbers)", () => {
      expect(() => {
        Expression.parse("+'poo'");
      }).to.throw("Expression parse error: subtract must have expression of type NUMBER (is STRING) on \'+\'poo\'\'");
    });

    it("should parse leading number in param", () => {
      let ex1 = Expression.parse('$data.filter(1 != null)');
      let ex2 = $('data').filter(r(1).isnt(null));

      expect(ex1.toJS()).to.deep.equal(ex2.toJS());
    });
  });
});

