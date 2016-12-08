/*
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
let Q = require("q");
let { sane, grabConsoleWarn } = require('../utils');

let { testImmutableClass } = require("immutable-class-tester");

let plywood = require('../plywood');
let { Expression, Dataset, External, TimeRange, AttributeInfo, $, ply, r } = plywood;

let wikiDataset = External.fromJS({
  engine: 'druid',
  source: 'wikipedia',
  timeAttribute: 'time',
  allowSelectQueries: true,
  attributes: [
    { name: 'time', type: 'TIME' },
    { name: 'language', type: 'STRING' },
    { name: 'user', type: 'STRING' },
    { name: 'page', type: 'STRING' },
    { name: 'commentLength', type: 'NUMBER' },
    { name: 'added', type: 'NUMBER', unsplitable: true },
    { name: 'deleted', type: 'NUMBER', unsplitable: true }
  ],
  derivedAttributes: {
    language: "$language.substr(0, 100)",
    pageTm: "$page ++ '(TM)'"
  }
});

let context = {
  wiki: wikiDataset.addExpression(Expression._.filter($('time').in(TimeRange.fromJS({
    start: new Date("2013-02-26T00:00:00Z"),
    end: new Date("2013-02-27T00:00:00Z")
  })))),
  wikiCmp: wikiDataset.addExpression(Expression._.filter($('time').in(TimeRange.fromJS({
    start: new Date("2013-02-25T00:00:00Z"),
    end: new Date("2013-02-26T00:00:00Z")
  }))))
};

describe("External", () => {
  it("is immutable class", () => {
    testImmutableClass(External, [
      {
        engine: 'mysql',
        source: 'diamonds',
        attributes: [
          { name: 'time', type: 'TIME' },
          { name: 'color', type: 'STRING' },
          { name: 'cut', type: 'STRING' },
          { name: 'tags', type: 'SET/STRING' }
        ]
      },

      {
        engine: 'druid',
        source: 'moon_child',
        timeAttribute: 'time',
        attributes: [
          { name: 'color', type: 'STRING' },
          { name: 'cut', type: 'STRING' },
          { name: 'carat', type: 'STRING' },
          { name: 'price', type: 'NUMBER', unsplitable: true }
        ]
      },

      {
        engine: 'druid',
        version: '0.9.0',
        source: 'wiki',
        timeAttribute: 'time',
        allowEternity: true,
        allowSelectQueries: true,
        introspectionStrategy: 'datasource-get',
        exactResultsOnly: true,
        context: {
          timeout: 10000
        }
      },

      {
        engine: 'druid',
        version: '0.8.0',
        source: 'moon_child',
        timeAttribute: 'time',
        attributeOverrides: [
          { name: 'color', type: 'STRING' },
          { name: 'cut', type: 'STRING' },
          { name: 'unique', type: "STRING", special: 'unique' }
        ],
        customAggregations: {
          test: {
            aggregation: { type: 'longSum', fieldName: 'count' }
          }
        },
        customTransforms: {
          test: {
            extractionFn: { type: 'javascript', "function": "function(str) { return str + '!!!'; }" }
          }
        }
      },

      {
        engine: 'druid',
        version: '0.8.0',
        rollup: true,
        source: 'moon_child',
        timeAttribute: 'time',
        attributeOverrides: [
          { name: 'color', type: 'STRING' },
          { name: 'cut', type: 'STRING' },
          { name: 'unique', type: "STRING", special: 'unique' }
        ],
        customAggregations: {
          test: {
            aggregation: { type: 'longSum', fieldName: 'count' }
          }
        }
      },

      {
        engine: 'druid',
        version: '0.9.0',
        source: 'wiki',
        timeAttribute: 'time',
        derivedAttributes: {
          city3: $('city').substr(0, 3).toJS()
        }
      }
    ], {
      newThrows: true
    });
  });


  describe("does not die with hasOwnProperty", () => {
    it("survives a troll", () => {
      expect(External.fromJS({
        engine: 'druid',
        version: '0.8.2',
        source: 'wiki',
        hasOwnProperty: 'troll'
      }).toJS()).to.deep.equal({
        engine: 'druid',
        version: '0.8.2',
        source: 'wiki'
      });
    });

  });


  describe("constructor", () => {
    it("type checks", () => {
      expect(wikiDataset.derivedAttributes['language'].toString()).to.equals('$language:STRING.substr(0,100)');
      expect(wikiDataset.derivedAttributes['pageTm'].toString()).to.equals('$page:STRING.concat("(TM)")');
    });
  });


  describe("version check", () => {
    it("works with invalid version", () => {
      expect(() => {
        External.fromJS({
          engine: 'druid',
          version: 'koalas0.2.',
          source: 'wiki',
          timeAttribute: 'time'
        });
      }).to.throw('invalid version koalas0.2.');
    });

    it("works with version too low", () => {
      expect(() => {
        External.fromJS({
          engine: 'druid',
          version: '0.7.3',
          source: 'wiki',
          timeAttribute: 'time'
        });
      }).to.throw('only druid versions >= 0.8.0 are supported');
    });

  });


  describe("back compat", () => {
    it("druidVersion -> version", () => {
      expect(grabConsoleWarn(() => {
        expect(External.fromJS({
          engine: 'druid',
          druidVersion: '0.8.2',
          source: 'wiki'
        }).toJS()).to.deep.equal({
          engine: 'druid',
          version: '0.8.2',
          source: 'wiki'
        });
      })).to.equal("'druidVersion' parameter is deprecated, use 'version: 0.8.2' instead\n");
    });

    it("requester in JS", () => {
      expect(grabConsoleWarn(() => {
        let requester = () => null;
        let external = External.fromJS({
          engine: 'druid',
          source: 'wiki',
          requester
        });

        expect(external.requester).to.equal(requester);
      })).to.equal("'requester' parameter should be passed as context (2nd argument)\n");
    });

    it("dataSource -> source", () => {
      expect(External.fromJS({
        engine: 'druid',
        dataSource: 'wiki'
      }).toJS()).to.deep.equal({
        engine: 'druid',
        source: 'wiki'
      });
    });

    it("table -> source", () => {
      expect(External.fromJS({
        engine: 'mysql',
        table: 'wiki'
      }).toJS()).to.deep.equal({
        engine: 'mysql',
        source: 'wiki'
      });
    });

  });


  describe(".extractVersion", () => {
    it("works in null case", () => {
      expect(External.extractVersion(null)).to.equal(null);
    });

    it("works in basic case", () => {
      expect(External.extractVersion('0.8.1')).to.equal('0.8.1');
    });

    it("works in basic case 2", () => {
      expect(External.extractVersion('0.8.10')).to.equal('0.8.10');
    });

    it("works in extra stuff case", () => {
      expect(External.extractVersion('0.9.1-iap1')).to.equal('0.9.1-iap1');
    });

    it("works in multiple -s", () => {
      expect(External.extractVersion('0.9.1-iap1-legacy-lookups')).to.equal('0.9.1-iap1-legacy-lookups');
    });

    it("works in bad case", () => {
      expect(External.extractVersion('lol: 0.9.1-iap1')).to.equal(null);
    });

    it("works in extra numbers case", () => {
      expect(External.extractVersion('0.9.1.1')).to.equal('0.9.1');
    });

  });


  describe(".versionLessThan", () => {
    it("works in basic case", () => {
      expect(External.versionLessThan('0.0.0', '0.8.0')).to.equal(true);
    });

    it("works in basic case 2", () => {
      expect(External.versionLessThan('0.8.2', '0.9.1')).to.equal(true);
    });

    it("works in basic case 3", () => {
      expect(External.versionLessThan('0.8.2', '0.8.3')).to.equal(true);
    });

    it("works in basic case 3", () => {
      expect(External.versionLessThan('0.1.2', '0.10.2')).to.equal(true);
    });

    it("works with extra", () => {
      expect(External.versionLessThan('0.1.2-iap1', '0.10.2-iap3')).to.equal(true);
    });

    it("works in same inputs", () => {
      expect(External.versionLessThan('0.1.2', '0.1.2')).to.equal(false);
    });

    it("works in reverse inputs", () => {
      expect(External.versionLessThan('0.9.1', '0.8.2')).to.equal(false);
    });

  });


  describe("#introspect", () => {
    it("does two introspects", (testComplete) => {
      let dummyRequester = () => null;
      let external = External.fromJS({
        engine: 'druid',
        version: '0.9.0-yo',
        source: 'moon_child',
        attributeOverrides: [
          { "name": "unique_thing", "special": "unique", "type": "STRING" }
        ]
      }, dummyRequester);

      Q(external)
        .then((initExternal) => {
          initExternal.getIntrospectAttributes = () => {
            return Q(AttributeInfo.fromJSs([
              { name: 'color', type: 'STRING' },
              { name: 'cut', type: 'STRING' },
              { name: 'carat', type: 'STRING' },
              { name: 'unique_thing', type: 'NUMBER', unsplitable: true }
            ]));
          };

          return initExternal.introspect()
        })
        .then((introspectedExternal1) => {
          expect(introspectedExternal1.toJS()).to.deep.equal({
            engine: 'druid',
            version: '0.9.0-yo',
            source: 'moon_child',
            attributeOverrides: [
              { name: "unique_thing", special: "unique", type: "STRING" }
            ],
            attributes: [
              { name: 'color', type: 'STRING' },
              { name: 'cut', type: 'STRING' },
              { name: 'carat', type: 'STRING' },
              { name: "unique_thing", special: "unique", type: "STRING" }
            ]
          });
          return introspectedExternal1;
        })
        .then((introspectedExternal1) => {
          introspectedExternal1.getIntrospectAttributes = () => {
            return Q(AttributeInfo.fromJSs([
              // Color removed
              { name: 'cut', type: 'STRING' },
              { name: 'carat', type: 'STRING' },
              { name: 'price', type: 'NUMBER' },
              { name: 'unique_thing', type: 'NUMBER', unsplitable: true }
            ]));
          };

          return introspectedExternal1.introspect()
        })
        .then((introspectedExternal2) => {
          expect(introspectedExternal2.toJS()).to.deep.equal({
            engine: 'druid',
            version: '0.9.0-yo',
            source: 'moon_child',
            attributeOverrides: [
              { name: "unique_thing", special: "unique", type: "STRING" }
            ],
            attributes: [
              { name: 'color', type: 'STRING' },
              { name: 'cut', type: 'STRING' },
              { name: 'carat', type: 'STRING' },
              { name: "unique_thing", special: "unique", type: "STRING" },
              { name: 'price', type: 'NUMBER' }
            ]
          });
          testComplete();
        })
        .done();
    })

  });


  describe("#updateAttribute", () => {
    it("works", () => {
      let external = External.fromJS({
        engine: 'druid',
        source: 'moon_child',
        timeAttribute: 'time',
        attributes: [
          { name: 'color', type: 'STRING' },
          { name: 'cut', type: 'STRING' },
          { name: 'carat', type: 'STRING' },
          { name: 'unique_thing', type: 'NUMBER', unsplitable: true }
        ]
      });

      external = external.updateAttribute(AttributeInfo.fromJS({ name: 'unique_thing', special: 'unique' }));

      expect(external.toJS().attributes).to.deep.equal([
        { "name": "color", "type": "STRING" },
        { "name": "cut", "type": "STRING" },
        { "name": "carat", "type": "STRING" },
        { "name": "unique_thing", "special": "unique", "type": "STRING" }
      ]);
    });
  });


  describe(".normalizeAndAddApply", () => {
    let attributesAndApplies = {
      attributes: AttributeInfo.fromJSs([
        { name: 'Count', type: 'NUMBER' },
        { name: 'Added', type: 'NUMBER' },
        { name: 'Volatile', type: 'NUMBER' }
      ]),
      applies: [
        Expression._.apply('Count', '$D.count()'),
        Expression._.apply('Added', '$D.sum($added)'),
        Expression._.apply('Volatile', '$D.max($added) - $D.min($deleted)')
      ]
    };

    it("works in noop case", () => {
      let nextApply = Expression._.apply('Deleted', '$D.sum($deleted)');
      let added = External.normalizeAndAddApply(attributesAndApplies, nextApply);

      expect(added.attributes).to.have.length(4);
      expect(added.applies).to.have.length(4);
      expect(added.applies[3] === nextApply).to.equal(true);
    });

    it("works in simple case", () => {
      let nextApply = Expression._.apply('AddedMinusDeleted', '$Added - $D.sum($deleted)');
      let added = External.normalizeAndAddApply(attributesAndApplies, nextApply);

      expect(added.attributes.join('\n')).to.equal(sane`
        Count::NUMBER
        Added::NUMBER
        Volatile::NUMBER
        AddedMinusDeleted::NUMBER
      `);

      expect(added.applies.join('\n')).to.equal(sane`
        $_.apply(Count,$D.count())
        $_.apply(Added,$D.sum($added))
        $_.apply(Volatile,$D.max($added).subtract($D.min($deleted)))
        $_.apply(AddedMinusDeleted,$D.sum($added).subtract($D.sum($deleted)))
      `);
    });

    it("works in redefine case", () => {
      let nextApply = Expression._.apply('Volatile', '$Added - $D.sum($deleted)');
      let added = External.normalizeAndAddApply(attributesAndApplies, nextApply);

      expect(added.attributes.join('\n')).to.equal(sane`
        Count::NUMBER
        Added::NUMBER
        Volatile::NUMBER
      `);

      expect(added.applies.join('\n')).to.equal(sane`
        $_.apply(Count,$D.count())
        $_.apply(Added,$D.sum($added))
        $_.apply(Volatile,$D.sum($added).subtract($D.sum($deleted)))
      `);
    });
  });


  describe(".segregationAggregateApplies", () => {
    it("breaks up correctly in simple case", () => {
      let { aggregateApplies, postAggregateApplies } = External.segregationAggregateApplies([
        Expression._.apply('Count', '$D.count()'),
        Expression._.apply('Added', '$D.sum($added)'),
        Expression._.apply('Volatile', '$D.max($added) - $D.min($deleted)')
      ]);

      expect(aggregateApplies.join('\n')).to.equal(sane`
        $_.apply(Count,$D.count())
        $_.apply(Added,$D.sum($added))
        $_.apply("!T_0",$D.max($added))
        $_.apply("!T_1",$D.min($deleted))
      `);

      expect(postAggregateApplies.join('\n')).to.equal(sane`
        $_.apply(Volatile,$\{!T_0}:NUMBER.subtract($\{!T_1}:NUMBER))
      `);
    });

    it("breaks up correctly in case of duplicate name", () => {
      let { aggregateApplies, postAggregateApplies } = External.segregationAggregateApplies([
        Expression._.apply('Count', '$D.count()'),
        Expression._.apply('Added', '$D.sum($added)'),
        Expression._.apply('Volatile', '$D.sum($added) - $D.sum($deleted)')
      ]);

      expect(aggregateApplies.join('\n')).to.equal(sane`
        $_.apply(Count,$D.count())
        $_.apply(Added,$D.sum($added))
        $_.apply("!T_0",$D.sum($deleted))
      `);

      expect(postAggregateApplies.join('\n')).to.equal(sane`
        $_.apply(Volatile,$Added:NUMBER.subtract($\{!T_0}:NUMBER))
      `);
    });

    it("breaks up correctly in case of variable reference", () => {
      let { aggregateApplies, postAggregateApplies } = External.segregationAggregateApplies([
        Expression._.apply('Count', '$D.count()'),
        Expression._.apply('Added', '$D.sum($added)'),
        Expression._.apply('Volatile', '$Added - $D.sum($deleted)')
      ]);

      expect(aggregateApplies.join('\n')).to.equal(sane`
        $_.apply(Count,$D.count())
        $_.apply(Added,$D.sum($added))
        $_.apply("!T_0",$D.sum($deleted))
      `);

      expect(postAggregateApplies.join('\n')).to.equal(sane`
        $_.apply(Volatile,$Added.subtract($\{!T_0}:NUMBER))
      `);
    });

    it("breaks up correctly in complex case", () => {
      let { aggregateApplies, postAggregateApplies } = External.segregationAggregateApplies([
        Expression._.apply('AddedByDeleted', '$D.sum($added) / $D.sum($deleted)'),
        Expression._.apply('DeletedByInserted', '$D.sum($deleted) / $D.sum($inserted)'),
        Expression._.apply('Deleted', '$D.sum($deleted)')
      ]);

      expect(aggregateApplies.join('\n')).to.equal(sane`
        $_.apply(Deleted,$D.sum($deleted))
        $_.apply("!T_0",$D.sum($added))
        $_.apply("!T_1",$D.sum($inserted))
      `);

      expect(postAggregateApplies.join('\n')).to.equal(sane`
        $_.apply(AddedByDeleted,$\{!T_0}:NUMBER.divide($Deleted:NUMBER))
        $_.apply(DeletedByInserted,$Deleted:NUMBER.divide($\{!T_1}:NUMBER))
      `);
    });
  });


  describe("#hasAttribute", () => {
    let rawExternal = External.fromJS({
      engine: 'druid',
      source: 'moon_child',
      timeAttribute: 'time',
      attributes: [
        { name: 'page', type: 'STRING' },
        { name: 'added', type: 'NUMBER', unsplitable: true }
      ],
      derivedAttributes: {
        'addedX2': '$added * 2'
      }
    });

    it('works in raw mode', () => {
      expect(rawExternal.hasAttribute('page')).to.equal(true);
      expect(rawExternal.hasAttribute('added')).to.equal(true);
      expect(rawExternal.hasAttribute('addedX2')).to.equal(true);
      expect(rawExternal.hasAttribute('moon')).to.equal(false);
    });

  });


  describe("#addExpression / #getRaw", () => {
    let rawExternal = External.fromJS({
      engine: 'druid',
      source: 'moon_child',
      timeAttribute: 'time',
      attributes: [
        { name: 'page', type: 'STRING' },
        { name: 'added', type: 'NUMBER', unsplitable: true }
      ]
    });

    it('runs through a life cycle', () => {
      let filteredRawExternal = rawExternal.addExpression(Expression._.filter($('page').contains('lol')));

      let filteredValueExternal = filteredRawExternal.addExpression(Expression._.sum('$added:NUMBER'));

      expect(filteredValueExternal.mode).to.equal('value');
      expect(filteredValueExternal.valueExpression.toString()).to.equal('$__SEGMENT__:DATASET.sum($added:NUMBER)');

      let filteredRawExternal2 = filteredValueExternal.getRaw();
      expect(filteredRawExternal2.equals(filteredRawExternal)).to.equal(true);

      let rawExternal2 = filteredValueExternal.getBase();
      expect(rawExternal2.equals(rawExternal)).to.equal(true);
    });

    it('it checks that expressions are internally defined (filter raw)', () => {
      expect(rawExternal.addExpression(Expression._.filter('$user:STRING.contains("lol")'))).to.equal(null);
      expect(rawExternal.addExpression(Expression._.filter('$page:STRING.contains("lol")'))).to.not.equal(null);
    });

    it('it checks that expressions are internally defined (filter split)', () => {
      let splitExternal = rawExternal.addExpression(Expression._.split('$page:STRING', 'Page', 'blah'));
      expect(splitExternal.addExpression(Expression._.filter('$User:STRING.contains("lol")'))).to.equal(null);
      expect(splitExternal.addExpression(Expression._.filter('$Page:STRING.contains("lol")'))).to.not.equal(null);
    });

    it('it checks that expressions are internally defined (split)', () => {
      expect(rawExternal.addExpression(Expression._.split('$user:STRING', 'User', 'blah'))).to.equal(null);
      expect(rawExternal.addExpression(Expression._.split('$page:STRING', 'Page', 'blah'))).to.not.equal(null);
    });

    it('it checks that expressions are internally defined (apply on raw)', () => {
      expect(rawExternal.addExpression(Expression._.apply('DeltaPlusOne', '$delta:NUMBER + 1'))).to.equal(null);
      expect(rawExternal.addExpression(Expression._.apply('AddedPlusOne', '$added:NUMBER + 1'))).to.not.equal(null);
    });

    it('it checks that expressions are internally defined (apply on split)', () => {
      let splitExternal = rawExternal.addExpression(Expression._.split('$page:STRING', 'Page', 'blah'));
      expect(splitExternal.addExpression(Expression._.apply('DeltaPlusOne', '$blah.sum($delta:NUMBER)'))).to.equal(null);
      expect(splitExternal.addExpression(Expression._.apply('AddedPlusOne', '$blah.sum($added:NUMBER)'))).to.not.equal(null);
    });

    it('it checks that expressions are internally defined (value / aggregate)', () => {
      expect(rawExternal.addExpression(Expression._.sum('$delta:NUMBER'))).to.equal(null);
      expect(rawExternal.addExpression(Expression._.sum('$added:NUMBER'))).to.not.equal(null);
    });

    it('it checks that expressions are internally defined (select)', () => {
      //expect(rawExternal.addExpression(Expression._.select('user'))).to.equal(null);
      expect(rawExternal.addExpression(Expression._.select('page'))).to.not.equal(null);
    });

  });


  describe("#bucketsConcealed", () => {
    let bucketedExternal = External.fromJS({
      engine: 'druid',
      source: 'wikipedia',
      timeAttribute: 'time',
      attributes: [
        { name: 'time', type: 'TIME', maker: { action: 'timeFloor', duration: 'PT1H', timezone: 'Etc/UTC' } },
        { name: 'language', type: 'STRING' }
      ]
    });

    it('accepts', () => {
      let exs = [
        $('time').timeFloor('PT1H', 'Etc/UTC'),
        $('time').timeFloor('PT2H', 'Etc/UTC'),
        $('time').timeFloor('P1D', 'Etc/UTC'),
        $('time').timeBucket('P1D', 'Etc/UTC'),
        $('language').is('en').and($('time').timeFloor('PT1H', 'Etc/UTC').is('$blah')),
        $('time').in(new Date('2016-09-01T01:00:00Z'), new Date('2016-09-02T01:00:00Z'))
      ];

      for (let ex of exs) {
        expect(bucketedExternal.bucketsConcealed(ex), ex.toString()).to.equal(true);
      }
    });

    it('rejects', () => {
      let exs = [
        $('time'),
        $('time').timeFloor('PT1H'),
        $('time').timeFloor('PT1M', 'Etc/UTC'),
        $('time').timeFloor('PT1S', 'Etc/UTC'),
        $('language').is('en').and($('time').timeFloor('PT1M', 'Etc/UTC').is('$blah')),
        $('time').in(new Date('2016-09-01T01:00:00Z'), new Date('2016-09-02T01:00:01Z'))
      ];

      for (let ex of exs) {
        expect(bucketedExternal.bucketsConcealed(ex), ex.toString()).to.equal(false);
      }
    });

  });


  describe("simplifies / digests", () => {

    describe("raw mode", () => {
      it("works in basic raw mode", () => {
        let ex = $('wiki');

        ex = ex.referenceCheck(context).resolve(context).simplify();
        expect(ex.op).to.equal('external');
      });

      it("works with a simple select", () => {
        let ex = $('wiki').select('time', 'language', 'added');

        ex = ex.referenceCheck(context).resolve(context).simplify();
        expect(ex.op).to.equal('external');
        let externalDataset = ex.external;

        expect(externalDataset.select.attributes).to.deep.equal(['time', 'language', 'added']);
      });

      it("works with a derived attribute and a filter", () => {
        let ex = $('wiki')
          .apply('addedTwice', '$added * 2')
          .filter($("language").is('en'));

        ex = ex.referenceCheck(context).resolve(context).simplify();
        expect(ex.op).to.equal('external');
        let externalDataset = ex.external;

        expect(externalDataset.derivedAttributes).to.have.all.keys(['addedTwice', 'language', 'pageTm']);

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z]).and($language:STRING.is("en"))
        `);
      });

      it("works with a sort and a limit", () => {
        let ex = $('wiki')
          .sort('$time')
          .limit(10);

        ex = ex.referenceCheck(context).resolve(context).simplify();
        expect(ex.op).to.equal('external');
        let externalDataset = ex.external;

        expect(externalDataset.sort.toString()).to.equal('$_.sort($time:TIME,ascending)');
        expect(externalDataset.limit.toString()).to.equal('$_.limit(10)');
      });

      it("works with a sort and a limit where there is also an aggregate", () => {
        let ex = $('wiki')
          .sort('$time')
          .limit(10)
          .count();

        ex = ex.referenceCheck(context).resolve(context).simplify();
        expect(ex.op).to.equal('count');
        let externalDataset = ex.operand.external;

        expect(externalDataset.sort.toString()).to.equal('$_.sort($time:TIME,ascending)');
        expect(externalDataset.limit.toString()).to.equal('$_.limit(10)');
      });

    });


    describe("value mode", () => {
      it("works with a basic aggregate", () => {
        let ex = $('wiki').sum('$added');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        let externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z])
        `);

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          $__SEGMENT__:DATASET.sum($added:NUMBER)
        `);

        expect(externalDataset.simulateValue(true, [])).to.equal(4);
      });

      it("works with a filter and aggregate", () => {
        let ex = $('wiki').filter('$page == USA').sum('$added');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        let externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z]).and($page:STRING.is("USA"))
        `);

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          $__SEGMENT__:DATASET.sum($added:NUMBER)
        `);
      });

      it("works with aggregate that has a simple post process", () => {
        let ex = $('wiki').filter('$page == USA').sum('$added').multiply(2);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        let externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z]).and($page:STRING.is("USA"))
        `);

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          $__SEGMENT__:DATASET.sum($added:NUMBER).multiply(2)
        `);
      });

      it("works with aggregate that has an expressionless post process", () => {
        let ex = $('wiki').filter('$page == USA').sum('$added').absolute();

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        let externalDataset = ex.external;

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          $__SEGMENT__:DATASET.sum($added:NUMBER).absolute()
        `);
      });

      it("works with aggregate that has a complex post process", () => {
        let ex = $('wiki').filter('$page == USA').sum('$added').add($('wiki').sum('$deleted'));

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        let externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z])
        `);

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          $__SEGMENT__:DATASET.filter($page:STRING.is("USA")).sum($added:NUMBER).add($__SEGMENT__:DATASET.sum($deleted:NUMBER))
        `);
      });

      it("works with aggregate that has a LHS post process", () => {
        let ex = r(5).subtract($('wiki').filter('$page == USA').sum('$added'));

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        let externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z]).and($page:STRING.is("USA"))
        `);

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          5.subtract($__SEGMENT__:DATASET.sum($added:NUMBER))
        `);
      });

      it("works with aggregate that has LHS and RHS post process", () => {
        let ex = r(5).subtract($('wiki').filter('$page == USA').sum('$added'), $('wiki').count());

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        let externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z])
        `);

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          5.subtract($__SEGMENT__:DATASET.filter($page:STRING.is("USA")).sum($added:NUMBER)).subtract($__SEGMENT__:DATASET.count())
        `);
      });

    });


    describe("total mode", () => {
      it("works with a single apply", () => {
        let ex = ply()
          .apply('TotalAdded', '$wiki.sum($added)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('literal');
        let externalDataset = ex.value.getReadyExternals()[0].external;

        expect(externalDataset.mode).to.equal('value');
        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z])
        `);

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          $__SEGMENT__:DATASET.sum($added:NUMBER)
        `);
      });

      it("works with a multiple applies", () => {
        let ex = ply()
          .apply('Count', '$wiki.count()')
          .apply('TotalAdded', '$wiki.sum($added)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('literal');
        let externalDataset = ex.value.getReadyExternals()[0].external;

        expect(externalDataset.mode).to.equal('total');
        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z])
        `);

        expect(externalDataset.applies.join('\n')).to.equal(sane`
          $_.apply(Count,$__SEGMENT__:DATASET.count())
          $_.apply(TotalAdded,$__SEGMENT__:DATASET.sum($added:NUMBER))
        `);
      });

      it("works with a multiple applies with a divide", () => {
        let ex = ply()
          .apply('Count', '$wiki.count()')
          .apply('TotalAdded', '$wiki.sum($added)')
          .apply('CountPlusAdded', '$Count + $TotalAdded');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('literal');
        let externalDataset = ex.value.getReadyExternals()[0].external;

        expect(externalDataset.mode).to.equal('total');
        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z])
        `);

        expect(externalDataset.applies.join('\n')).to.equal(sane`
          $_.apply(Count,$__SEGMENT__:DATASET.count())
          $_.apply(TotalAdded,$__SEGMENT__:DATASET.sum($added:NUMBER))
          $_.apply(CountPlusAdded,$__SEGMENT__:DATASET.count().add($__SEGMENT__:DATASET.sum($added:NUMBER)))
        `);
      });

      it("works with several applies, some filtered", () => {
        let ex = ply()
          .apply('Count', '$wiki.count()')
          .apply('TotalAdded', '$wiki.sum($added)')
          .apply('TotalUSA', '$wiki.filter($page == USA).sum($added)')
          .apply('TotalUK', '$wiki.filter($page == UK).sum($added)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('literal');
        let externalDataset = ex.value.getReadyExternals()[0].external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z])
        `);

        expect(externalDataset.applies.join('\n')).to.equal(sane`
          $_.apply(Count,$__SEGMENT__:DATASET.count())
          $_.apply(TotalAdded,$__SEGMENT__:DATASET.sum($added:NUMBER))
          $_.apply(TotalUSA,$__SEGMENT__:DATASET.filter($page:STRING.is("USA")).sum($added:NUMBER))
          $_.apply(TotalUK,$__SEGMENT__:DATASET.filter($page:STRING.is("UK")).sum($added:NUMBER))
        `);

        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Count", "type": "NUMBER" },
          { name: "TotalAdded", "type": "NUMBER" },
          { name: "TotalUSA", "type": "NUMBER" },
          { name: "TotalUK", "type": "NUMBER" }
        ]);

        expect(externalDataset.simulateValue(true, []).toJS()).to.deep.equal({
          datum: {
            "Count": 4,
            "TotalAdded": 4,
            "TotalUSA": 4,
            "TotalUK": 4
          }
        });
      });

      it("works with several applies, all filtered", () => {
        let ex = ply()
          .apply('TotalUSA', '$wiki.filter($page == USA).sum($added)')
          .apply('TotalUK', '$wiki.filter($page == UK).sum($added)')
          .apply('TotalIndia', '$wiki.filter($page == India).sum($added)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('literal');
        let externalDataset = ex.value.getReadyExternals()[0].external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z])
        `);

        expect(externalDataset.applies.join('\n')).to.equal(sane`
          $_.apply(TotalUSA,$__SEGMENT__:DATASET.filter($page:STRING.is("USA")).sum($added:NUMBER))
          $_.apply(TotalUK,$__SEGMENT__:DATASET.filter($page:STRING.is("UK")).sum($added:NUMBER))
          $_.apply(TotalIndia,$__SEGMENT__:DATASET.filter($page:STRING.is("India")).sum($added:NUMBER))
        `);

        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "TotalUSA", "type": "NUMBER" },
          { name: "TotalUK", "type": "NUMBER" },
          { name: "TotalIndia", "type": "NUMBER" }
        ]);
      });

      it("works with a filter and applies", () => {
        let ex = ply()
          .apply("wiki", $('wiki').filter("$language == 'en'"))
          .apply('Count', '$wiki.count()')
          .apply('TotalAdded', '$wiki.sum($added)')
          .apply('TotalUSA', '$wiki.filter($page == USA).sum($added)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('literal');
        let externalDataset = ex.value.getReadyExternals()[0].external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z]).and($language:STRING.is("en"))
        `);

        expect(externalDataset.applies.join('\n')).to.equal(sane`
          $_.apply(Count,$__SEGMENT__:DATASET.count())
          $_.apply(TotalAdded,$__SEGMENT__:DATASET.sum($added:NUMBER))
          $_.apply(TotalUSA,$__SEGMENT__:DATASET.filter($page:STRING.is("USA")).sum($added:NUMBER))
        `);

        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Count", "type": "NUMBER" },
          { name: "TotalAdded", "type": "NUMBER" },
          { name: "TotalUSA", "type": "NUMBER" }
        ]);
      });

      it("works with a filter and many applies and re-define", () => {
        let ex = ply()
          .apply('Count', '$wiki.count()')
          .apply("wiki", $('wiki').apply('addedTwice', '$added * 2'))
          .apply('TotalAdded', '$wiki.sum($added)')
          .apply("wiki", $('wiki').filter("$language == 'en'"))
          .apply('TotalEnAdded', '$wiki.sum($added)')
          .apply("wiki_alt", $('wiki').filter("$page == USA"))
          .apply('TotalUsAdded', '$wiki_alt.sum($added)')
          .apply('OrigMinAdded', '$^wiki.min($added)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('literal');
        let externalDataset = ex.value.getReadyExternals()[0].external;

        expect(externalDataset.derivedAttributes).to.have.all.keys(['addedTwice', 'language', 'pageTm']);

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z])
        `);

        expect(externalDataset.applies.join('\n')).to.equal(sane`
          $_.apply(Count,$__SEGMENT__:DATASET.count())
          $_.apply(TotalAdded,$__SEGMENT__:DATASET.sum($added:NUMBER))
          $_.apply(TotalEnAdded,$__SEGMENT__:DATASET.filter($language:STRING.is("en")).sum($added:NUMBER))
          $_.apply(TotalUsAdded,$__SEGMENT__:DATASET.filter($language:STRING.is("en").and($page:STRING.is("USA"))).sum($added:NUMBER))
          $_.apply(OrigMinAdded,$__SEGMENT__:DATASET.min($added:NUMBER))
        `);

        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Count", "type": "NUMBER" },
          { name: "TotalAdded", "type": "NUMBER" },
          { name: "TotalEnAdded", "type": "NUMBER" },
          { name: "TotalUsAdded", "type": "NUMBER" },
          { name: "OrigMinAdded", "type": "NUMBER" }
        ]);
      });

      it("works with fancy applies", () => {
        let ex = ply()
          .apply("wiki", $('wiki').filter("$language == 'en'"))
          .apply("Five", 5)
          .apply('CountX3', '$wiki.count() * 3')
          .apply('AddedPlusDeleted', '$wiki.sum($added) + $wiki.sum($deleted)')
          .apply("Six", 6)
          .apply('AddedUsPlusDeleted', '$wiki.filter($page == USA).sum($added) + $wiki.sum($deleted)');
          //.apply('CountX3Plus5', '$CountX3 + 5');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('literal');
        expect(ex.value.data[0]['Five']).to.equal(5);
        expect(ex.value.data[0]['Six']).to.equal(6);
        let externalDataset = ex.value.getReadyExternals()[0].external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time:TIME.in([2013-02-26T00:00:00Z,2013-02-27T00:00:00Z]).and($language:STRING.is("en"))
        `);

        expect(externalDataset.applies.join('\n')).to.equal(sane`
          $_.apply(CountX3,$__SEGMENT__:DATASET.count().multiply(3))
          $_.apply(AddedPlusDeleted,$__SEGMENT__:DATASET.sum($added:NUMBER).add($__SEGMENT__:DATASET.sum($deleted:NUMBER)))
          $_.apply(AddedUsPlusDeleted,$__SEGMENT__:DATASET.filter($page:STRING.is("USA")).sum($added:NUMBER).add($__SEGMENT__:DATASET.sum($deleted:NUMBER)))
        `);
        //apply(CountX3Plus5,$__SEGMENT__:DATASET.count().multiply(3).add(5))

        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "CountX3", "type": "NUMBER" },
          { name: "AddedPlusDeleted", "type": "NUMBER" },
          { name: "AddedUsPlusDeleted", "type": "NUMBER" },
          //{ name: "CountX3Plus5", "type": "NUMBER" }
        ]);
      });

    });


    describe("split mode", () => {
      it("works with a split on string", () => {
        let ex = $('wiki').split("$page", 'Page')
          .apply('Count', '$wiki.count()')
          .apply('Added', '$wiki.sum($added)')
          .sort('$Count', 'descending')
          .limit(5);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        let externalDataset = ex.external;
        expect(externalDataset.applies).to.have.length(2);
        expect(externalDataset.limit.value).to.equal(5);
        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Page", "type": "STRING" },
          { name: "Count", "type": "NUMBER" },
          { name: "Added", "type": "NUMBER" }
        ]);

        expect(externalDataset.simulateValue(true, []).toJS()).to.deep.equal([
          {
            "Added": 4,
            "Count": 4,
            "Page": "some_page"
          }
        ]);
      });

      it("works with a split on string with multiple limits in ascending order", () => {
        let ex = $('wiki').split("$page", 'Page')
          .apply('Count', '$wiki.count()')
          .sort('$Count', 'descending')
          .limit(5)
          .apply('Added', '$wiki.sum($added)')
          .limit(9);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        let externalDataset = ex.external;
        expect(externalDataset.applies).to.have.length(2);
        expect(externalDataset.limit.value).to.equal(5);
        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Page", "type": "STRING" },
          { name: "Count", "type": "NUMBER" },
          { name: "Added", "type": "NUMBER" }
        ]);
      });

      it("works with a split on string with multiple limits in descending order", () => {
        let ex = $('wiki').split("$page", 'Page')
          .apply('Count', '$wiki.count()')
          .sort('$Count', 'descending')
          .limit(9)
          .apply('Added', '$wiki.sum($added)')
          .limit(5);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        let externalDataset = ex.external;
        expect(externalDataset.applies).to.have.length(2);
        expect(externalDataset.limit.value).to.equal(5);
        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Page", "type": "STRING" },
          { name: "Count", "type": "NUMBER" },
          { name: "Added", "type": "NUMBER" }
        ]);
      });

      it("works with a split on time", () => {
        let ex = $('wiki').split($("time").timeBucket('P1D', 'America/Los_Angeles'), 'Timestamp')
          .apply('Count', '$wiki.count()')
          .apply('Added', '$wiki.sum($added)')
          .sort('$Count', 'descending')
          .limit(5);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('limit');
        expect(ex.operand.op).to.equal('sort');
        let externalDataset = ex.operand.operand.external;
        expect(externalDataset.applies).to.have.length(2);
        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Timestamp", "type": "TIME_RANGE" },
          { name: "Count", "type": "NUMBER" },
          { name: "Added", "type": "NUMBER" }
        ]);

        expect(externalDataset.simulateValue(true, []).toJS()).to.deep.equal([
          {
            "Added": 4,
            "Count": 4,
            "Timestamp": {
              "start": new Date('2015-03-13T07:00:00Z'),
              "end": new Date('2015-03-14T07:00:00Z'),
              "type": "TIME_RANGE"
            }
          }
        ]);
      });

      it("works with a filtered split on string", () => {
        let ex = $('wiki').filter('$language == "en"').split("$page", 'Page')
          .apply('Count', '$wiki.count()')
          .apply('Added', '$wiki.sum($added)')
          .sort('$Count', 'descending')
          .limit(5);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        let externalDataset = ex.external;

        expect(
          externalDataset.filter.toJS()
        ).to.deep.equal(
          context.wiki.filter.and($("language", "STRING").is('en')).toJS()
        );

        expect(externalDataset.applies).to.have.length(2);
        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Page", "type": "STRING" },
          { name: "Count", "type": "NUMBER" },
          { name: "Added", "type": "NUMBER" }
        ]);

        expect(externalDataset.simulateValue(true, []).toJS()).to.deep.equal([
          {
            "Added": 4,
            "Count": 4,
            "Page": "some_page"
          }
        ]);
      });

    });


    describe("complex cases (multi mode)", () => {
      it("works with a total and a split", () => {
        let ex = ply()
          .apply(
            "wiki",
            $('wiki')
              .apply('addedTwice', '$added * 2')
              .filter($("language").is('en'))
          )
          .apply('Count', '$wiki.count()')
          .apply('TotalAdded', '$wiki.sum($added)')
          .apply(
            'Pages',
            $('wiki').split("$page", 'Page')
              .apply('Count', '$wiki.count()')
              .apply('Added', '$wiki.sum($added)')
              .sort('$Count', 'descending')
              .limit(5)
          );

        ex = ex.referenceCheck(context).resolve(context).simplify();

        let readyExternals = ex.value.getReadyExternals();
        expect(readyExternals.length).to.equal(2);

        let externalDataset0 = readyExternals[0].external;
        expect(externalDataset0.applies).to.have.length(2);
        expect(externalDataset0.toJS().attributes).to.deep.equal([
          { name: "Count", "type": "NUMBER" },
          { name: "TotalAdded", "type": "NUMBER" }
        ]);

        let externalDataset1 = readyExternals[1].external;
        expect(externalDataset1.applies).to.have.length(2);
        expect(externalDataset1.toJS().attributes).to.deep.equal([
          { name: "Page", "type": "STRING" },
          { name: "Count", "type": "NUMBER" },
          { name: "Added", "type": "NUMBER" }
        ]);
      });

      it("works with a blank total and a split", () => {
        let ex = ply()
          .apply("wiki", $('wiki').filter($("language").is('en')))
          .apply(
            'Pages',
            $('wiki').split("$page", 'Page')
              .apply('Count', '$wiki.count()')
              .apply('Added', '$wiki.sum($added)')
              .sort('$Count', 'descending')
              .limit(5)
          );

        ex = ex.referenceCheck(context).resolve(context).simplify();

        let readyExternals = ex.value.getReadyExternals();
        expect(readyExternals.length).to.equal(1);

        let externalDataset = readyExternals[0].external;
        expect(externalDataset.mode).to.equal('split');
      });

      it("works with a total and a split with a parent reference", () => {
        let ex = ply()
          .apply('Count', '$wiki.count()')
          .apply(
            'Pages',
            $('wiki').split("$page", 'Page')
              .apply('Added', '$wiki.sum($added)')
              .apply('AddedByTotal', '$Added / $^Count')
              .sort('$Added', 'descending')
              .limit(5)
          );

        ex = ex.referenceCheck(context).resolve(context).simplify();

        let readyExternals = ex.value.getReadyExternals();
        expect(readyExternals.length).to.equal(1);

        let externalDataset = readyExternals[0].external;
        expect(externalDataset.mode).to.equal('value');
      });

      it("works with up reference", () => {
        let ex = ply()
          .apply('Count', '$wiki.count()')
          .apply(
            'Pages',
            $('wiki').split("$page", 'Page')
              .apply('Count', $('wiki').count())
              .apply('PercentOfTotal', '$Count / $^Count')
          );

        ex = ex.referenceCheck(context).resolve(context).simplify();

        let readyExternals = ex.value.getReadyExternals();
        expect(readyExternals.length).to.equal(1);

        let externalDataset = readyExternals[0].external;
        expect(externalDataset.mode).to.equal('value');
      });

      it("works with a total and a split in a strange order", () => {
        let ex = ply()
          .apply(
            "wiki",
            $('wiki', 1)
              .apply('addedTwice', '$added * 2')
              .filter($("language").is('en'))
          )
          .apply('Count', '$wiki.count()')
          .apply(
            'Pages',
            $('wiki').split("$page", 'Page')
              .apply('Count', '$wiki.count()')
              .apply('Added', '$wiki.sum($added)')
              .sort('$Count', 'descending')
              .limit(5)
          )
          .apply('TotalAdded', '$wiki.sum($added)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        let readyExternals = ex.value.getReadyExternals();
        expect(readyExternals.length).to.equal(2);

        let externalDataset0 = readyExternals[0].external;
        expect(externalDataset0.mode).to.equal('total');
        expect(externalDataset0.toJS().attributes).to.deep.equal([
          { name: "Count", "type": "NUMBER" },
          { name: "TotalAdded", "type": "NUMBER" }
        ]);

        let externalDataset1 = readyExternals[1].external;
        expect(externalDataset1.mode).to.equal('split');
        expect(externalDataset1.toJS().attributes).to.deep.equal([
          { name: "Page", "type": "STRING" },
          { name: "Count", "type": "NUMBER" },
          { name: "Added", "type": "NUMBER" }
        ]);
      });

      it("works with a split and another split in a strange order", () => {
        let ex = $('wiki').split("$page", 'Page')
          .apply('Count', '$wiki.count()')
          .sort('$Count', 'descending')
          .apply(
            'Users',
            $('wiki').split("$user", 'User')
              .apply('Count', '$wiki.count()')
              .sort('$Count', 'descending')
              .limit(3)
          )
          .apply('Added', '$wiki.sum($added)')
          .limit(5);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        let readyExternals = ex.getReadyExternals();
        expect(Object.keys(readyExternals)).to.deep.equal(['1']);

        let externalDataset = readyExternals['1'].external;
        expect(externalDataset.applies).to.have.length(2);
        expect(externalDataset.limit.value).to.equal(5);
        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Page", "type": "STRING" },
          { name: "Count", "type": "NUMBER" },
          { name: "Added", "type": "NUMBER" }
        ]);
      });

      it.skip("a join of two splits", () => {
        let ex = $('wiki').split('$page', 'Page').join($('wikiCmp').split('$page', 'Page'))
          .apply('wiki', '$wiki.filter($page = $^Page)')
          .apply('wikiCmp', '$wikiCmp.filter($page = $^Page)')
          .apply('Count', '$wiki.count()')
          .apply('CountDiff', '$wiki.count() - $wikiCmp.count()')
          .sort('$CountDiff', 'descending')
          .limit(5);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('chain');
        expect(ex.operand.op).to.equal('join');

        let externalDatasetMain = ex.operand.lhs.value;
        expect(externalDatasetMain.applies).to.have.length(2);
        expect(externalDatasetMain.toJS().attributes).to.deep.equal([
          { name: "Count", "type": "NUMBER" },
          { name: "Page", "type": "STRING" },
          { name: "_br_0", "type": "NUMBER" }
        ]);

        let externalDatasetCmp = ex.operand.rhs.value;
        expect(externalDatasetCmp.applies).to.have.length(1);
        expect(externalDatasetCmp.toJS().attributes).to.deep.equal([
          { name: "Page", "type": "STRING" },
          { name: "_br_1", "type": "NUMBER" }
        ]);

        expect(ex.actions[0].toString()).to.equal('.apply(CountDiff, ($_br_0 + $_br_1))');
      });

      it("works with a split and further compute", () => {
        let ex = ply()
          .apply(
            'Pages',
            $('wiki').split("$page", 'Page')
              .apply('Count', '$wiki.count()')
              .apply('Added', '$wiki.sum($added)')
              .sort('$Count', 'descending')
              .limit(5)
          )
          .apply('MinCount', '$Pages.min($Count)')
          .apply('MaxAdded', '$Pages.max($Added)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        let readyExternals = ex.value.getReadyExternals();
        expect(readyExternals.length).to.equal(1);

        let externalDataset = readyExternals[0].external;
        expect(externalDataset.mode).to.equal('split');
      });

      it("works with a split and a further split", () => {
        let ex = $('wiki')
          .split({ 'user': '$user', 'page': '$page' })
          .apply('TotalAdded', '$wiki.sum($added)')
          .split('$user', 'user', 'data')
          .apply('SumTotalEdits', '$data.sum($TotalAdded)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        let readyExternals = ex.getReadyExternals();
        expect(Object.keys(readyExternals)).to.deep.equal(['2']);
        expect(readyExternals['2'].external.mode).to.deep.equal('split');
      });

    });

    describe("attribute order is respected in raw mode", () => {
      it("pure select: get selected attributes respects order", () => {
        let ex = $('wiki')
          .select("page", "language", "user")
          .limit(5);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        let external = ex.external;
        expect(external.getSelectedAttributes().map(a => a.name)).to.deep.equal([ 'page', 'language', 'user' ]);
      });

      it("pure select: dimension order reflects select order", () => {
        let ex = $('wiki')
          .select("page", "language", "user")
          .limit(5);

        ex = ex.referenceCheck(context).resolve(context).simplify();
        let external = ex.external;
        expect(external.getQueryAndPostProcess().query.dimensions).to.deep.equal([
          'page',
          {
            "dimension": "language",
            "extractionFn": {
              "index": 0,
              "length": 100,
              "type": "substring"
            },
            "outputName": "language",
            "type": "extraction"
          },
          'user'
        ]);
      });

    });

    describe("attribute order is respected in split mode", () => {
      it("get selected attributes respects order", () => {
        let ex = $('wiki')
          .split('$page', 'Page')
          .apply('Count', '$wiki.count()')
          .apply('Added', '$wiki.sum($added)')
          .select("Count", "Page", "Added");

        ex = ex.referenceCheck(context).resolve(context).simplify();

        let external = ex.external;
        expect(external.getSelectedAttributes().map(a => a.name)).to.deep.equal([ "Count", "Page", "Added" ]);
      });

      it("get selected attributes respects order with remove", () => {
        let ex = $('wiki')
          .split('$page', 'Page')
          .apply('Count', '$wiki.count()')
          .apply('Added', '$wiki.sum($added)')
          .select("Count", "Page");

        ex = ex.referenceCheck(context).resolve(context).simplify();

        let external = ex.external;
        expect(external.getQueryAndPostProcess().query.aggregations).to.deep.equal([
          {
            "name": "Count",
            "type": "count"
          }
        ]);
      });

    });

  });
});
