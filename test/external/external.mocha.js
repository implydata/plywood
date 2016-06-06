var { expect } = require("chai");
var Q = require("q");
var { sane, grabConsoleWarn } = require('../utils');

var { testImmutableClass } = require("immutable-class/build/tester");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { Expression, Dataset, External, TimeRange, AttributeInfo, $, ply, r } = plywood;

var wikiDataset = External.fromJS({
  engine: 'druid',
  dataSource: 'wikipedia',
  timeAttribute: 'time',
  attributes: [
    { name: 'time', type: 'TIME' },
    { name: 'language', type: 'STRING' },
    { name: 'user', type: 'STRING' },
    { name: 'page', type: 'STRING' },
    { name: 'added', type: 'NUMBER' },
    { name: 'deleted', type: 'NUMBER' }
  ]
});

var context = {
  wiki: wikiDataset.addFilter($('time').in(TimeRange.fromJS({
    start: new Date("2013-02-26T00:00:00Z"),
    end: new Date("2013-02-27T00:00:00Z")
  }))),
  wikiCmp: wikiDataset.addFilter($('time').in(TimeRange.fromJS({
    start: new Date("2013-02-25T00:00:00Z"),
    end: new Date("2013-02-26T00:00:00Z")
  })))
};

describe("External", () => {
  it("is immutable class", () => {
    testImmutableClass(External, [
      {
        engine: 'mysql',
        table: 'diamonds',
        attributes: [
          { name: 'time', type: 'TIME' },
          { name: 'color', type: 'STRING' },
          { name: 'cut', type: 'STRING' },
          { name: 'tags', type: 'SET/STRING' }
        ]
      },

      {
        engine: 'druid',
        dataSource: 'moon_child',
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
        dataSource: 'wiki',
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
        dataSource: 'moon_child',
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
        version: '0.8.0',
        rollup: true,
        dataSource: 'moon_child',
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
        dataSource: 'wiki',
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
        dataSource: 'wiki',
        hasOwnProperty: 'troll'
      }).toJS()).to.deep.equal({
        engine: 'druid',
        version: '0.8.2',
        dataSource: 'wiki'
      });
    });

  });


  describe("version check", () => {
    it("works with invalid version", () => {
      expect(() => {
        External.fromJS({
          engine: 'druid',
          version: 'koalas0.2.',
          dataSource: 'wiki',
          timeAttribute: 'time'
        });
      }).to.throw('invalid version koalas0.2.');
    });

    it("works with version too low", () => {
      expect(() => {
        External.fromJS({
          engine: 'druid',
          version: '0.7.3',
          dataSource: 'wiki',
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
          dataSource: 'wiki'
        }).toJS()).to.deep.equal({
          engine: 'druid',
          version: '0.8.2',
          dataSource: 'wiki'
        });
      })).to.equal("'druidVersion' parameter is deprecated, use 'version: 0.8.2' instead\n");
    });

    it("requester in JS", () => {
      expect(grabConsoleWarn(() => {
        var requester = () => null;
        var external = External.fromJS({
          engine: 'druid',
          dataSource: 'wiki',
          requester
        });

        expect(external.requester).to.equal(requester);
      })).to.equal("'requester' parameter should be passed as context (2nd argument)\n");
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

    it("works in bad case", () => {
      expect(External.extractVersion('lol: 0.9.1-iap1')).to.equal(null);
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
      var dummyRequester = () => null;
      var external = External.fromJS({
        engine: 'druid',
        dataSource: 'moon_child',
        attributeOverrides: [
          { "name": "unique_thing", "special": "unique", "type": "STRING" }
        ]
      }, dummyRequester);

      Q(external)
        .then((baseExternal) => {
          baseExternal.getIntrospectAttributes = () => {
            return Q({
              version: '0.9.0-yo',
              attributes: AttributeInfo.fromJSs([
                { name: 'color', type: 'STRING' },
                { name: 'cut', type: 'STRING' },
                { name: 'carat', type: 'STRING' },
                { name: 'unique_thing', type: 'NUMBER', unsplitable: true }
              ])
            });
          };

          return baseExternal.introspect()
        })
        .then((introspectedExternal1) => {
          expect(introspectedExternal1.toJS()).to.deep.equal({
            engine: 'druid',
            version: '0.9.0-yo',
            dataSource: 'moon_child',
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
            return Q({
              attributes: AttributeInfo.fromJSs([
                // Color removed
                { name: 'cut', type: 'STRING' },
                { name: 'carat', type: 'STRING' },
                { name: 'price', type: 'NUMBER' },
                { name: 'unique_thing', type: 'NUMBER', unsplitable: true }
              ])
            });
          };

          return introspectedExternal1.introspect()
        })
        .then((introspectedExternal2) => {
          expect(introspectedExternal2.toJS()).to.deep.equal({
            engine: 'druid',
            version: '0.9.0-yo',
            dataSource: 'moon_child',
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
      var external = External.fromJS({
        engine: 'druid',
        dataSource: 'moon_child',
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
    var ex = ply()
      .apply('Count', '$D.count()')
      .apply('Added', '$D.sum($added)')
      .apply('Volatile', '$D.max($added) - $D.min($deleted)');

    var attributesAndApplies = {
      attributes: AttributeInfo.fromJSs([
        { name: 'Count', type: 'NUMBER' },
        { name: 'Added', type: 'NUMBER' },
        { name: 'Volatile', type: 'NUMBER' }
      ]),
      applies: ex.actions
    };

    it("works in noop case", () => {
      var nextApply = ply()
        .apply('Deleted', '$D.sum($deleted)')
        .simplify()
        .actions[0];

      var added = External.normalizeAndAddApply(attributesAndApplies, nextApply);

      expect(added.attributes).to.have.length(4);
      expect(added.applies).to.have.length(4);
      expect(added.applies[3] === nextApply).to.equal(true);
    });

    it("works in simple case", () => {
      var nextApply = ply()
        .apply('AddedMinusDeleted', '$Added - $D.sum($deleted)')
        .actions[0];

      var added = External.normalizeAndAddApply(attributesAndApplies, nextApply);

      expect(added.attributes.join('\n')).to.equal(sane`
        Count::NUMBER
        Added::NUMBER
        Volatile::NUMBER
        AddedMinusDeleted::NUMBER
      `);

      expect(added.applies.join('\n')).to.equal(sane`
        apply(Count,$D.count())
        apply(Added,$D.sum($added))
        apply(Volatile,$D.max($added).subtract($D.min($deleted)))
        apply(AddedMinusDeleted,$D.sum($added).subtract($D.sum($deleted)))
      `);
    });

    it("works in redefine case", () => {
      var nextApply = ply()
        .apply('Volatile', '$Added - $D.sum($deleted)')
        .actions[0];

      var added = External.normalizeAndAddApply(attributesAndApplies, nextApply);

      expect(added.attributes.join('\n')).to.equal(sane`
        Count::NUMBER
        Added::NUMBER
        Volatile::NUMBER
      `);

      expect(added.applies.join('\n')).to.equal(sane`
        apply(Count,$D.count())
        apply(Added,$D.sum($added))
        apply(Volatile,$D.sum($added).subtract($D.sum($deleted)))
      `);
    });
  });


  describe(".segregationAggregateApplies", () => {
    it("breaks up correctly in simple case", () => {
      var ex = ply()
        .apply('Count', '$D.count()')
        .apply('Added', '$D.sum($added)')
        .apply('Volatile', '$D.max($added) - $D.min($deleted)');

      var { aggregateApplies, postAggregateApplies } = External.segregationAggregateApplies(ex.actions);

      expect(aggregateApplies.join('\n')).to.equal(sane`
        apply(Count,$D.count())
        apply(Added,$D.sum($added))
        apply("!T_0",$D.max($added))
        apply("!T_1",$D.min($deleted))
      `);

      expect(postAggregateApplies.join('\n')).to.equal(sane`
        apply(Volatile,$\{!T_0}:NUMBER.subtract($\{!T_1}:NUMBER))
      `);
    });

    it("breaks up correctly in case of duplicate name", () => {
      var ex = ply()
        .apply('Count', '$D.count()')
        .apply('Added', '$D.sum($added)')
        .apply('Volatile', '$D.sum($added) - $D.sum($deleted)');

      var { aggregateApplies, postAggregateApplies } = External.segregationAggregateApplies(ex.actions);

      expect(aggregateApplies.join('\n')).to.equal(sane`
        apply(Count,$D.count())
        apply(Added,$D.sum($added))
        apply("!T_0",$D.sum($deleted))
      `);

      expect(postAggregateApplies.join('\n')).to.equal(sane`
        apply(Volatile,$Added:NUMBER.subtract($\{!T_0}:NUMBER))
      `);
    });

    it("breaks up correctly in case of variable reference", () => {
      var ex = ply()
        .apply('Count', '$D.count()')
        .apply('Added', '$D.sum($added)')
        .apply('Volatile', '$Added - $D.sum($deleted)');

      var { aggregateApplies, postAggregateApplies } = External.segregationAggregateApplies(ex.actions);

      expect(aggregateApplies.join('\n')).to.equal(sane`
        apply(Count,$D.count())
        apply(Added,$D.sum($added))
        apply("!T_0",$D.sum($deleted))
      `);

      expect(postAggregateApplies.join('\n')).to.equal(sane`
        apply(Volatile,$Added.subtract($\{!T_0}:NUMBER))
      `);
    });

    it("breaks up correctly in complex case", () => {
      var ex = ply()
        .apply('AddedByDeleted', '$D.sum($added) / $D.sum($deleted)')
        .apply('DeletedByInserted', '$D.sum($deleted) / $D.sum($inserted)')
        .apply('Deleted', '$D.sum($deleted)');

      var { aggregateApplies, postAggregateApplies } = External.segregationAggregateApplies(ex.actions);

      expect(aggregateApplies.join('\n')).to.equal(sane`
        apply(Deleted,$D.sum($deleted))
        apply("!T_0",$D.sum($added))
        apply("!T_1",$D.sum($inserted))
      `);

      expect(postAggregateApplies.join('\n')).to.equal(sane`
        apply(AddedByDeleted,$\{!T_0}:NUMBER.divide($Deleted:NUMBER))
        apply(DeletedByInserted,$Deleted:NUMBER.divide($\{!T_1}:NUMBER))
      `);
    });
  });


  describe("#hasAttribute", () => {
    var rawExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'moon_child',
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


  describe("#addAction / #getRaw", () => {
    var rawExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'moon_child',
      timeAttribute: 'time',
      attributes: [
        { name: 'page', type: 'STRING' },
        { name: 'added', type: 'NUMBER', unsplitable: true }
      ]
    });

    it('runs through a life cycle', () => {
      var filteredRawExternal = rawExternal.addFilter($('page').contains('lol'));

      var ex = $('blah').sum('$added:NUMBER');
      var filteredValueExternal = filteredRawExternal.addAction(ex.actions[0]);

      expect(filteredValueExternal.mode).to.equal('value');
      expect(filteredValueExternal.valueExpression.toString()).to.equal('$__SEGMENT__:DATASET.sum($added:NUMBER)');

      var filteredRawExternal2 = filteredValueExternal.getRaw();
      expect(filteredRawExternal2.equals(filteredRawExternal)).to.equal(true);

      var rawExternal2 = filteredValueExternal.getBase();
      expect(rawExternal2.equals(rawExternal)).to.equal(true);
    });

    it('it checks that expressions are internally defined (filter raw)', () => {
      expect(rawExternal.addAction($('blah').filter('$user:STRING.contains("lol")').actions[0])).to.equal(null);
      expect(rawExternal.addAction($('blah').filter('$page:STRING.contains("lol")').actions[0])).to.not.equal(null);
    });

    it('it checks that expressions are internally defined (filter split)', () => {
      var splitExternal = rawExternal.addAction($('blah').split('$page:STRING', 'Page', 'blah').actions[0]);
      expect(splitExternal.addAction($('blah').filter('$User:STRING.contains("lol")').actions[0])).to.equal(null);
      expect(splitExternal.addAction($('blah').filter('$Page:STRING.contains("lol")').actions[0])).to.not.equal(null);
    });

    it('it checks that expressions are internally defined (split)', () => {
      expect(rawExternal.addAction($('blah').split('$user:STRING', 'User', 'blah').actions[0])).to.equal(null);
      expect(rawExternal.addAction($('blah').split('$page:STRING', 'Page', 'blah').actions[0])).to.not.equal(null);
    });

    it('it checks that expressions are internally defined (apply on raw)', () => {
      expect(rawExternal.addAction($('blah').apply('DeltaPlusOne', '$delta:NUMBER + 1').actions[0])).to.equal(null);
      expect(rawExternal.addAction($('blah').apply('AddedPlusOne', '$added:NUMBER + 1').actions[0])).to.not.equal(null);
    });

    it('it checks that expressions are internally defined (apply on split)', () => {
      var splitExternal = rawExternal.addAction($('blah').split('$page:STRING', 'Page', 'blah').actions[0]);
      expect(splitExternal.addAction($('blah').apply('DeltaPlusOne', '$blah.sum($delta:NUMBER)').actions[0])).to.equal(null);
      expect(splitExternal.addAction($('blah').apply('AddedPlusOne', '$blah.sum($added:NUMBER)').actions[0])).to.not.equal(null);
    });

    it('it checks that expressions are internally defined (value / aggregate)', () => {
      expect(rawExternal.addAction($('blah').sum('$delta:NUMBER').actions[0])).to.equal(null);
      expect(rawExternal.addAction($('blah').sum('$added:NUMBER').actions[0])).to.not.equal(null);
    });

    it('it checks that expressions are internally defined (select)', () => {
      //expect(rawExternal.addAction($('blah').select('user').actions[0])).to.equal(null);
      expect(rawExternal.addAction($('blah').select('page').actions[0])).to.not.equal(null);
    });

  });


  describe("#bucketsConcealed", () => {
    var bucketedExternal = External.fromJS({
      engine: 'druid',
      dataSource: 'wikipedia',
      timeAttribute: 'time',
      attributes: [
        { name: 'time', type: 'TIME', makerAction: { action: 'timeFloor', duration: 'PT1H', timezone: 'Etc/UTC' } },
        { name: 'language', type: 'STRING' }
      ]
    });

    it('accepts', () => {
      var exs = [
        $('time').timeFloor('PT1H', 'Etc/UTC'),
        $('time').timeFloor('PT2H', 'Etc/UTC'),
        $('time').timeFloor('P1D', 'Etc/UTC'),
        $('time').timeBucket('P1D', 'Etc/UTC'),
        $('language').is('en').and($('time').timeFloor('PT1H', 'Etc/UTC')),
        $('time').in(new Date('2016-09-01T01:00:00Z'), new Date('2016-09-02T01:00:00Z'))
      ];

      for (var ex of exs) {
        expect(bucketedExternal.bucketsConcealed(ex), ex.toString()).to.equal(true);
      }
    });

    it('rejects', () => {
      var exs = [
        $('time'),
        $('time').timeFloor('PT1H'),
        $('time').timeFloor('PT1M', 'Etc/UTC'),
        $('time').timeFloor('PT1S', 'Etc/UTC'),
        $('language').is('en').and($('time').timeFloor('PT1M', 'Etc/UTC')),
        $('time').in(new Date('2016-09-01T01:00:00Z'), new Date('2016-09-02T01:00:01Z'))
      ];

      for (var ex of exs) {
        expect(bucketedExternal.bucketsConcealed(ex), ex.toString()).to.equal(false);
      }
    });

  });


  describe("simplifies / digests", () => {

    describe("raw mode", () => {
      it("works in basic raw mode", () => {
        var ex = $('wiki');

        ex = ex.referenceCheck(context).resolve(context).simplify();
        expect(ex.op).to.equal('external');
      });

      it("works with a simple select", () => {
        var ex = $('wiki').select('time', 'language', 'added');

        ex = ex.referenceCheck(context).resolve(context).simplify();
        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.select.attributes).to.deep.equal(['time', 'language', 'added']);
      });

      it("works with a derived attribute and a filter", () => {
        var ex = $('wiki')
          .apply('addedTwice', '$added * 2')
          .filter($("language").is('en'));

        ex = ex.referenceCheck(context).resolve(context).simplify();
        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.derivedAttributes).to.have.all.keys(['addedTwice']);

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time.in([2013-02-26T00:00:00.000Z,2013-02-27T00:00:00.000Z)).and($language:STRING.is("en"))
        `);
      });

      it("works with a sort and a limit", () => {
        var ex = $('wiki')
          .sort('$time')
          .limit(10);

        ex = ex.referenceCheck(context).resolve(context).simplify();
        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.sort.toString()).to.equal('sort($time:TIME,ascending)');
        expect(externalDataset.limit.toString()).to.equal('limit(10)');
      });

      it("works with a sort and a limit where there is also an aggregate", () => {
        var ex = $('wiki')
          .sort('$time')
          .limit(10)
          .count();

        ex = ex.referenceCheck(context).resolve(context).simplify();
        expect(ex.op).to.equal('chain');
        var externalDataset = ex.expression.external;

        expect(externalDataset.sort.toString()).to.equal('sort($time:TIME,ascending)');
        expect(externalDataset.limit.toString()).to.equal('limit(10)');

        expect(ex.actions).to.have.length(1);
      });

    });


    describe("value mode", () => {
      it("works with a basic aggregate", () => {
        var ex = $('wiki').sum('$added');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time.in([2013-02-26T00:00:00.000Z,2013-02-27T00:00:00.000Z))
        `);

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          $__SEGMENT__:DATASET.sum($added:NUMBER)
        `);

        expect(externalDataset.simulateValue(true, [])).to.equal(4);
      });

      it("works with a filter and aggregate", () => {
        var ex = $('wiki').filter('$page == USA').sum('$added');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time.in([2013-02-26T00:00:00.000Z,2013-02-27T00:00:00.000Z)).and($page:STRING.is("USA"))
        `);

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          $__SEGMENT__:DATASET.sum($added:NUMBER)
        `);
      });

      it("works with aggregate that has a simple post process", () => {
        var ex = $('wiki').filter('$page == USA').sum('$added').multiply(2);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time.in([2013-02-26T00:00:00.000Z,2013-02-27T00:00:00.000Z)).and($page:STRING.is("USA"))
        `);

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          $__SEGMENT__:DATASET.sum($added:NUMBER).multiply(2)
        `);
      });

      it("works with aggregate that has an expressionless post process", () => {
        var ex = $('wiki').filter('$page == USA').sum('$added').absolute();

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          $__SEGMENT__:DATASET.sum($added:NUMBER).absolute()
        `);
      });

      it("works with aggregate that has a complex post process", () => {
        var ex = $('wiki').filter('$page == USA').sum('$added').add($('wiki').sum('$deleted'));

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time.in([2013-02-26T00:00:00.000Z,2013-02-27T00:00:00.000Z))
        `);

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          $__SEGMENT__:DATASET.filter($page:STRING.is("USA")).sum($added:NUMBER).add($__SEGMENT__:DATASET.sum($deleted:NUMBER))
        `);
      });

      it("works with aggregate that has a LHS post process", () => {
        var ex = r(5).subtract($('wiki').filter('$page == USA').sum('$added'));

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time.in([2013-02-26T00:00:00.000Z,2013-02-27T00:00:00.000Z)).and($page:STRING.is("USA"))
        `);

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          5.subtract($__SEGMENT__:DATASET.sum($added:NUMBER))
        `);
      });

      it("works with aggregate that has LHS and RHS post process", () => {
        var ex = r(5).subtract($('wiki').filter('$page == USA').sum('$added'), $('wiki').count());

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time.in([2013-02-26T00:00:00.000Z,2013-02-27T00:00:00.000Z))
        `);

        expect(externalDataset.valueExpression.toString()).to.equal(sane`
          5.subtract($__SEGMENT__:DATASET.filter($page:STRING.is("USA")).sum($added:NUMBER)).subtract($__SEGMENT__:DATASET.count())
        `);
      });

    });


    describe("total mode", () => {
      it("works with a single apply", () => {
        var ex = ply()
          .apply('TotalAdded', '$wiki.sum($added)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time.in([2013-02-26T00:00:00.000Z,2013-02-27T00:00:00.000Z))
        `);

        expect(externalDataset.applies.join('\n')).to.equal(sane`
          apply(TotalAdded,$__SEGMENT__:DATASET.sum($added:NUMBER))
        `);

        expect(externalDataset.rawAttributes).to.have.length(6);
        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "TotalAdded", "type": "NUMBER" }
        ]);

        expect(externalDataset.simulateValue(true, []).toJS()).to.deep.equal([
          {
            "TotalAdded": 4
          }
        ]);
      });

      it("works with several applies, some filtered", () => {
        var ex = ply()
          .apply('Count', '$wiki.count()')
          .apply('TotalAdded', '$wiki.sum($added)')
          .apply('TotalUSA', '$wiki.filter($page == USA).sum($added)')
          .apply('TotalUK', '$wiki.filter($page == UK).sum($added)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time.in([2013-02-26T00:00:00.000Z,2013-02-27T00:00:00.000Z))
        `);

        expect(externalDataset.applies.join('\n')).to.equal(sane`
          apply(Count,$__SEGMENT__:DATASET.count())
          apply(TotalAdded,$__SEGMENT__:DATASET.sum($added:NUMBER))
          apply(TotalUSA,$__SEGMENT__:DATASET.filter($page:STRING.is("USA")).sum($added:NUMBER))
          apply(TotalUK,$__SEGMENT__:DATASET.filter($page:STRING.is("UK")).sum($added:NUMBER))
        `);

        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Count", "type": "NUMBER" },
          { name: "TotalAdded", "type": "NUMBER" },
          { name: "TotalUSA", "type": "NUMBER" },
          { name: "TotalUK", "type": "NUMBER" }
        ]);

        expect(externalDataset.simulateValue(true, []).toJS()).to.deep.equal([
          {
            "Count": 4,
            "TotalAdded": 4,
            "TotalUSA": 4,
            "TotalUK": 4
          }
        ]);
      });

      it("works with several applies, all filtered", () => {
        var ex = ply()
          .apply('TotalUSA', '$wiki.filter($page == USA).sum($added)')
          .apply('TotalUK', '$wiki.filter($page == UK).sum($added)')
          .apply('TotalIndia', '$wiki.filter($page == India).sum($added)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time.in([2013-02-26T00:00:00.000Z,2013-02-27T00:00:00.000Z))
        `);

        expect(externalDataset.applies.join('\n')).to.equal(sane`
          apply(TotalUSA,$__SEGMENT__:DATASET.filter($page:STRING.is("USA")).sum($added:NUMBER))
          apply(TotalUK,$__SEGMENT__:DATASET.filter($page:STRING.is("UK")).sum($added:NUMBER))
          apply(TotalIndia,$__SEGMENT__:DATASET.filter($page:STRING.is("India")).sum($added:NUMBER))
        `);

        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "TotalUSA", "type": "NUMBER" },
          { name: "TotalUK", "type": "NUMBER" },
          { name: "TotalIndia", "type": "NUMBER" }
        ]);
      });

      it("works with a filter and applies", () => {
        var ex = ply()
          .apply("wiki", $('wiki').filter("$language == 'en'"))
          .apply('Count', '$wiki.count()')
          .apply('TotalAdded', '$wiki.sum($added)')
          .apply('TotalUSA', '$wiki.filter($page == USA).sum($added)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time.in([2013-02-26T00:00:00.000Z,2013-02-27T00:00:00.000Z)).and($language:STRING.is("en"))
        `);

        expect(externalDataset.applies.join('\n')).to.equal(sane`
          apply(Count,$__SEGMENT__:DATASET.count())
          apply(TotalAdded,$__SEGMENT__:DATASET.sum($added:NUMBER))
          apply(TotalUSA,$__SEGMENT__:DATASET.filter($page:STRING.is("USA")).sum($added:NUMBER))
        `);

        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Count", "type": "NUMBER" },
          { name: "TotalAdded", "type": "NUMBER" },
          { name: "TotalUSA", "type": "NUMBER" }
        ]);
      });

      it("works with a filter and many applies and re-define", () => {
        var ex = ply()
          .apply('Count', '$wiki.count()')
          .apply("wiki", $('wiki').apply('addedTwice', '$added * 2'))
          .apply('TotalAdded', '$wiki.sum($added)')
          .apply("wiki", $('wiki').filter("$language == 'en'"))
          .apply('TotalEnAdded', '$wiki.sum($added)')
          .apply("wiki_alt", $('wiki').filter("$page == USA"))
          .apply('TotalUsAdded', '$wiki_alt.sum($added)')
          .apply('OrigMinAdded', '$^wiki.min($added)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.derivedAttributes).to.have.all.keys(['addedTwice']);

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time.in([2013-02-26T00:00:00.000Z,2013-02-27T00:00:00.000Z))
        `);

        expect(externalDataset.applies.join('\n')).to.equal(sane`
          apply(Count,$__SEGMENT__:DATASET.count())
          apply(TotalAdded,$__SEGMENT__:DATASET.sum($added:NUMBER))
          apply(TotalEnAdded,$__SEGMENT__:DATASET.filter($language:STRING.is("en")).sum($added:NUMBER))
          apply(TotalUsAdded,$__SEGMENT__:DATASET.filter($language:STRING.is("en").and($page:STRING.is("USA"))).sum($added:NUMBER))
          apply(OrigMinAdded,$__SEGMENT__:DATASET.min($added:NUMBER))
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
        var ex = ply()
          .apply("wiki", $('wiki').filter("$language == 'en'"))
          .apply("Five", 5)
          .apply('CountX3', '$wiki.count() * 3')
          .apply('AddedPlusDeleted', '$wiki.sum($added) + $wiki.sum($deleted)')
          .apply("Six", 6)
          .apply('AddedUsPlusDeleted', '$wiki.filter($page == USA).sum($added) + $wiki.sum($deleted)')
          .apply('CountX3Plus5', '$CountX3 + 5');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

        expect(externalDataset.filter.toString()).to.equal(sane`
          $time.in([2013-02-26T00:00:00.000Z,2013-02-27T00:00:00.000Z)).and($language:STRING.is("en"))
        `);

        expect(externalDataset.applies.join('\n')).to.equal(sane`
          apply(Five,5)
          apply(CountX3,$__SEGMENT__:DATASET.count().multiply(3))
          apply(AddedPlusDeleted,$__SEGMENT__:DATASET.sum($added:NUMBER).add($__SEGMENT__:DATASET.sum($deleted:NUMBER)))
          apply(Six,6)
          apply(AddedUsPlusDeleted,$__SEGMENT__:DATASET.filter($page:STRING.is("USA")).sum($added:NUMBER).add($__SEGMENT__:DATASET.sum($deleted:NUMBER)))
          apply(CountX3Plus5,$__SEGMENT__:DATASET.count().multiply(3).add(5))
        `);

        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Five", "type": "NUMBER" },
          { name: "CountX3", "type": "NUMBER" },
          { name: "AddedPlusDeleted", "type": "NUMBER" },
          { name: "Six", "type": "NUMBER" },
          { name: "AddedUsPlusDeleted", "type": "NUMBER" },
          { name: "CountX3Plus5", "type": "NUMBER" }
        ]);
      });

    });


    describe("split mode", () => {
      it("works with a split on string", () => {
        var ex = $('wiki').split("$page", 'Page')
          .apply('Count', '$wiki.count()')
          .apply('Added', '$wiki.sum($added)')
          .sort('$Count', 'descending')
          .limit(5);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;
        expect(externalDataset.applies).to.have.length(2);
        expect(externalDataset.limit.limit).to.equal(5);
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
        var ex = $('wiki').split("$page", 'Page')
          .apply('Count', '$wiki.count()')
          .sort('$Count', 'descending')
          .limit(5)
          .apply('Added', '$wiki.sum($added)')
          .limit(9);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;
        expect(externalDataset.applies).to.have.length(2);
        expect(externalDataset.limit.limit).to.equal(5);
        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Page", "type": "STRING" },
          { name: "Count", "type": "NUMBER" },
          { name: "Added", "type": "NUMBER" }
        ]);
      });

      it("works with a split on string with multiple limits in descending order", () => {
        var ex = $('wiki').split("$page", 'Page')
          .apply('Count', '$wiki.count()')
          .sort('$Count', 'descending')
          .limit(9)
          .apply('Added', '$wiki.sum($added)')
          .limit(5);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;
        expect(externalDataset.applies).to.have.length(2);
        expect(externalDataset.limit.limit).to.equal(5);
        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Page", "type": "STRING" },
          { name: "Count", "type": "NUMBER" },
          { name: "Added", "type": "NUMBER" }
        ]);
      });

      it("works with a split on time", () => {
        var ex = $('wiki').split($("time").timeBucket('P1D', 'America/Los_Angeles'), 'Timestamp')
          .apply('Count', '$wiki.count()')
          .apply('Added', '$wiki.sum($added)')
          .sort('$Count', 'descending')
          .limit(5);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('chain');
        expect(ex.actions).to.have.length(2);
        var externalDataset = ex.expression.external;
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
              "start": new Date('2015-03-13T07:00:00.000Z'),
              "end": new Date('2015-03-14T07:00:00.000Z'),
              "type": "TIME_RANGE"
            }
          }
        ]);
      });

      it("works with a filtered split on string", () => {
        var ex = $('wiki').filter('$language == "en"').split("$page", 'Page')
          .apply('Count', '$wiki.count()')
          .apply('Added', '$wiki.sum($added)')
          .sort('$Count', 'descending')
          .limit(5);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('external');
        var externalDataset = ex.external;

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
        var ex = ply()
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

        expect(ex.op).to.equal('chain');
        expect(ex.actions).to.have.length(1);

        var externalDataset = ex.expression.external;
        expect(externalDataset.applies).to.have.length(2);
        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Count", "type": "NUMBER" },
          { name: "TotalAdded", "type": "NUMBER" }
        ]);
      });

      it("works with a blank total and a split", () => {
        var ex = ply()
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

        expect(ex.op).to.equal('chain');
        expect(ex.actions).to.have.length(1);
        expect(ex.expression.op).to.equal('literal');
      });

      it("works with a total and a split with a parent reference", () => {
        var ex = ply()
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

        expect(ex.op).to.equal('chain');
        expect(ex.actions).to.have.length(1);

        var externalDataset = ex.expression.external;
        expect(externalDataset.applies).to.have.length(1);
        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Count", "type": "NUMBER" }
        ]);
      });

      it("works with a total and a split in a strange order", () => {
        var ex = ply()
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

        expect(ex.op).to.equal('chain');
        expect(ex.actions).to.have.length(1);

        var externalDataset = ex.expression.external;
        expect(externalDataset.applies).to.have.length(2);
        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Count", "type": "NUMBER" },
          { name: "TotalAdded", "type": "NUMBER" }
        ]);
      });

      it("works with a split and another split in a strange order", () => {
        var ex = $('wiki').split("$page", 'Page')
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

        expect(ex.op).to.equal('chain');
        expect(ex.actions).to.have.length(1);

        var externalDataset = ex.expression.external;
        expect(externalDataset.applies).to.have.length(2);
        expect(externalDataset.limit.limit).to.equal(5);
        expect(externalDataset.toJS().attributes).to.deep.equal([
          { name: "Page", "type": "STRING" },
          { name: "Count", "type": "NUMBER" },
          { name: "Added", "type": "NUMBER" }
        ]);
      });

      it.skip("a join of two splits", () => {
        var ex = $('wiki').split('$page', 'Page').join($('wikiCmp').split('$page', 'Page'))
          .apply('wiki', '$wiki.filter($page = $^Page)')
          .apply('wikiCmp', '$wikiCmp.filter($page = $^Page)')
          .apply('Count', '$wiki.count()')
          .apply('CountDiff', '$wiki.count() - $wikiCmp.count()')
          .sort('$CountDiff', 'descending')
          .limit(5);

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('chain');
        expect(ex.operand.op).to.equal('join');

        var externalDatasetMain = ex.operand.lhs.value;
        expect(externalDatasetMain.applies).to.have.length(2);
        expect(externalDatasetMain.toJS().attributes).to.deep.equal([
          { name: "Count", "type": "NUMBER" },
          { name: "Page", "type": "STRING" },
          { name: "_br_0", "type": "NUMBER" }
        ]);

        var externalDatasetCmp = ex.operand.rhs.value;
        expect(externalDatasetCmp.applies).to.have.length(1);
        expect(externalDatasetCmp.toJS().attributes).to.deep.equal([
          { name: "Page", "type": "STRING" },
          { name: "_br_1", "type": "NUMBER" }
        ]);

        expect(ex.actions[0].toString()).to.equal('.apply(CountDiff, ($_br_0 + $_br_1))');
      });

      it("works with a split and further compute", () => {
        var ex = ply()
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

        expect(ex.op).to.equal('chain');
        expect(ex.actions).to.have.length(3);

        expect(ex.actions.map(a => a.name)).to.deep.equal(['Pages', 'MinCount', 'MaxAdded']);
      });

      it("works with a split and a further split", () => {
        var ex = $('wiki')
          .split({ 'user': '$user', 'page': '$page' })
          .apply('TotalAdded', '$wiki.sum($added)')
          .split('$user', 'user', 'data')
          .apply('SumTotalEdits', '$data.sum($TotalAdded)');

        ex = ex.referenceCheck(context).resolve(context).simplify();

        expect(ex.op).to.equal('chain');
        expect(ex.actions.map(a => a.action)).to.deep.equal(['split', 'apply']);
      });

    });

  });
});
