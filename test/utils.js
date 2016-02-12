var { expect } = require("chai");
var Q = require('q');

var plywood = require('../build/plywood');

var uniformizeResults = function(result) {
  if (!((typeof result !== "undefined" && result !== null) ? result.prop : undefined)) {
    return result;
  }

  var ret = {};
  for (var k in result) {
    var p = result[k];
    if (!result.hasOwnProperty(k)) {
      continue;
    }
    if (k === 'split') {
      continue;
    }
    if (k === 'prop') {
      var propNames = [];
      for (var name in p) {
        var value = p[name];
        propNames.push(name);
      }
      propNames.sort();

      var prop = {};
      for (var i = 0, name; i < propNames.length; i++) {
        name = propNames[i];
        value = p[name];
        if (!p.hasOwnProperty(name)) {
          continue;
        }
        if (typeof value === 'number' && value !== Math.floor(value)) {
          prop[name] = Number(value.toPrecision(5));
        } else if (Array.isArray(value) && typeof value[0] === 'number' && typeof value[1] === 'number' && (value[0] !== Math.floor(value[0]) || value[1] !== Math.floor(value[1]))) {
          prop[name] = [value[0].toFixed(3), value[1].toFixed(3)];
        } else {
          prop[name] = value;
        }
      }
      p = prop;
    }

    ret[k] = p;
  }

  if (result.splits) {
    ret.splits = result.splits.map(uniformizeResults);
  }

  if (result.loading) {
    ret.loading = true;
  }

  return ret;
};

exports.wrapVerbose = function(requester, name) {
  return function(request) {
    console.log(`Requesting ${name}:`);
    console.log('', JSON.stringify(request.query, null, 2));
    var startTime = Date.now();
    return requester(request).then(
      function(result) {
        console.log(`GOT RESULT FROM ${name} (took ${Date.now() - startTime}ms)`);
        return result;
      },
      function(err) {
        console.log(`GOT ${name} ERROR`, err);
        throw err;
      }
    );
  };
};

exports.makeEqualityTest = function(driverFnMap) {
  return function({drivers, query, verbose, before, after}) {
    if (drivers.length < 2) {
      throw new Error("must have at least two drivers");
    }
    query = FacetQuery.isFacetQuery(query) ? query : new FacetQuery(query);

    var driverFns = drivers.map(function(driverName) {
      var driverFn = driverFnMap[driverName];
      if (!driverFn) {
        throw new Error(`no such driver ${driverName}`);
      }
      return driverFn;
    });

    return function(testComplete) {
      if (typeof before === "function") {
        before();
      }
      return Q.all(
        driverFns.map(function(driverFn) {
            return driverFn({
              query,
              context: { priority: -3 }
            });
          }
        )
      ).then(
        function(results) {
          if (typeof after === "function") {
            after(null, results[0], results);
          }

          results = results.map(function(result) {
              expect(result).to.be.instanceof(SegmentTree);
              return uniformizeResults(result.toJS());
            }
          );

          if (verbose) {
            console.log('vvvvvvvvvvvvvvvvvvvvvvv');
            console.log(`From ${drivers[0]} I got:`);
            console.log(JSON.stringify(results[0], null, 2));
            console.log('^^^^^^^^^^^^^^^^^^^^^^^');
          }

          var i = 1;
          while (i < drivers.length) {
            try {
              expect(results[0]).to.deep.equal(results[i], `results of '${drivers[0]}' and '${drivers[i]}' must match`);
            } catch (e) {
              console.log(`results of '${drivers[0]}' and '${drivers[i]}' (expected) must match`);
              throw e;
            }
            i++;
          }

          testComplete(null, results[0]);
          return;
        },
        function(err) {
          if (typeof after === "function") {
            after(err);
          }
          console.log("got error from driver");
          console.log(err);
          throw err;
        })
        .done();
    };
  };
};

exports.makeErrorTest = function(driverFnMap) {
  return function({drivers, request, error, verbose}) {
    if (drivers.length < 1) {
      throw new Error("must have at least one driver");
    }

    var driverFns = drivers.map(function(driverName) {
      var driverFn = driverFnMap[driverName];
      if (!driverFn) {
        throw new Error(`no such driver ${driverName}`);
      }
      return driverFn;
    });

    return function(testComplete) {
      return Q.allSettled(driverFns.map(function(driverFn) {
          return driverFn(request);
        }))
        .then(function(results) {
            for (var i = 0, result; i < results.length; i++) {
              result = results[i];
              if (result.state === "fulfilled") {
                throw new Error(`${drivers[i]} did not error`);
              } else {
                expect(result.reason.message).to.equal(error, `${drivers[i]} did not conform to error`);
              }
            }
            testComplete();
          }
        )
        .done();
    };
  };
};
